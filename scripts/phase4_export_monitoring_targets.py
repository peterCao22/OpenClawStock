"""
从 Phase4 合并候选 JSON 导出 OpenClaw / stock_monitor 可用的监控列表。

背景：
    合并池（4A top250 + 4B top100）体量过大，不适合全部接入实时轮询；本脚本按「母池 → 子集」
    默认导出 **300** 条（`DEFAULT_HEAD`），默认按 monitor_trend（先按 score_global 定人选，再按周线多头+近 5 日涨幅重排），
    写入 results/monitoring_targets.json。

输出格式：
    与 scripts/stock_monitor.py 约定一致：每项含 code、name、concepts（可为空列表）。

运行示例：
    python scripts/phase4_export_monitoring_targets.py --merged output/phase4_merged_20251231.json --head 50
    python scripts/phase4_export_monitoring_targets.py --merged output/phase4_merged_20251231.json --dry-run

排序模式（--order-mode）：
    monitor_trend（默认）：与 score_global 相同的入选集合与截断，但在最终 head 条内按
        「周线多头排列分 → 收盘/MA60 → 收盘/MA18 周(≈90 交易日) → 近 5 交易日涨幅」重排，便于轮询时优先强趋势、近期走强标的。
        依赖合并 JSON 中带 ret_5d_pct、weekly_ma_alignment_score 等字段（须用新版 4A/4B 扫描生成）。
    score_global：整池按 score 降序、同分按 trigger_recency_days 降序，再取前 head 条。
        避免「合并文件先 4A 后 4B」导致 4B 高分股永远进不了前 50，也更贴近「监控模型最看好的标的」。
    merge：沿用合并 JSON 中的顺序（先 4A 再 4B），再取前 head 条（旧行为）。
    learned_proxy：读取「截面校准」JSON（多项式特征 + OLS 系数），按 proxy 分值排序；**仅适用于与校准文件
        同一市场环境的复盘**，换截面须重新运行 scripts/fit_monitor_learned_proxy.py 生成新配置。
"""
from __future__ import annotations

import argparse
import json
import sys
from itertools import combinations
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_LEARNED_PROXY_CONFIG = ROOT / "config" / "monitor_learned_proxy_20251231.json"
RESULTS_DIR = ROOT / "results"
# 默认监控条数：在 score_global 下提高「事后涨幅前列」进入监控名单的概率；
# 具体 KPI 请在多个 as-of 上用 phase4_simulate_monitor_vs_backtest / phase4_multi_asof_metrics 校验。
DEFAULT_HEAD = 300


def _sort_key_vr_band(row: Dict[str, Any]) -> Tuple[float, float, str]:
    """次优带内按放量比 vr 挑递补：vr 高优先，同 vr 用 score、代码稳定序。"""
    vr = row.get("vr")
    if vr is None:
        vr = float("-inf")
    else:
        vr = float(vr)
    sc = row.get("score")
    if sc is None:
        sc = float("-inf")
    else:
        sc = float(sc)
    inst = str(row.get("instrument") or "")
    return (vr, sc, inst)


def _order_composite_blend(full: List[Dict[str, Any]], w_score: float) -> List[Dict[str, Any]]:
    """
    综合排名：在「纯分数序」与「触发较新序」之间加权（rank 混合），用于监控名单实验排序。

    参数：
        full: 合并候选列表
        w_score: 分数秩权重，越大越接近 score_global；越小越偏向 trigger 较新
    返回：
        新顺序的列表（全量，不截断）
    注意：
        在 2025-12-31 截面上略利于抬高「事后涨幅前50」捕获，但可能压低「前10命中」；
        默认导出仍用 score_global，本模式仅 --order-mode composite 时启用。
    """
    n = len(full)
    if n == 0:
        return []

    w_score = max(0.0, min(1.0, float(w_score)))
    idx = list(range(n))

    by_s = sorted(
        idx,
        key=lambda i: _sort_key_score_global(full[i]),
        reverse=True,
    )
    rank_s = [0] * n
    for pos, i in enumerate(by_s):
        rank_s[i] = pos

    by_f = sorted(
        idx,
        key=lambda i: (
            full[i].get("trigger_recency_days") is None,
            full[i].get("trigger_recency_days")
            if full[i].get("trigger_recency_days") is not None
            else 10**12,
            str(full[i].get("instrument") or ""),
        ),
    )
    rank_f = [0] * n
    for pos, i in enumerate(by_f):
        rank_f[i] = pos

    blended = sorted(
        idx,
        key=lambda i: (w_score * rank_s[i] + (1.0 - w_score) * rank_f[i], str(full[i].get("instrument") or "")),
    )
    return [full[i] for i in blended]


def _sort_key_score_global(row: Dict[str, Any]) -> Tuple[float, float, str]:
    """
    全局按模型分数排序用键：score 高优先，同分 trigger_recency_days 高优先（与 4A 并列分 tie-break 一致），最后 instrument 稳定序。

    参数：
        row: 合并池单条
    返回：
        用于 sorted(reverse=True) 的三元组
    """
    sc = row.get("score")
    if sc is None:
        sc = float("-inf")
    else:
        sc = float(sc)
    tr = row.get("trigger_recency_days")
    if tr is None:
        tr = float("-inf")
    else:
        tr = float(tr)
    inst = str(row.get("instrument") or "")
    return (sc, tr, inst)


def _sort_key_monitor_trend(row: Dict[str, Any]) -> Tuple[float, float, float, float, float, float, str]:
    """
    监控名单「趋势+短线动量」排序键：值越大越靠前。

    优先级：周线多头排列分 → 收盘相对 MA60（周）→ 收盘相对 MA18 周（约 90 交易日）→ 近 5 交易日涨幅
    → 原模型 score → trigger_recency_days → 代码（稳定序）。
    缺失字段按最差处理，旧版无辅助字段的合并 JSON 会主要按 score 次序退化。

    参数：
        row: 合并池单条候选（含 score、可选 weekly_ma_alignment_score 等）
    返回：
        用于 sorted(reverse=True) 的元组
    """
    ali = row.get("weekly_ma_alignment_score")
    if ali is None:
        ali_v = -1.0
    else:
        ali_v = float(ali)
    c60 = row.get("current_close_vs_ma60w")
    c60f = float(c60) if c60 is not None else float("-inf")
    c18 = row.get("current_close_vs_ma18w")
    c18f = float(c18) if c18 is not None else float("-inf")
    r5 = row.get("ret_5d_pct")
    r5f = float(r5) if r5 is not None else float("-inf")
    sc, tr, inst = _sort_key_score_global(row)
    return (ali_v, c60f, c18f, r5f, sc, tr, inst)


def _load_learned_proxy_config(path: Path) -> Dict[str, Any]:
    """
    读取 learned_proxy 排序用的 JSON 配置（由 fit_monitor_learned_proxy.py 生成）。

    参数：
        path: JSON 文件路径
    返回：
        含 base_columns、medians、coef、lam_vr_score4b 等字段的字典
    """
    return json.loads(path.read_text(encoding="utf-8"))


def _learned_proxy_pred(full: List[Dict[str, Any]], cfg: Dict[str, Any]) -> np.ndarray:
    """
    按配置计算合并池每条候选的 proxy 分值（越大越优先监控）。

    与 fit_monitor_learned_proxy.py 中设计矩阵一致：19 维数值列 + 二阶交互 + score/vr 低次项 + 偏置，
    再叠加 lam * rank_pct(vr) * score_4b。缺失列按配置中的训练期中位数填充。

    参数：
        full: 合并 candidates 列表
        cfg: _load_learned_proxy_config 的结果
    返回：
        shape=(n,) 的 float64 数组，与 full 行序对齐
    """
    df = pd.DataFrame(full)
    col_names: List[str] = list(cfg["base_columns"])
    medians: Dict[str, Any] = cfg["medians"]
    for c in col_names:
        if c not in df.columns:
            df[c] = np.nan
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(float(medians[str(c)]))
    blocks: List[np.ndarray] = [df[c].values.astype(np.float64) for c in col_names]
    for ia, ib in combinations(range(len(col_names)), 2):
        blocks.append(blocks[ia] * blocks[ib])
    sc = df[col_names[0]].values.astype(np.float64)
    vr_col = df["vr"].values.astype(np.float64)
    for e in (sc**3, vr_col**3, sc**2 * vr_col, vr_col**2 * sc, sc**2, vr_col**2):
        blocks.append(e)
    xmat = np.column_stack(blocks + [np.ones(len(df), dtype=np.float64)])
    coef = np.asarray(cfg["coef"], dtype=np.float64)
    base = xmat @ coef
    vrn = pd.Series(vr_col).rank(pct=True).values.astype(np.float64)
    s4b = pd.to_numeric(df.get("score_4b"), errors="coerce").fillna(0.0).values.astype(np.float64)
    lam = float(cfg["lam_vr_score4b"])
    return base + lam * vrn * s4b


def _order_by_learned_proxy(
    full: List[Dict[str, Any]],
    cfg: Dict[str, Any],
    *,
    defer_to_end: Optional[Set[str]] = None,
) -> List[Dict[str, Any]]:
    """
    将 full 按 learned_proxy 分值降序排列；同分用 instrument 稳定序。

    参数：
        full: 候选列表
        cfg: learned_proxy 配置字典
        defer_to_end: 若给定，这些代码排在列表末尾（保持其在 full 中的相对顺序），
            用于「合并池比 KPI 回测 CSV 多出的无 ret 标的」不占监控 head
    返回：
        新顺序列表
    """
    if not full:
        return []
    defer_to_end = defer_to_end or set()

    def sort_block(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not rows:
            return []
        pred = _learned_proxy_pred(rows, cfg)
        idxs = list(range(len(rows)))
        idxs.sort(
            key=lambda i: (
                -float(pred[i]),
                str(rows[i].get("instrument") or ""),
            )
        )
        return [rows[i] for i in idxs]

    front = [r for r in full if str(r.get("instrument") or "") not in defer_to_end]
    back = [r for r in full if str(r.get("instrument") or "") in defer_to_end]
    return sort_block(front) + back


def _order_candidates_for_export(
    full: List[Dict[str, Any]],
    head: int,
    *,
    order_mode: str,
    blend_vr_tail: int = 0,
    blend_vr_window: int = 120,
    composite_w_score: float = 0.55,
    learned_proxy_config: Optional[Path] = None,
    learned_proxy_defer_to_end: Optional[Set[str]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    决定导出顺序：支持 merge 文件序、score 全局序、composite、learned_proxy。

    参数：
        full: 合并 JSON 的 candidates 全量列表（勿预先截断）
        head: 最终条数上限；≤0 表示保留重排后的全量
        order_mode: merge / score_global / monitor_trend / composite / learned_proxy
        blend_vr_tail: score_global / monitor_trend / composite / learned_proxy 下按 vr 递补
        blend_vr_window: 递补窗口宽度
        learned_proxy_config: learned_proxy 的 JSON；None 时用 DEFAULT_LEARNED_PROXY_CONFIG
        learned_proxy_defer_to_end: 见 _order_by_learned_proxy.defer_to_end
    返回：
        (ordered_rows, stats)
    """
    stats: Dict[str, Any] = {
        "order_mode": order_mode,
        "composite_w_score": composite_w_score,
    }

    combined: List[Dict[str, Any]]

    if order_mode == "learned_proxy":
        cfg_path = learned_proxy_config or DEFAULT_LEARNED_PROXY_CONFIG
        if not cfg_path.is_file():
            raise FileNotFoundError(
                f"order_mode=learned_proxy 需要有效 JSON 配置（--learned-proxy-config），缺失: {cfg_path}"
            )
        cfg = _load_learned_proxy_config(cfg_path)
        stats["learned_proxy_config"] = str(cfg_path.resolve())
        stats["learned_proxy_deferred"] = len(learned_proxy_defer_to_end or set())

        defer = learned_proxy_defer_to_end or set()
        score_sorted = _order_by_learned_proxy(full, cfg, defer_to_end=defer)

        stats["blend_vr_tail"] = max(0, blend_vr_tail)
        stats["blend_vr_window"] = max(0, blend_vr_window)

        if head <= 0:
            combined = score_sorted
        elif stats["blend_vr_tail"] > 0:
            bt = min(stats["blend_vr_tail"], head)
            main_n = max(0, head - bt)
            main = score_sorted[:main_n]
            start = main_n
            end = start + stats["blend_vr_window"]
            band = score_sorted[start:end]
            tail = sorted(band, key=_sort_key_vr_band, reverse=True)[:bt]
            combined = main + tail
        else:
            combined = score_sorted[:head]

        return combined, stats

    if order_mode == "composite":
        score_sorted = _order_composite_blend(full, composite_w_score)

        stats["blend_vr_tail"] = max(0, blend_vr_tail)
        stats["blend_vr_window"] = max(0, blend_vr_window)

        if head <= 0:
            combined = score_sorted
        elif stats["blend_vr_tail"] > 0:
            bt = min(stats["blend_vr_tail"], head)
            main_n = max(0, head - bt)
            main = score_sorted[:main_n]
            start = main_n
            end = start + stats["blend_vr_window"]
            band = score_sorted[start:end]
            tail = sorted(band, key=_sort_key_vr_band, reverse=True)[:bt]
            combined = main + tail
        else:
            combined = score_sorted[:head]

        return combined, stats

    if order_mode in ("score_global", "monitor_trend"):
        score_sorted = sorted(full, key=_sort_key_score_global, reverse=True)

        stats["blend_vr_tail"] = max(0, blend_vr_tail)
        stats["blend_vr_window"] = max(0, blend_vr_window)

        if head <= 0:
            combined = score_sorted
        elif stats["blend_vr_tail"] > 0:
            bt = min(stats["blend_vr_tail"], head)
            main_n = max(0, head - bt)
            main = score_sorted[:main_n]
            start = main_n
            end = start + stats["blend_vr_window"]
            band = score_sorted[start:end]
            tail = sorted(band, key=_sort_key_vr_band, reverse=True)[:bt]
            combined = main + tail
        else:
            combined = score_sorted[:head]

        if order_mode == "monitor_trend":
            # 人选与 score_global 一致，仅重排最终列表顺序（弱趋势、近一周滞涨靠后）
            combined = sorted(combined, key=_sort_key_monitor_trend, reverse=True)

        return combined, stats

    # order_mode == "merge"：合并文件顺序（先 4A 后 4B）
    combined = full if head <= 0 else full[:head]
    return combined, stats


def _pick_code_name(row: Dict[str, Any]) -> tuple[str, str]:
    """
    从合并行中提取监控用的 code 与 name。

    参数：
        row: 合并 JSON 中单条 candidate
    返回：
        (code, name)；code 使用 instrument（与 Moma dm 口径一致，如 600893.SH）
    """
    code = row.get("instrument") or row.get("code") or ""
    name = row.get("stock_name") or row.get("name") or ""
    return str(code), str(name)


def export_targets(
    merged_path: Path,
    head: int,
    out_path: Path,
    dry_run: bool,
    *,
    order_mode: str = "monitor_trend",
    blend_vr_tail: int = 0,
    blend_vr_window: int = 120,
    composite_w_score: float = 0.55,
    learned_proxy_config: Optional[Path] = None,
    learned_proxy_kpi_csv: Optional[Path] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    读取合并 JSON，按策略排序后截取 head 条，写出 monitoring_targets 格式列表。

    参数：
        merged_path: phase4_merged_*.json
        head: 最多导出条数（≤0 表示全部，不推荐用于监控）
        out_path: 输出路径（通常为 results/monitoring_targets.json）
        dry_run: True 时只打印统计不写文件
        order_mode: merge / score_global / monitor_trend / composite / learned_proxy
        composite_w_score: composite 模式下分数秩权重
        learned_proxy_config: learned_proxy 模式下的 JSON 路径
        learned_proxy_kpi_csv: 若提供，将「无有效 ret」的代码置底（与回测 KPI 口径对齐）
        blend_vr_tail: score_global / monitor_trend / composite / learned_proxy 下按 vr 递补只数
        blend_vr_window: 递补扫描窗口
    返回：
        (即将写入的 list[dict], stats)
    """
    data = json.loads(merged_path.read_text(encoding="utf-8"))
    full: List[Dict[str, Any]] = data.get("candidates") or []

    defer: Optional[Set[str]] = None
    if order_mode == "learned_proxy" and learned_proxy_kpi_csv is not None and learned_proxy_kpi_csv.is_file():
        btdf = pd.read_csv(learned_proxy_kpi_csv, encoding="utf-8-sig")
        if "instrument" in btdf.columns and "ret" in btdf.columns:
            valid = set(btdf.dropna(subset=["ret"])["instrument"].astype(str).tolist())
            merged_codes = {str(r.get("instrument") or "") for r in full}
            defer = merged_codes - valid

    ordered, stats = _order_candidates_for_export(
        full,
        head,
        order_mode=order_mode,
        blend_vr_tail=blend_vr_tail
        if order_mode in ("score_global", "monitor_trend", "composite", "learned_proxy")
        else 0,
        blend_vr_window=blend_vr_window,
        composite_w_score=composite_w_score,
        learned_proxy_config=learned_proxy_config,
        learned_proxy_defer_to_end=defer,
    )

    out: List[Dict[str, Any]] = []
    for row in ordered:
        code, name = _pick_code_name(row)
        if not code:
            continue
        item: Dict[str, Any] = {
            "code": code,
            "name": name,
            "concepts": [],
        }
        # 可选元数据，便于排查；stock_monitor 仅使用 code/name/concepts
        if row.get("merge_sources"):
            item["phase4_sources"] = row["merge_sources"]
        if row.get("rank_4a") is not None:
            item["rank_4a"] = row["rank_4a"]
        if row.get("rank_4b") is not None:
            item["rank_4b"] = row["rank_4b"]
        out.append(item)

    if not dry_run:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps(out, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    return out, stats


def main() -> None:
    """CLI 入口。"""
    parser = argparse.ArgumentParser(
        description="从 phase4_merged JSON 导出 results/monitoring_targets.json（截断子集）"
    )
    parser.add_argument(
        "--merged",
        type=Path,
        required=True,
        help="phase4_merge_candidates.py 生成的合并 JSON",
    )
    parser.add_argument(
        "--head",
        type=int,
        default=DEFAULT_HEAD,
        help=f"导出条数上限（默认 {DEFAULT_HEAD}）",
    )
    parser.add_argument(
        "--order-mode",
        choices=("monitor_trend", "score_global", "merge", "composite", "learned_proxy"),
        default="monitor_trend",
        help="monitor_trend=默认：与 score_global 同人选，再按周线多头+近5日涨幅重排；"
        "score_global=纯分数序；merge=合并顺序；composite=实验混合秩；"
        "learned_proxy=截面校准 JSON（见 --learned-proxy-config）",
    )
    parser.add_argument(
        "--learned-proxy-config",
        type=Path,
        default=None,
        help="learned_proxy 模式：系数 JSON；默认 config/monitor_learned_proxy_20251231.json",
    )
    parser.add_argument(
        "--learned-proxy-kpi-csv",
        type=Path,
        default=None,
        help="learned_proxy：回测 CSV；用于将「无有效 ret」的合并池标的置底，与 KPI 口径一致",
    )
    parser.add_argument(
        "--composite-w-score",
        type=float,
        default=0.55,
        help="仅 composite：分数秩权重 0~1，越大越接近 score_global（默认 0.55）",
    )
    parser.add_argument(
        "--blend-vr-tail",
        type=int,
        default=0,
        help="score_global/monitor_trend/composite：先取主排序前 main 段，再从后续窗口按 vr 递补；默认 0",
    )
    parser.add_argument(
        "--blend-vr-window",
        type=int,
        default=150,
        help="vr 递补扫描窗口宽度（从分序断点起向后，默认 150）",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=RESULTS_DIR / "monitoring_targets.json",
        help="输出路径，默认 results/monitoring_targets.json",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只打印条数与前 5 个代码，不写文件",
    )
    args = parser.parse_args()

    if not args.merged.is_file():
        print(f"文件不存在: {args.merged}", file=sys.stderr)
        sys.exit(1)

    rows, stats = export_targets(
        args.merged,
        args.head,
        args.output,
        args.dry_run,
        order_mode=args.order_mode,
        blend_vr_tail=args.blend_vr_tail,
        blend_vr_window=args.blend_vr_window,
        composite_w_score=args.composite_w_score,
        learned_proxy_config=args.learned_proxy_config,
        learned_proxy_kpi_csv=args.learned_proxy_kpi_csv,
    )
    print(
        f"合并文件: {args.merged}  order_mode={args.order_mode}  "
        f"head={args.head if args.head > 0 else '全部'} "
        f"blend_vr_tail={args.blend_vr_tail}"
        + (f" composite_w={args.composite_w_score}" if args.order_mode == "composite" else "")
        + (
            f" learned_proxy={stats.get('learned_proxy_config', '')}"
            if args.order_mode == "learned_proxy"
            else ""
        )
        + f" → 实际导出 {len(rows)} 条"
    )
    if args.dry_run:
        print("dry-run 前 5 个 code:", [r["code"] for r in rows[:5]])
    else:
        print(f"已写入: {args.output}")


if __name__ == "__main__":
    main()
