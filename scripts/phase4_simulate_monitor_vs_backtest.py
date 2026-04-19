"""
模拟「操作指南」流程：合并池 → 导出监控 N 只 → 与回测涨幅榜对比。

用途：
    1) 快速：回测 ret 前 K 名中有几只落在当前 head 的监控集合（--top-winners）。
    2) KPI：在合并池内定义「涨幅大」= ret 排名前 N 只（默认 N=50），计算捕获率是否达到
       --target-capture（默认 0.7），并用二分求「至少需监控多少只」才能达到该比例。

注意：
    - 「涨幅大」仅指本合并池内有有效 ret 的标的中的涨幅排名，不是全市场。
    - 排序规则须与 phase4_export_monitoring_targets 一致（order_mode / blend）。

运行示例：
    python scripts/phase4_simulate_monitor_vs_backtest.py \\
        --merged output/phase4_merged_20240524.json \\
        --backtest-csv output/phase4_merged_20240524_backtest.csv \\
        --head 100 --top-winners 10

    python scripts/phase4_simulate_monitor_vs_backtest.py \\
        --merged output/phase4_merged_20240524.json \\
        --backtest-csv output/phase4_merged_20240524_backtest.csv \\
        --head 100 --gainer-top-n 50 --target-capture 0.7 --solve-min-head
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
import math
from typing import Any, Dict, List, Set, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts.phase4_export_monitoring_targets import (
    DEFAULT_LEARNED_PROXY_CONFIG,
    _order_candidates_for_export,
)


def _monitor_instruments(
    merged_path: Path,
    head: int,
    order_mode: str,
    blend_vr_tail: int = 0,
    blend_vr_window: int = 120,
    composite_w_score: float = 0.55,
    learned_proxy_config: Path | None = None,
    learned_proxy_defer_to_end: Set[str] | None = None,
) -> tuple[Set[str], Dict[str, Any]]:
    """
    按与导出脚本相同的规则，得到监控名单中的 instrument 集合。

    参数：
        merged_path: phase4_merged_*.json
        head: 监控只数（如 50）
        order_mode: score_global / merge / composite / learned_proxy
    返回：
        (codes, stats) — stats 来自 _order_candidates_for_export
    """
    data = json.loads(merged_path.read_text(encoding="utf-8"))
    full: List[Dict[str, Any]] = data.get("candidates") or []
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
        learned_proxy_defer_to_end=learned_proxy_defer_to_end,
    )
    codes = {r["instrument"] for r in ordered if r.get("instrument")}
    return codes, stats


def _gainer_set_from_df(df: pd.DataFrame, gainer_top_n: int) -> Tuple[Set[str], int]:
    """
    用回测 CSV 定义「涨幅大」集合：ret 降序前 min(gainer_top_n, 有效行数) 只。

    返回：
        (instrument 集合, 实际 N)
    """
    n = min(max(1, gainer_top_n), len(df))
    top = df.head(n)
    return set(top["instrument"].astype(str).tolist()), n


def _capture_count(
    merged_path: Path,
    head: int,
    gainer_codes: Set[str],
    *,
    order_mode: str,
    blend_vr_tail: int,
    blend_vr_window: int,
    composite_w_score: float = 0.55,
    learned_proxy_config: Path | None = None,
    learned_proxy_defer_to_end: Set[str] | None = None,
) -> int:
    """监控 head 只与涨幅集合的交集数量。"""
    mon, _ = _monitor_instruments(
        merged_path,
        head,
        order_mode=order_mode,
        blend_vr_tail=blend_vr_tail,
        blend_vr_window=blend_vr_window,
        composite_w_score=composite_w_score,
        learned_proxy_config=learned_proxy_config,
        learned_proxy_defer_to_end=learned_proxy_defer_to_end,
    )
    return len(gainer_codes & mon)


def _min_head_for_capture(
    merged_path: Path,
    pool_size: int,
    gainer_codes: Set[str],
    need: int,
    *,
    order_mode: str,
    blend_vr_tail: int,
    blend_vr_window: int,
    composite_w_score: float = 0.55,
    learned_proxy_config: Path | None = None,
    learned_proxy_defer_to_end: Set[str] | None = None,
) -> int:
    """
    二分求最小 head，使 |监控(head) ∩ G| >= need。

    参数：
        pool_size: 合并池候选数量（head 上界）
        gainer_codes: 涨幅集合 G
        need: 至少需捕获只数（如 ceil(0.7*|G|)）
    返回：
        最小 head；若 pool_size 全选仍不足 need，返回 pool_size（调用方需提示不可达）
    """
    if need <= 0:
        return 0

    def ok(h: int) -> bool:
        return _capture_count(
            merged_path,
            h,
            gainer_codes,
            order_mode=order_mode,
            blend_vr_tail=blend_vr_tail,
            blend_vr_window=blend_vr_window,
            composite_w_score=composite_w_score,
            learned_proxy_config=learned_proxy_config,
            learned_proxy_defer_to_end=learned_proxy_defer_to_end,
        ) >= need

    if not ok(pool_size):
        return pool_size

    lo, hi = 1, pool_size
    while lo < hi:
        mid = (lo + hi) // 2
        if ok(mid):
            hi = mid
        else:
            lo = mid + 1
    return lo


def _defer_codes_missing_ret(merged_path: Path, df_with_ret: pd.DataFrame) -> Set[str]:
    """
    合并池相对「含有效 ret」的回测表多出的代码（无 KPI 可比），用于 learned_proxy 排序置底。

    参数：
        merged_path: phase4_merged JSON
        df_with_ret: 已 dropna(subset=[\"ret\"]) 的回测 DataFrame
    返回：
        应置底的 instrument 集合
    """
    data = json.loads(merged_path.read_text(encoding="utf-8"))
    full: List[Dict[str, Any]] = data.get("candidates") or []
    merged_codes = {str(r.get("instrument") or "") for r in full if r.get("instrument")}
    valid = set(df_with_ret["instrument"].astype(str).tolist())
    return merged_codes - valid


def main() -> None:
    """CLI：对比监控子集与回测涨幅前列的覆盖情况。"""
    parser = argparse.ArgumentParser(
        description="模拟监控 head 只 vs 回测涨幅前 K 名的覆盖统计"
    )
    parser.add_argument("--merged", type=Path, required=True, help="phase4_merged_*.json")
    parser.add_argument(
        "--backtest-csv",
        type=Path,
        required=True,
        help="phase4_backtest 生成的 *_backtest.csv",
    )
    parser.add_argument("--head", type=int, default=300, help="监控列表只数，默认 300（与导出脚本 DEFAULT_HEAD 对齐）")
    parser.add_argument(
        "--top-winners",
        type=int,
        default=10,
        help="取回测 ret 降序前 K 名作为「强势股样本」，默认 10",
    )
    parser.add_argument(
        "--order-mode",
        choices=("monitor_trend", "score_global", "merge", "composite", "learned_proxy"),
        default="monitor_trend",
        help="与 phase4_export_monitoring_targets 一致，默认 monitor_trend",
    )
    parser.add_argument(
        "--learned-proxy-config",
        type=Path,
        default=None,
        help="learned_proxy：系数 JSON；默认使用项目 config/monitor_learned_proxy_20251231.json",
    )
    parser.add_argument(
        "--composite-w-score",
        type=float,
        default=0.55,
        help="order_mode=composite 时分数秩权重（与导出脚本一致，默认 0.55）",
    )
    parser.add_argument("--blend-vr-tail", type=int, default=0)
    parser.add_argument("--blend-vr-window", type=int, default=150)
    parser.add_argument(
        "--gainer-top-n",
        type=int,
        default=50,
        help="KPI：定义「涨幅大」= 池内 ret 排名前 N（默认 50），用于捕获率",
    )
    parser.add_argument(
        "--target-capture",
        type=float,
        default=0.7,
        help="KPI：希望至少捕获涨幅集合中多少比例（默认 0.7 即 70%%）",
    )
    parser.add_argument(
        "--solve-min-head",
        action="store_true",
        help="二分求达到 target-capture 的最小 head（打印结果）",
    )
    parser.add_argument(
        "--strict-target",
        action="store_true",
        help="若当前 --head 未达到 target-capture 则 exit 1（供自动化检查）",
    )
    args = parser.parse_args()

    if not args.merged.is_file():
        print(f"找不到合并 JSON: {args.merged}", file=sys.stderr)
        sys.exit(1)
    if not args.backtest_csv.is_file():
        print(f"找不到回测 CSV: {args.backtest_csv}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(args.backtest_csv, encoding="utf-8-sig")
    if "instrument" not in df.columns or "ret" not in df.columns:
        print("CSV 需含 instrument、ret 列", file=sys.stderr)
        sys.exit(1)
    df_valid_ret = df.dropna(subset=["ret"])
    df = df_valid_ret.sort_values("ret", ascending=False).reset_index(drop=True)

    lp_cfg = args.learned_proxy_config
    if args.order_mode == "learned_proxy" and lp_cfg is None:
        lp_cfg = DEFAULT_LEARNED_PROXY_CONFIG

    learned_defer = (
        _defer_codes_missing_ret(args.merged, df_valid_ret)
        if args.order_mode == "learned_proxy"
        else None
    )

    monitor_set, _ = _monitor_instruments(
        args.merged,
        args.head,
        order_mode=args.order_mode,
        blend_vr_tail=args.blend_vr_tail,
        blend_vr_window=args.blend_vr_window,
        composite_w_score=args.composite_w_score,
        learned_proxy_config=lp_cfg,
        learned_proxy_defer_to_end=learned_defer,
    )

    data = json.loads(args.merged.read_text(encoding="utf-8"))
    pool_size = len(data.get("candidates") or [])

    gainer_codes, gainer_n = _gainer_set_from_df(df, args.gainer_top_n)
    need = min(
        len(gainer_codes),
        int(math.ceil(args.target_capture * gainer_n)),
    )
    cap_now = _capture_count(
        args.merged,
        args.head,
        gainer_codes,
        order_mode=args.order_mode,
        blend_vr_tail=args.blend_vr_tail,
        blend_vr_window=args.blend_vr_window,
        composite_w_score=args.composite_w_score,
        learned_proxy_config=lp_cfg,
        learned_proxy_defer_to_end=learned_defer,
    )
    ratio_now = cap_now / gainer_n if gainer_n else 0.0

    print("=== KPI：池内「涨幅大」集合 vs 监控捕获率 ===")
    print(
        f"定义：合并池内有 ret 的标的中，涨幅排名前 {gainer_n} 只（--gainer-top-n 上限受池子大小限制）"
    )
    print(f"目标捕获比例: {args.target_capture:.0%} → 至少需命中 {need} / {gainer_n} 只")
    print(
        f"当前 --head={args.head}: 命中 {cap_now} / {gainer_n} = {ratio_now:.1%} "
        f"({'达标' if cap_now >= need else '未达标'})"
    )
    print()

    if args.solve_min_head:
        h_min = _min_head_for_capture(
            args.merged,
            pool_size,
            gainer_codes,
            need,
            order_mode=args.order_mode,
            blend_vr_tail=args.blend_vr_tail,
            blend_vr_window=args.blend_vr_window,
            composite_w_score=args.composite_w_score,
            learned_proxy_config=lp_cfg,
            learned_proxy_defer_to_end=learned_defer,
        )
        cap_full = _capture_count(
            args.merged,
            pool_size,
            gainer_codes,
            order_mode=args.order_mode,
            blend_vr_tail=args.blend_vr_tail,
            blend_vr_window=args.blend_vr_window,
            composite_w_score=args.composite_w_score,
            learned_proxy_config=lp_cfg,
            learned_proxy_defer_to_end=learned_defer,
        )
        if cap_full < need:
            print(
                f"[结论] 即使监控整池 {pool_size} 只，涨幅前 {gainer_n} 中最多命中 {cap_full} 只，"
                f"无法达到 {need} 只（{args.target_capture:.0%}）。需改排序/特征或放宽 KPI。"
            )
        else:
            print(
                f"[结论] 达到 {args.target_capture:.0%}（≥{need}/{gainer_n}）所需最小 head ≈ {h_min} "
                f"（在当前 order_mode / 排序规则下）。"
            )
        print()

    k = max(1, args.top_winners)
    topk = df.head(k)
    top_codes = topk["instrument"].astype(str).tolist()

    in_monitor = [c for c in top_codes if c in monitor_set]
    missing = [c for c in top_codes if c not in monitor_set]

    print("=== 模拟：操作指南「合并 → 监控 head 只」vs 回测强势股 ===")
    print(f"合并文件     : {args.merged}")
    print(f"回测 CSV     : {args.backtest_csv}")
    print(f"监控只数 head: {args.head}")
    print(f"order_mode     : {args.order_mode}")
    if args.order_mode == "composite":
        print(f"composite_w_score: {args.composite_w_score}")
    if args.order_mode == "learned_proxy":
        print(f"learned_proxy_config: {lp_cfg}")
        if learned_defer:
            print(f"learned_proxy 置底无 ret 标的: {len(learned_defer)} 只 {sorted(learned_defer)}")
    print(f"blend_vr_tail  : {args.blend_vr_tail}  window={args.blend_vr_window}")
    print(f"强势股样本   : 回测 ret 排名前 {k} 名（共 {len(topk)} 只有效数据）")
    print()
    print(f"监控名单内命中: {len(in_monitor)} / {k}")
    if in_monitor:
        print("  命中代码:", ", ".join(in_monitor))
    if missing:
        print(f"  未纳入监控（前{k}名中）: {len(missing)} 只")
        for code in missing:
            row = topk[topk["instrument"] == code].iloc[0]
            name = row.get("stock_name", row.get("name", ""))
            ret = row["ret"]
            print(f"    - {code}  {name}  ret={ret:+.2f}%")

    if args.strict_target and cap_now < need:
        sys.exit(1)


if __name__ == "__main__":
    main()
