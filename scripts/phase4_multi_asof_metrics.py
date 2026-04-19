"""
多截面 Phase4 监控 KPI 汇总脚本

用途：
    对若干组「合并池 JSON + 回测 CSV」批量计算与 phase4_simulate_monitor_vs_backtest 一致的指标：
    池内涨幅前 N 只的捕获率（默认 N=50、目标 70%）、以及回测 ret 前 K 名在监控 head 中的命中数。

输入：
    默认尝试扫描 output/ 下已存在的 phase4_merged_* 与对应 *_backtest.csv；
    也可用 --spec 多次指定：merged路径|backtest路径|标签

输出：
    默认写入 output/phase4_multi_asof_metrics.csv（UTF-8-SIG，便于 Excel）。

运行示例：
    python scripts/phase4_multi_asof_metrics.py
    python scripts/phase4_multi_asof_metrics.py --head 300
    python scripts/phase4_multi_asof_metrics.py --spec "output/a.json|output/a_backtest.csv|v1.5"
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts.phase4_simulate_monitor_vs_backtest import (  # noqa: E402
    _capture_count,
    _gainer_set_from_df,
    _monitor_instruments,
)


def _default_specs(root: Path) -> list[tuple[Path, Path, str]]:
    """
    构造默认待评估文件组（若磁盘上存在则加入）。

    参数：
        root: 项目根目录
    返回：
        (merged_path, backtest_csv, label) 列表
    """
    out = root / "output"
    candidates: list[tuple[Path, Path, str]] = []

    for merged in sorted(out.glob("phase4_merged_*.json")):
        if "v15" in merged.name:
            tag = "v1.5"
        else:
            tag = "baseline"
        stem = merged.with_suffix("")
        bt = Path(str(stem) + "_backtest.csv")
        if bt.is_file():
            candidates.append((merged, bt, f"{tag}_{merged.stem}"))

    return candidates


def _parse_spec(s: str) -> tuple[Path, Path, str]:
    """
    解析 --spec 字符串。

    参数：
        s: 格式 merged.json|backtest.csv|label
    返回：
        (merged_path, backtest_path, label)
    """
    parts = s.split("|")
    if len(parts) != 3:
        raise ValueError(f"--spec 需为 merged|backtest|label 三段，用 | 分隔，得到: {s!r}")
    return Path(parts[0].strip()), Path(parts[1].strip()), parts[2].strip()


def _row_metrics(
    merged: Path,
    backtest_csv: Path,
    *,
    head: int,
    gainer_top_n: int,
    target_capture: float,
    top_winners: int,
    order_mode: str,
    blend_vr_tail: int,
    blend_vr_window: int,
    composite_w_score: float,
) -> dict[str, object]:
    """
    计算单行指标。

    参数：
        merged: phase4_merged_*.json
        backtest_csv: 对应 backtest CSV
        head: 监控截断只数
        gainer_top_n: 涨幅「大」集合取前 N
        target_capture: 目标捕获比例（相对 gainer_top_n）
        top_winners: 强势股样本前 K
        order_mode: score_global / merge / composite
        其余：与导出/模拟脚本一致
    返回：
        字段字典，供写 CSV
    """
    data = json.loads(merged.read_text(encoding="utf-8"))
    as_of = str(data.get("as_of") or "")
    pool_size = len(data.get("candidates") or [])

    df = pd.read_csv(backtest_csv, encoding="utf-8-sig")
    if "instrument" not in df.columns or "ret" not in df.columns:
        raise ValueError(f"{backtest_csv} 需含 instrument、ret 列")
    df = df.dropna(subset=["ret"]).sort_values("ret", ascending=False).reset_index(drop=True)

    gainer_codes, gainer_n = _gainer_set_from_df(df, gainer_top_n)
    need = min(len(gainer_codes), int(math.ceil(target_capture * gainer_n)))
    cap_now = _capture_count(
        merged,
        head,
        gainer_codes,
        order_mode=order_mode,
        blend_vr_tail=blend_vr_tail,
        blend_vr_window=blend_vr_window,
        composite_w_score=composite_w_score,
    )

    monitor_set, _ = _monitor_instruments(
        merged,
        head,
        order_mode=order_mode,
        blend_vr_tail=blend_vr_tail,
        blend_vr_window=blend_vr_window,
        composite_w_score=composite_w_score,
    )

    k = max(1, top_winners)
    top_codes = df.head(k)["instrument"].astype(str).tolist()
    top_hits = sum(1 for c in top_codes if c in monitor_set)

    ratio = cap_now / gainer_n if gainer_n else 0.0

    return {
        "tag": "",  # 由调用方填入
        "as_of": as_of,
        "merged_file": str(merged.as_posix()),
        "pool_size": pool_size,
        "head": head,
        "order_mode": order_mode,
        "gainer_top_n": gainer_n,
        "cap_in_top_gainers": cap_now,
        "cap_ratio": round(ratio, 4),
        "target_need": need,
        "cap_ok_vs_target": cap_now >= need,
        "topk": k,
        "topk_hits_in_monitor": top_hits,
    }


def main() -> None:
    """命令行入口：解析参数、汇总指标并写出 CSV。"""
    parser = argparse.ArgumentParser(description="多截面 Phase4 监控 KPI 汇总")
    parser.add_argument(
        "--spec",
        action="append",
        default=None,
        help="merged.json|backtest.csv|展示标签，可多次传入；不传则自动发现 output/phase4_merged_*",
    )
    parser.add_argument("--head", type=int, default=300)
    parser.add_argument("--gainer-top-n", type=int, default=50)
    parser.add_argument("--target-capture", type=float, default=0.7)
    parser.add_argument("--top-winners", type=int, default=10)
    parser.add_argument(
        "--order-mode",
        default="monitor_trend",
        choices=("monitor_trend", "score_global", "merge", "composite"),
    )
    parser.add_argument("--blend-vr-tail", type=int, default=0)
    parser.add_argument("--blend-vr-window", type=int, default=150)
    parser.add_argument("--composite-w-score", type=float, default=0.55)
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=ROOT / "output" / "phase4_multi_asof_metrics.csv",
    )
    args = parser.parse_args()

    specs: list[tuple[Path, Path, str]] = []
    if args.spec:
        for s in args.spec:
            m, b, lab = _parse_spec(s)
            specs.append((m, b, lab))
    else:
        specs = _default_specs(ROOT)

    if not specs:
        print("未找到可评估的 merged + backtest 文件对", file=sys.stderr)
        sys.exit(1)

    rows_out: list[dict[str, object]] = []
    for merged, bt, display_tag in specs:
        if not merged.is_file():
            print(f"跳过（无 merged）: {merged}", file=sys.stderr)
            continue
        if not bt.is_file():
            print(f"跳过（无 backtest）: {bt}", file=sys.stderr)
            continue
        row = _row_metrics(
            merged,
            bt,
            head=args.head,
            gainer_top_n=args.gainer_top_n,
            target_capture=args.target_capture,
            top_winners=args.top_winners,
            order_mode=args.order_mode,
            blend_vr_tail=args.blend_vr_tail,
            blend_vr_window=args.blend_vr_window,
            composite_w_score=args.composite_w_score,
        )
        row["tag"] = display_tag
        rows_out.append(row)

    if not rows_out:
        print("没有成功计算任何一行", file=sys.stderr)
        sys.exit(1)

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "tag",
        "as_of",
        "merged_file",
        "pool_size",
        "head",
        "order_mode",
        "gainer_top_n",
        "cap_in_top_gainers",
        "cap_ratio",
        "target_need",
        "cap_ok_vs_target",
        "topk",
        "topk_hits_in_monitor",
    ]
    with open(args.output_csv, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows_out:
            w.writerow(r)

    print(f"已写入: {args.output_csv}（{len(rows_out)} 行）")
    for r in rows_out:
        print(
            f"  [{r['tag']}] as_of={r['as_of']} pool={r['pool_size']} "
            f"cap50={r['cap_in_top_gainers']}/{r['gainer_top_n']} "
            f"top{r['topk']}={r['topk_hits_in_monitor']}/{r['topk']} "
            f"达标={'Y' if r['cap_ok_vs_target'] else 'N'}"
        )


if __name__ == "__main__":
    main()
