"""选股特征-胜率统计报告生成器（两种成败口径并列对比）。

== 定位 ==
读取 ``validate_selections.py`` 产出的样本 CSV（每行 = 选股日×股票，含全部特征 +
5/10/20 日前向收益/最大浮盈/最大回撤/两口径成败标签），生成一份 Markdown 报告。
**只读分析，不改 quant_picker 逻辑。**

报告对每个分组同时给出两种口径的胜率，便于对比：
  - **可持有胜率** (success_hold_H): 窗口末累计收益 ≥ 阈值 且 最大回撤 ≤ 阈值（“拿得住”）；
  - **冲一波胜率** (success_mg_H): 窗口内最大浮盈 ≥ 阈值（“期间能否冲出一波涨幅”）。

两者口径在 ``validate_selections.py`` 生成 CSV 时已固定，本脚本仅做聚合展示。

== 运行方式 ==
    python scripts/report_selection_stats.py
    python scripts/report_selection_stats.py --csv output/selection_validation_regen.csv \
        --out output/selection_report.md --primary 10

注意:
    - 某周期 H 的统计只纳入 ``days_used_H >= H`` 的样本（行情走满），避免“没走完”污染。
    - 连续型特征按固定分箱；样本量小的分箱（n 偏小）仅作线索，勿当定论。
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Callable, List, Tuple

import numpy as np
import pandas as pd

# 特征分桶定义：返回 [(分组标题, [(子项标签, 布尔mask), ...]), ...]
BucketDef = Tuple[str, List[Tuple[str, pd.Series]]]


def _safe(df: pd.DataFrame, col: str) -> pd.Series:
    """取列，列不存在时返回全 NaN 的 Series（避免 KeyError）。"""
    if col in df.columns:
        return df[col]
    return pd.Series([np.nan] * len(df), index=df.index)


def metrics(sub: pd.DataFrame, H: int) -> dict:
    """计算子集在周期 H 的两口径胜率与收益/回撤均值。

    参数:
        sub: 样本子集。
        H:   周期（交易日）。

    返回:
        dict: n / hold_wr / mg_wr / avg_ret / avg_maxgain / avg_maxdd；
        仅纳入 days_used_H>=H 且 success_hold_H 非空的样本。
    """
    used = _safe(sub, f"days_used_{H}")
    valid = sub[used >= H].dropna(subset=[f"success_hold_{H}"])
    n = len(valid)
    if n == 0:
        return {"n": 0, "hold_wr": None, "mg_wr": None,
                "avg_ret": None, "avg_maxgain": None, "avg_maxdd": None}
    return {
        "n": n,
        "hold_wr": valid[f"success_hold_{H}"].mean() * 100,
        "mg_wr": valid[f"success_mg_{H}"].mean() * 100,
        "avg_ret": valid[f"ret_{H}"].mean(),
        "avg_maxgain": valid[f"maxgain_{H}"].mean(),
        "avg_maxdd": valid[f"maxdd_{H}"].mean(),
    }


def fmt_row(label: str, m: dict) -> str:
    """把一行指标格式化为 Markdown 表格行。"""
    if m["n"] == 0:
        return f"| {label} | 0 | - | - | - | - | - |"
    return (f"| {label} | {m['n']} | {m['hold_wr']:.1f}% | {m['mg_wr']:.1f}% | "
            f"{m['avg_ret']:+.1f}% | {m['avg_maxgain']:+.1f}% | {m['avg_maxdd']:.1f}% |")


TABLE_HEADER = ("| 分组 | n | 可持有胜率 | 冲一波胜率 | 均末收益 | 均最大浮盈 | 均最大回撤 |\n"
                "|---|---:|---:|---:|---:|---:|---:|")


def make_buckets(df: pd.DataFrame) -> List[BucketDef]:
    """基于样本表构造所有特征分桶定义。"""
    out: List[BucketDef] = []

    out.append(("锚点日是否涨停（追板）", [
        ("锚点涨停", _safe(df, "anchor_is_limit_up") == True),
        ("锚点未涨停", _safe(df, "anchor_is_limit_up") == False),
    ]))
    run2 = _safe(df, "anchor_run2")
    out.append(("进场前2日累计涨幅", [
        ("≥18%（过热）", run2 >= 18),
        ("10~18%", (run2 >= 10) & (run2 < 18)),
        ("<10%", run2 < 10),
    ]))
    rs = _safe(df, "rebound_score")
    out.append(("rebound_score 分段", [
        ("≥50", rs >= 50),
        ("35~50", (rs >= 35) & (rs < 50)),
        ("<35", rs < 35),
    ]))
    dsr = _safe(df, "days_since_rebound")
    out.append(("days_since_rebound", [
        ("≤2 天（刚反弹）", dsr <= 2),
        ("3~5 天", (dsr >= 3) & (dsr <= 5)),
        (">5 天", dsr > 5),
    ]))
    vol = _safe(df, "m_vol_expand_ratio")
    out.append(("月线量能扩张比", [
        ("≥1（放量）", vol >= 1),
        ("<1（缩量）", vol < 1),
    ]))
    cvp = _safe(df, "close_vs_3y_peak")
    out.append(("距3年高点（close_vs_3y_peak）", [
        ("≥1.0（站上/接近）", cvp >= 1.0),
        ("0.9~1.0", (cvp >= 0.9) & (cvp < 1.0)),
        ("<0.9（仍远离）", cvp < 0.9),
    ]))
    fs = _safe(df, "final_score")
    out.append(("final_score 分段", [
        ("≥150", fs >= 150),
        ("120~150", (fs >= 120) & (fs < 150)),
        ("<120", fs < 120),
    ]))
    # 形态
    pats = [p for p in df["pattern"].dropna().unique()] if "pattern" in df else []
    out.append(("形态类型", [(str(p), df["pattern"] == p) for p in pats]))
    # 质量标签
    tag_cols = [c for c in df.columns if c.startswith("tag_")]
    out.append(("质量标签（命中=True）", [(c.replace("tag_", ""), df[c] == True) for c in tag_cols]))
    return out


def make_combos(df: pd.DataFrame) -> List[Tuple[str, pd.Series]]:
    """构造若干“特征组合”策略的 mask（验证叠加效果）。"""
    pat_strong = df["pattern"].isin(["N型: 强势整理后再起", "H型: 高位横盘强者恒强"]) \
        if "pattern" in df else pd.Series(False, index=df.index)
    t_peak = _safe(df, "tag_站稳3年高点") == True
    t_newhigh = _safe(df, "tag_反弹后仍创新高") == True
    t_nobreak = _safe(df, "tag_未突破3年高点") == True
    t_nonewhigh = _safe(df, "tag_反弹后未创新高") == True
    return [
        ("强势形态(N/H) + 站稳3年高点 + 反弹后仍创新高", pat_strong & t_peak & t_newhigh),
        ("强势形态(N/H) + 反弹后仍创新高", pat_strong & t_newhigh),
        ("仅 反弹后仍创新高", t_newhigh),
        ("弱势(未破3年高点 或 反弹后未创新高)", t_nobreak | t_nonewhigh),
        ("最弱(未突破3年高点)", t_nobreak),
    ]


def build_report(df: pd.DataFrame, horizons: List[int], primary: int) -> str:
    """生成 Markdown 报告文本。"""
    lines: List[str] = []
    ap = lines.append

    ap("# 选股结果事后验证报告（两口径对比）\n")
    ap(f"- 样本来源：`{len(df)}` 条（选股日 × 股票），覆盖选股日："
       f"{', '.join(sorted(df['select_date'].unique()))}")
    ap("- **可持有胜率**：窗口末累计收益 ≥ 阈值 且 最大回撤 ≤ 阈值（拿得住、赚得到）")
    ap("- **冲一波胜率**：窗口内最大浮盈 ≥ 阈值（期间能否冲出一波）")
    ap("- 说明：每个周期 H 只统计行情走满（days_used≥H）的样本；小样本分组仅作线索。\n")

    # ── 概览：各周期全样本 ──
    ap("## 一、总览（全样本，各周期）\n")
    ap(TABLE_HEADER)
    for H in horizons:
        ap(fmt_row(f"H={H} 交易日", metrics(df, H)))
    ap("")

    # ── 分选股日（市场环境）──
    ap(f"## 二、分选股日 / 市场环境（H={primary}）\n")
    ap("> 同一套规则在不同市场环境下胜率天差地别，**择时（大盘）影响往往大于选股特征本身**。\n")
    ap(TABLE_HEADER)
    for dt, g in df.groupby("select_date"):
        ap(fmt_row(dt, metrics(g, primary)))
    ap("")

    # ── 各特征分桶 ──
    ap(f"## 三、各特征分桶（H={primary}）\n")
    for title, pairs in make_buckets(df):
        ap(f"### {title}\n")
        ap(TABLE_HEADER)
        for label, mask in pairs:
            ap(fmt_row(label, metrics(df[mask], primary)))
        ap("")

    # ── 组合策略 ──
    ap(f"## 四、特征组合策略（H={primary}）\n")
    ap(TABLE_HEADER)
    for label, mask in make_combos(df):
        ap(fmt_row(label, metrics(df[mask], primary)))
    ap("")

    # ── 关键特征跨周期（可持有 vs 冲一波）──
    ap("## 五、关键正/负特征跨周期对比\n")
    key_masks = [
        ("强势形态(N/H)+创新高+站稳高点",
         (df["pattern"].isin(["N型: 强势整理后再起", "H型: 高位横盘强者恒强"]) if "pattern" in df else False)
         & (_safe(df, "tag_站稳3年高点") == True) & (_safe(df, "tag_反弹后仍创新高") == True)),
        ("W底 双底结构", df.get("pattern") == "W底: 双底结构" if "pattern" in df else pd.Series(False, index=df.index)),
        ("未突破3年高点", _safe(df, "tag_未突破3年高点") == True),
    ]
    for name, mask in key_masks:
        ap(f"### {name}\n")
        ap(TABLE_HEADER)
        for H in horizons:
            ap(fmt_row(f"H={H}", metrics(df[mask], H)))
        ap("")

    ap("---")
    ap("*本报告为规则化形态/量价的客观统计，非投资建议。样本与市场环境有限，结论需持续累积验证。*")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--csv", default="output/selection_validation_regen.csv", help="验证样本 CSV")
    parser.add_argument("--out", default="output/selection_report.md", help="输出 Markdown 报告")
    parser.add_argument("--horizons", type=int, nargs="+", default=[5, 10, 20], help="周期列表")
    parser.add_argument("--primary", type=int, default=10, help="主周期（分桶用）")
    args = parser.parse_args()

    df = pd.read_csv(args.csv)
    report = build_report(df, args.horizons, args.primary)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(report, encoding="utf-8")
    print(f"[done] 报告已写出: {out_path}  ({len(report.splitlines())} 行)")


if __name__ == "__main__":
    main()
