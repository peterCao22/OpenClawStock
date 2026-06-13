"""选股结果「事后验证」样本收集 + 特征-胜率统计脚本。

== 定位 ==
这是一个 **只读分析** 脚本，**不修改 quant_picker.py 的任何逻辑**。它把历史各选股日的
``results/top_50_stocks_YYYY-MM-DD.json`` 快照逐只票拿出来，用 ``kline_qfq`` 计算每只票
在锚点（W1.end_date）之后多个周期（5/10/20 交易日）的前向表现，并把 quant_picker 当初
算出的全部特征一并带上，汇总成一张大表（CSV）。目的：

  1) 攒够样本（多个选股日 × 每日约 50 只 ≈ 数百样本），避免“看一两只就改核心逻辑”；
  2) 用统计（按特征分桶看胜率/平均最大浮盈/平均回撤）找出真正区分正反例的特征，
     为后续调 quant_picker 提供证据。

== 成败标签 ==
对每个周期 H ∈ {5,10,20} 同时算两种口径，CSV 都落列，统计主口径由 --label 选：
  - ``success_hold_H`` (默认, holdable): 窗口末累计收益 ≥ --hold-ret-thresh(默认5%)
    **且** 窗口内最大回撤 ≥ -(--hold-maxdd-thresh)(默认12%) —— 即“拿得住、赚得到”；
  - ``success_mg_H`` (maxgain): 窗口内最大浮盈 ≥ --maxgain-thresh(默认10%) —— 即“能否冲一波”。

== 关键派生特征 ==
  - ``anchor_day_chg``   : 锚点日自身单日涨幅（用锚点日 / 前一交易日收盘）
  - ``anchor_run2``      : 进入锚点的 2 日累计涨幅（锚点日 / 前 2 根收盘）
  - ``anchor_is_limit_up``: 锚点日自身是否涨停（按板块阈值）——验证“选在加速末端”假设
  - 其余特征（各项 score、quality_tags、monthly.* 等）直接取自选股 JSON

== 运行方式 ==
    # 默认：扫所有 results/top_50_stocks_<date>.json，输出 output/selection_validation.csv
    python scripts/validate_selections.py

    # 自定义周期 / 成功阈值 / 输出
    python scripts/validate_selections.py --horizons 5 10 20 --maxgain-thresh 10 \
        --out output/selection_validation.csv

注意:
    - 锚点后行情不足 H 根的样本（如最近的选股日），对应周期标 days_used<H，统计时按
      ``--min-coverage`` 过滤（默认要求该周期 bar 齐全才纳入该周期统计），避免“没走完”污染。
    - 默认排除带 _before_b4 / _reranked / -- 等后缀的非正式快照。
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from scripts.db_session import engine
from scripts.rerank_top_stocks import get_limit_thresh, get_w1_end_date

DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")
EXCLUDE_TOKENS = ("_before_b4", "_reranked", "--")


# ─── 选股快照发现 ────────────────────────────────────────────────────────────
def discover_snapshots(results_dir: Path, prefix: str) -> List[Path]:
    """找出正式的选股快照文件并按日期排序。

    参数:
        results_dir: results 目录。
        prefix:      文件名前缀，如 ``top_50_stocks_``。

    返回:
        合规的快照路径列表（排除 _before_b4 / _reranked / -- 等非正式产物）。
    """
    files = []
    for p in sorted(results_dir.glob(f"{prefix}*.json")):
        name = p.name
        if any(tok in name for tok in EXCLUDE_TOKENS):
            continue
        if not DATE_RE.search(name):
            continue
        files.append(p)
    return files


# ─── 日线批量取数 ────────────────────────────────────────────────────────────
def fetch_daily_window(instruments: List[str], start_date: str, lookahead_rows: int) -> pd.DataFrame:
    """批量取若干票自 start_date 前几日起的日线（一次查询省 RTT）。

    多向前取 ~10 个日历日，用于计算锚点日自身涨幅 / 进场 2 日涨幅。

    参数:
        instruments:    标的代码列表。
        start_date:     最早锚点日（YYYY-MM-DD）。
        lookahead_rows: 仅用于注释；实际不在 SQL 限制行数，按票切片时再取 head。

    返回:
        含 instrument/date/open/high/low/close/volume 的 DataFrame（升序）。
    """
    if not instruments:
        return pd.DataFrame()
    # 向前留 15 个日历日缓冲，确保能取到锚点日的前 1~2 根 bar
    start_buf = (pd.Timestamp(start_date) - pd.Timedelta(days=15)).strftime("%Y-%m-%d")
    placeholders = ",".join(f"'{i}'" for i in set(instruments))
    sql = (
        "SELECT instrument, date, open, high, low, close, volume FROM kline_qfq "
        f"WHERE instrument IN ({placeholders}) AND date >= '{start_buf}' "
        "ORDER BY instrument, date ASC"
    )
    df = pd.read_sql(sql, engine)
    df["date"] = pd.to_datetime(df["date"])
    return df


# ─── 单票前向指标 ────────────────────────────────────────────────────────────
def compute_outcomes(df_inst: pd.DataFrame, anchor_date: str, instrument: str,
                     horizons: List[int], maxgain_thresh: float,
                     hold_ret_thresh: float, hold_maxdd_thresh: float) -> dict:
    """计算单票自锚点日起多周期前向表现 + 锚点自身特征。

    参数:
        df_inst:        该票的日线（升序，含锚点前缓冲）。
        anchor_date:    锚点日字符串。
        instrument:     代码（用于板块涨停阈值）。
        horizons:       周期列表（交易日数）。
        maxgain_thresh: maxgain 口径成功阈值（窗口内最大浮盈 ≥ 该值，单位 %）。
        hold_ret_thresh:  可持有口径：窗口末累计收益 ≥ 该值（%）。
        hold_maxdd_thresh: 可持有口径：窗口内最大回撤 ≥ -该值（%，传正数）。

    返回:
        dict：含 anchor_close / anchor_day_chg / anchor_run2 / anchor_is_limit_up，
        以及每周期 ret_H / maxgain_H / maxdd_H / limitups_H / days_used_H /
        success_mg_H（冲一波）/ success_hold_H（可持有）。
    """
    out: dict = {
        "anchor_close": None, "anchor_day_chg": None, "anchor_run2": None,
        "anchor_is_limit_up": None,
    }
    anchor_ts = pd.Timestamp(anchor_date)
    g = df_inst.sort_values("date").reset_index(drop=True)
    # 锚点行：date >= anchor 的第一根；其前缀用于算锚点日自身涨幅
    pos_list = g.index[g["date"] >= anchor_ts]
    if len(pos_list) == 0:
        return out
    apos = int(pos_list[0])
    closes = g["close"].astype(float)
    highs = g["high"].astype(float)
    lows = g["low"].astype(float)
    anchor_close = float(closes.iloc[apos])
    out["anchor_close"] = round(anchor_close, 3)

    thresh = get_limit_thresh(instrument)
    if apos >= 1:
        prev_c = float(closes.iloc[apos - 1])
        if prev_c > 0:
            chg = (anchor_close / prev_c - 1) * 100
            out["anchor_day_chg"] = round(chg, 2)
            out["anchor_is_limit_up"] = bool(chg >= thresh)
    if apos >= 2:
        c2 = float(closes.iloc[apos - 2])
        if c2 > 0:
            out["anchor_run2"] = round((anchor_close / c2 - 1) * 100, 2)

    if anchor_close <= 0:
        return out

    # 各周期：窗口 = 锚点后 1..H 根
    for H in horizons:
        win_close = closes.iloc[apos + 1: apos + 1 + H]
        win_high = highs.iloc[apos + 1: apos + 1 + H]
        win_low = lows.iloc[apos + 1: apos + 1 + H]
        n = len(win_close)
        out[f"days_used_{H}"] = n
        if n == 0:
            out[f"ret_{H}"] = None
            out[f"maxgain_{H}"] = None
            out[f"maxdd_{H}"] = None
            out[f"limitups_{H}"] = 0
            out[f"success_mg_{H}"] = None
            out[f"success_hold_{H}"] = None
            continue
        ret = (float(win_close.iloc[-1]) / anchor_close - 1) * 100
        out[f"ret_{H}"] = round(ret, 2)
        maxgain = (float(win_high.max()) / anchor_close - 1) * 100
        out[f"maxgain_{H}"] = round(maxgain, 2)
        # 最大回撤：窗口内（含锚点收盘为起点的 running-max）最低点相对前高
        run_max = np.maximum.accumulate(
            np.concatenate([[anchor_close], win_close.to_numpy()])
        )[1:]
        dd = (win_low.to_numpy() / run_max - 1) * 100
        maxdd = float(dd.min())
        out[f"maxdd_{H}"] = round(maxdd, 2)
        # 逐日涨停数
        seq = np.concatenate([[anchor_close], win_close.to_numpy()])
        daily = (seq[1:] / seq[:-1] - 1) * 100
        out[f"limitups_{H}"] = int((daily >= thresh).sum())
        # 两种成败口径
        out[f"success_mg_{H}"] = bool(maxgain >= maxgain_thresh)
        out[f"success_hold_{H}"] = bool(ret >= hold_ret_thresh and maxdd >= -hold_maxdd_thresh)
    return out


# ─── 特征提取 ────────────────────────────────────────────────────────────────
SCALAR_FEATURES = [
    "total_score", "surge_score", "pullback_score", "rebound_score", "risk_score",
    "monthly_bonus", "quality_bonus", "final_score", "cum_return_5d",
    "days_since_rebound", "w2_amplitude", "close_vs_3y_peak",
]
BOOL_FEATURES = ["at_new_high_since_rebound", "still_above_rebound_close", "w2_is_real_wash"]
MONTHLY_FEATURES = ["vol_expand_ratio", "six_month_return", "two_months_cum_ret",
                    "monthly_consec_up_count", "eff_break_strength", "eff_break_when"]


def extract_features(rec: dict) -> dict:
    """从单只选股记录提取特征列（标量/布尔/月线/形态/质量标签）。"""
    feat = {"name": rec.get("name"), "pattern": rec.get("pattern")}
    for k in SCALAR_FEATURES:
        feat[k] = rec.get(k)
    for k in BOOL_FEATURES:
        feat[k] = rec.get(k)
    m = rec.get("monthly") or {}
    for k in MONTHLY_FEATURES:
        feat[f"m_{k}"] = m.get(k)
    # W1 振幅
    for w in rec.get("windows_detail", []) or []:
        if w.get("window") == "W1":
            feat["w1_amplitude"] = w.get("amplitude")
            feat["w1_role"] = w.get("role")
            break
    # 质量标签转 tag_* 布尔列
    for tag in rec.get("quality_tags", []) or []:
        feat[f"tag_{tag}"] = True
    return feat


# ─── 主流程 ──────────────────────────────────────────────────────────────────
def build_dataset(snapshots: List[Path], horizons: List[int], maxgain_thresh: float,
                  hold_ret_thresh: float, hold_maxdd_thresh: float) -> pd.DataFrame:
    """遍历快照构建样本表。每行 = 一个 (选股日, 股票) 样本。"""
    rows: List[dict] = []
    for path in snapshots:
        sel_date = DATE_RE.search(path.name).group(1)
        with path.open("r", encoding="utf-8") as f:
            stocks = json.load(f)
        if not isinstance(stocks, list) or not stocks:
            continue
        # 收集锚点并批量取数
        anchors: Dict[str, str] = {}
        for s in stocks:
            a = get_w1_end_date(s)
            if a:
                anchors[s["instrument"]] = a
        min_anchor = min(anchors.values())
        df_all = fetch_daily_window(list(anchors.keys()), min_anchor, max(horizons) + 2)
        by_inst = {i: g for i, g in df_all.groupby("instrument")} if not df_all.empty else {}

        for s in stocks:
            inst = s["instrument"]
            anchor = anchors.get(inst)
            row = {"select_date": sel_date, "instrument": inst, "anchor_date": anchor}
            row.update(extract_features(s))
            if anchor and inst in by_inst:
                row.update(compute_outcomes(by_inst[inst], anchor, inst, horizons,
                                            maxgain_thresh, hold_ret_thresh, hold_maxdd_thresh))
            rows.append(row)
        print(f"[scan] {path.name}: {len(stocks)} 只")
    return pd.DataFrame(rows)


# ─── 初步统计 ────────────────────────────────────────────────────────────────
def _winrate(sub: pd.DataFrame, H: int, label: str) -> str:
    """返回某子集在周期 H 上的 胜率/平均末收益/平均最大浮盈/平均回撤/样本数 文本。

    label='holdable' 用 success_hold_H；'maxgain' 用 success_mg_H。
    """
    col_s = f"success_hold_{H}" if label == "holdable" else f"success_mg_{H}"
    col_r, col_g, col_d, col_used = f"ret_{H}", f"maxgain_{H}", f"maxdd_{H}", f"days_used_{H}"
    valid = sub[(sub[col_used] >= H)] if col_used in sub else sub
    valid = valid.dropna(subset=[col_s])
    n = len(valid)
    if n == 0:
        return f"n=0"
    wr = valid[col_s].mean() * 100
    return (f"n={n:<4} 胜率={wr:5.1f}%  "
            f"均末收益={valid[col_r].mean():6.1f}%  均最大浮盈={valid[col_g].mean():6.1f}%  "
            f"均回撤={valid[col_d].mean():6.1f}%")


def analyze(df: pd.DataFrame, horizons: List[int], label: str) -> None:
    """打印按关键特征分桶的胜率统计（默认主看 H=10）。"""
    H = 10 if 10 in horizons else horizons[0]
    label_desc = "可持有(末收益+回撤达标)" if label == "holdable" else "最大浮盈达标"
    print("\n" + "=" * 88)
    print(f"  初步统计（主周期 H={H} 交易日，成功口径={label_desc}）")
    print("=" * 88)

    print(f"\n  全样本: {_winrate(df, H, label)}")

    def bucket(title: str, mask_pairs):
        print(f"\n  ── {title} ──")
        for blabel, mask in mask_pairs:
            print(f"    {blabel:<22} {_winrate(df[mask], H, label)}")

    # 假设 1：锚点日自身是否涨停（选在加速末端？）
    if "anchor_is_limit_up" in df:
        bucket("锚点日是否涨停", [
            ("锚点涨停(追板)", df["anchor_is_limit_up"] == True),
            ("锚点未涨停", df["anchor_is_limit_up"] == False),
        ])
    # 假设 2：进入锚点的 2 日涨幅过热
    if "anchor_run2" in df:
        bucket("进场前2日涨幅", [
            ("≥18% (过热)", df["anchor_run2"] >= 18),
            ("10~18%", (df["anchor_run2"] >= 10) & (df["anchor_run2"] < 18)),
            ("<10%", df["anchor_run2"] < 10),
        ])
    # 假设 3：rebound_score 高低
    if "rebound_score" in df:
        bucket("rebound_score", [
            ("≥50", df["rebound_score"] >= 50),
            ("35~50", (df["rebound_score"] >= 35) & (df["rebound_score"] < 50)),
            ("<35", df["rebound_score"] < 35),
        ])
    # 假设 4：days_since_rebound（反弹后才几天就入选）
    if "days_since_rebound" in df:
        bucket("days_since_rebound", [
            ("≤2 天(刚反弹)", df["days_since_rebound"] <= 2),
            ("3~5 天", (df["days_since_rebound"] >= 3) & (df["days_since_rebound"] <= 5)),
            (">5 天", df["days_since_rebound"] > 5),
        ])
    # 假设 5：月线量能扩张
    if "m_vol_expand_ratio" in df:
        bucket("月线量能扩张", [
            ("≥1 (放量)", df["m_vol_expand_ratio"] >= 1),
            ("<1 (缩量)", df["m_vol_expand_ratio"] < 1),
        ])
    # 假设 6：形态类型
    if "pattern" in df:
        pats = df["pattern"].dropna().unique()
        bucket("形态类型", [(str(p)[:18], df["pattern"] == p) for p in pats])
    # 假设 7：质量标签
    tag_cols = [c for c in df.columns if c.startswith("tag_")]
    if tag_cols:
        bucket("质量标签(命中=True)", [(c.replace("tag_", ""), df[c] == True) for c in tag_cols])


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--results-dir", default="results", help="选股快照目录")
    parser.add_argument("--prefix", default="top_50_stocks_", help="快照文件名前缀")
    parser.add_argument("--horizons", type=int, nargs="+", default=[5, 10, 20],
                        help="前向周期（交易日），默认 5 10 20")
    parser.add_argument("--label", choices=["holdable", "maxgain"], default="holdable",
                        help="主统计口径：holdable=可持有(默认) / maxgain=能否冲一波")
    parser.add_argument("--maxgain-thresh", type=float, default=10.0,
                        help="maxgain 口径阈值：窗口内最大浮盈 ≥ 该值（%%），默认 10")
    parser.add_argument("--hold-ret-thresh", type=float, default=5.0,
                        help="可持有口径：窗口末累计收益 ≥ 该值（%%），默认 5")
    parser.add_argument("--hold-maxdd-thresh", type=float, default=12.0,
                        help="可持有口径：窗口内最大回撤 ≥ -该值（%%，传正数），默认 12")
    parser.add_argument("--out", default="output/selection_validation.csv", help="输出 CSV 路径")
    args = parser.parse_args()

    snaps = discover_snapshots(Path(args.results_dir), args.prefix)
    if not snaps:
        sys.exit(f"[error] 未在 {args.results_dir} 找到 {args.prefix}*.json 快照")
    print(f"[info] 发现 {len(snaps)} 个快照: {[p.name for p in snaps]}")

    df = build_dataset(snaps, args.horizons, args.maxgain_thresh,
                       args.hold_ret_thresh, args.hold_maxdd_thresh)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n[done] 样本表已写出: {out_path}  （{len(df)} 行 × {len(df.columns)} 列）")

    analyze(df, args.horizons, args.label)


if __name__ == "__main__":
    main()
