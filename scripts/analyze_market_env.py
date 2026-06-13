"""市场环境（大盘择时）与选股胜率关系验证分析。

== 定位 ==
**只读分析，不改 quant_picker。** 用 ``index_bar1d``（上证 000001.SH / 沪深300 000300.SH）
为 ``validate_selections.py`` 产出的每个样本，按其锚点日（W1.end_date）关联大盘环境信号，
再统计不同环境下的“可持有胜率 / 冲一波胜率”，回答：

  1) 选股日**当天**的大盘环境（趋势/动量/距高点）能否预测该批票后续胜率？（可live用）
  2) **持有窗口内**大盘自身涨跌对结果的影响有多大？（beta 视角，事后归因，不可live用）

目的：在“是否给 quant_picker 加择时闸 + 怎么加”之前，先用数据判断哪种环境信号真正有效，
避免上一个“上证站上MA20才出手”的简单闸却误杀/漏判。

== 环境信号（每个指数各算一份，前缀 sse_/hs_）==
  - above_ma20 / above_ma60 : 收盘是否在 20/60 日均线上
  - ma20_slope              : MA20 相对 5 日前的斜率（%）
  - ret20                   : 近 20 日涨跌（%）
  - dd_from_high60          : 距近 60 日最高收盘的回撤（%，≤0）
  - fwd_ret_H               : 锚点后 H 个交易日指数自身涨跌（beta 视角）

== 运行方式 ==
    python scripts/analyze_market_env.py
    python scripts/analyze_market_env.py --csv output/selection_validation_regen.csv --H 10

注意:
    - index_bar1d 若未更新到样本期最新日，超出范围的样本环境信号为 NaN（自动剔除）。
    - 选股日只有少数几个 → 分 cohort 的样本量天然偏小，结论为线索而非定论。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from scripts.db_session import engine

INDEXES = [("000001.SH", "sse", "上证"), ("000300.SH", "hs", "沪深300")]


# ─── 指数环境特征 ────────────────────────────────────────────────────────────
def load_index_features(instrument: str) -> pd.DataFrame:
    """取单指数日线并计算环境特征（升序，date 为索引可比较）。"""
    df = pd.read_sql(
        f"SELECT date, close FROM index_bar1d WHERE instrument='{instrument}' ORDER BY date",
        engine,
    )
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    df["ma20"] = df["close"].rolling(20).mean()
    df["ma60"] = df["close"].rolling(60).mean()
    df["above_ma20"] = df["close"] >= df["ma20"]
    df["above_ma60"] = df["close"] >= df["ma60"]
    df["ma20_slope"] = (df["ma20"] - df["ma20"].shift(5)) / df["ma20"].shift(5) * 100
    df["ret20"] = df["close"].pct_change(20) * 100
    df["high60"] = df["close"].rolling(60).max()
    df["dd_from_high60"] = (df["close"] / df["high60"] - 1) * 100
    return df


def env_at(df_idx: pd.DataFrame, anchor: pd.Timestamp, H: int) -> dict:
    """取锚点日（≤anchor 的最后一根）环境特征 + 锚点后 H 日指数涨跌。"""
    prior = df_idx[df_idx["date"] <= anchor]
    if prior.empty:
        return {}
    pos = prior.index[-1]
    row = df_idx.loc[pos]
    out = {
        "above_ma20": bool(row["above_ma20"]) if pd.notna(row["ma20"]) else None,
        "above_ma60": bool(row["above_ma60"]) if pd.notna(row["ma60"]) else None,
        "ma20_slope": round(float(row["ma20_slope"]), 2) if pd.notna(row["ma20_slope"]) else None,
        "ret20": round(float(row["ret20"]), 2) if pd.notna(row["ret20"]) else None,
        "dd_from_high60": round(float(row["dd_from_high60"]), 2) if pd.notna(row["dd_from_high60"]) else None,
    }
    # 锚点后 H 日指数自身涨跌（beta）
    fpos = pos + H
    if fpos < len(df_idx):
        out["fwd_ret"] = round((float(df_idx.loc[fpos, "close"]) / float(row["close"]) - 1) * 100, 2)
    else:
        out["fwd_ret"] = None
    return out


# ─── 合并环境到样本表 ────────────────────────────────────────────────────────
ENV_KEYS = ["above_ma20", "above_ma60", "ma20_slope", "ret20", "dd_from_high60", "fwd_ret"]


def attach_env(df: pd.DataFrame, H: int) -> pd.DataFrame:
    """为每个样本按 anchor_date 关联上证/沪深300 环境信号。"""
    idx_feats: Dict[str, pd.DataFrame] = {inst: load_index_features(inst) for inst, _, _ in INDEXES}
    df = df.copy()
    df["_anchor"] = pd.to_datetime(df["anchor_date"], errors="coerce")
    records: List[dict] = []
    for _, r in df.iterrows():
        rec: dict = {}
        for inst, pre, _ in INDEXES:
            e = env_at(idx_feats[inst], r["_anchor"], H) if pd.notna(r["_anchor"]) else {}
            for k in ENV_KEYS:
                rec[f"{pre}_{k}"] = e.get(k)
        records.append(rec)
    env_df = pd.DataFrame(records, index=df.index)
    return pd.concat([df, env_df], axis=1)


# ─── 统计 ────────────────────────────────────────────────────────────────────
def _m(sub: pd.DataFrame, H: int) -> str:
    used = sub.get(f"days_used_{H}")
    valid = sub[used >= H].dropna(subset=[f"success_hold_{H}"]) if used is not None else sub
    n = len(valid)
    if n == 0:
        return "n=0"
    return (f"n={n:<4} 可持有={valid[f'success_hold_{H}'].mean()*100:5.1f}%  "
            f"冲一波={valid[f'success_mg_{H}'].mean()*100:5.1f}%  "
            f"均末收益={valid[f'ret_{H}'].mean():+5.1f}%  均回撤={valid[f'maxdd_{H}'].mean():6.1f}%")


def analyze(df: pd.DataFrame, H: int) -> None:
    print("\n" + "=" * 92)
    print(f"  市场环境 vs 选股胜率（H={H}，按锚点日关联上证/沪深300）")
    print("=" * 92)

    # 1) 分 cohort：环境快照 + 胜率
    print("\n【一】分选股日：大盘环境 + 胜率")
    print(f"  {'选股日':<12}{'上证vsMA20':>10}{'上证斜率':>9}{'上证近20':>9}"
          f"{'沪深300近20':>11}{'持有窗内上证%':>13}  胜率")
    for dt, g in df.groupby("select_date"):
        r = g.iloc[0]
        v20 = r.get("sse_above_ma20")
        a20 = "?" if pd.isna(v20) else ("上" if bool(v20) else "下")
        print(f"  {dt:<12}{a20:>10}{_fnum(r.get('sse_ma20_slope')):>9}{_fnum(r.get('sse_ret20')):>9}"
              f"{_fnum(r.get('hs_ret20')):>11}{_fnum(r.get('sse_fwd_ret')):>13}  {_m(g, H)}")

    def bucket(title, pairs):
        print(f"\n  ── {title} ──")
        for lab, mask in pairs:
            print(f"    {lab:<22} {_m(df[mask], H)}")

    # 2) 选股日当天可用的环境信号（live 可用）
    print("\n【二】选股日当天环境信号（可实盘用）")
    if "sse_above_ma20" in df:
        bucket("上证 vs MA20", [("MA20上", df["sse_above_ma20"] == True),
                                ("MA20下", df["sse_above_ma20"] == False)])
    if "sse_above_ma60" in df:
        bucket("上证 vs MA60", [("MA60上", df["sse_above_ma60"] == True),
                                ("MA60下", df["sse_above_ma60"] == False)])
    if "sse_ma20_slope" in df:
        s = df["sse_ma20_slope"]
        bucket("上证 MA20 斜率", [("上行≥0.3%", s >= 0.3), ("走平 -0.3~0.3%", (s > -0.3) & (s < 0.3)),
                                  ("下行≤-0.3%", s <= -0.3)])
    if "hs_ret20" in df:
        s = df["hs_ret20"]
        bucket("沪深300 近20日", [("≥+2%", s >= 2), ("-2~2%", (s > -2) & (s < 2)), ("≤-2%", s <= -2)])
    if "sse_dd_from_high60" in df:
        s = df["sse_dd_from_high60"]
        bucket("上证 距60日高点回撤", [("≥-2%(近高位)", s >= -2), ("-2~-6%", (s < -2) & (s >= -6)),
                                       ("<-6%(深调)", s < -6)])

    # 3) beta 视角：持有窗口内指数涨跌（事后归因，不可 live）
    print("\n【三】持有窗口内大盘涨跌（beta 视角，事后归因）")
    if "sse_fwd_ret" in df:
        s = df["sse_fwd_ret"]
        bucket("窗口内上证涨跌", [("≥+3%", s >= 3), ("0~3%", (s >= 0) & (s < 3)),
                                  ("-3~0%", (s >= -3) & (s < 0)), ("<-3%", s < -3)])


def _fnum(v) -> str:
    return f"{v:.2f}" if isinstance(v, (int, float)) and pd.notna(v) else "?"


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--csv", default="output/selection_validation_regen.csv", help="验证样本 CSV")
    parser.add_argument("--H", type=int, default=10, help="主周期（交易日），默认 10")
    parser.add_argument("--out", default="output/selection_validation_env.csv",
                        help="带环境列的样本表输出路径")
    args = parser.parse_args()

    df = pd.read_csv(args.csv)
    df = attach_env(df, args.H)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False, encoding="utf-8-sig")
    print(f"[done] 带环境列样本表已写出: {args.out}")
    analyze(df, args.H)


if __name__ == "__main__":
    main()
