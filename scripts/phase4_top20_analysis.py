"""
Phase 4 TOP20 大涨股票启动前特征深度分析

对涨幅 TOP20 股票，在「启动前 N 周」的周线 + 日线维度做量化特征提取，
寻找共同规律，为优化筛选条件提供数据依据。

分析维度：
  周线：
    - 启动前 8/12 周的振幅收敛程度
    - 启动前均线粘合度（MA5/10/20/60）
    - 启动前量能萎缩程度（VOL vs VOL_MA20）
    - 启动周 vs 前期均量的放量倍数
    - 启动前周线价格与 MA120/MA250 的关系
    - 价格位于历史最低点以上的距离（底部确认）
  日线：
    - 启动前 20/40 个交易日的日线量价特征
    - 量能底部均值 vs 启动周的放量比
    - 均线多头排列程度（日线 MA5/20/60 顺序）
    - 启动前 K 线实体大小（上影/下影比例）

运行方式：
    python scripts/phase4_top20_analysis.py
    python scripts/phase4_top20_analysis.py --backtest output/phase4_full_20251231_v2_backtest.csv --top 20
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import text

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))
from scripts.db_session import engine

# ─── 周 K 聚合（与 phase4_weekly_screener 保持一致）──────────────────────────

def load_trading_days(start: str, end: str) -> pd.Series:
    sql = text(f"""
        SELECT trade_date FROM trading_calendar
        WHERE is_trading_day = true AND trade_date >= '{start}' AND trade_date <= '{end}'
        ORDER BY trade_date ASC
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    return pd.to_datetime(df["trade_date"])


def load_daily(instrument: str, start: str, end: str) -> pd.DataFrame:
    sql = text(f"""
        SELECT date, open, high, low, close, volume, amount
        FROM kline_qfq
        WHERE instrument = '{instrument}' AND date >= '{start}' AND date <= '{end}'
        ORDER BY date ASC
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    df["date"] = pd.to_datetime(df["date"])
    return df


def aggregate_weekly(df_daily: pd.DataFrame, trading_days: pd.Series) -> pd.DataFrame:
    if df_daily.empty:
        return pd.DataFrame()
    df = df_daily.copy()
    df["iso_year"] = df["date"].dt.isocalendar().year.astype(int)
    df["iso_week"] = df["date"].dt.isocalendar().week.astype(int)
    df["week_key"] = df["iso_year"] * 100 + df["iso_week"]

    td_df = pd.DataFrame({"trade_date": trading_days})
    td_df["iso_year"] = td_df["trade_date"].dt.isocalendar().year.astype(int)
    td_df["iso_week"] = td_df["trade_date"].dt.isocalendar().week.astype(int)
    td_df["week_key"] = td_df["iso_year"] * 100 + td_df["iso_week"]
    last_td = td_df.groupby("week_key")["trade_date"].max()

    agg = df.groupby("week_key").agg(
        open=("open", "first"), high=("high", "max"),
        low=("low", "min"), close=("close", "last"),
        volume=("volume", "sum"), amount=("amount", "sum"),
    )
    agg["week_end_date"] = agg.index.map(last_td)
    agg = agg.dropna(subset=["week_end_date"]).sort_values("week_end_date").reset_index(drop=True)
    agg.set_index("week_end_date", inplace=True)
    return agg


def calc_weekly_ma(df_week: pd.DataFrame) -> pd.DataFrame:
    c, v = df_week["close"], df_week["volume"]
    for n in [5, 10, 20, 60, 120, 250]:
        df_week[f"ma{n}"] = c.rolling(n, min_periods=n).mean()
    for n in [5, 10, 20]:
        df_week[f"vol_ma{n}"] = v.rolling(n, min_periods=n).mean()
    df_week["amplitude_w"] = (df_week["high"] - df_week["low"]) / df_week["close"]
    return df_week


def calc_daily_ma(df_daily: pd.DataFrame) -> pd.DataFrame:
    c, v = df_daily["close"], df_daily["volume"]
    for n in [5, 10, 20, 60, 120]:
        df_daily[f"ma{n}"] = c.rolling(n, min_periods=n).mean()
    df_daily["vol_ma20"] = v.rolling(20, min_periods=20).mean()
    return df_daily


# ─── 单股特征提取 ─────────────────────────────────────────────────────────────

def analyze_single(
    instrument: str,
    name: str,
    trigger_week_str: Optional[str],
    df_week: pd.DataFrame,
    df_daily: pd.DataFrame,
    pre_weeks: int = 12,
    pre_days: int = 40,
) -> dict:
    """
    提取单只股票在启动前的关键特征。

    参数：
        trigger_week_str: 启动周的 week_end_date 字符串（可为 None）
        pre_weeks:        取启动前多少周做特征统计
        pre_days:         取启动前多少个交易日做特征统计
    """
    result = {"instrument": instrument, "name": name, "trigger_week": trigger_week_str}

    # ── 定位启动周 ──────────────────────────────────────────────────────────────
    if trigger_week_str and trigger_week_str != "nan":
        trigger_ts = pd.Timestamp(trigger_week_str)
    else:
        # 无启动信号：用回测期间涨幅最大的那一周（近似）
        trigger_ts = df_week.index[-1]

    if trigger_ts not in df_week.index:
        # 找最近的周
        available = df_week.index[df_week.index <= trigger_ts]
        if available.empty:
            return result
        trigger_ts = available[-1]

    trigger_iloc = df_week.index.get_loc(trigger_ts)

    # ── 周线：取启动前 pre_weeks 根 ─────────────────────────────────────────────
    pre_start_iloc = max(0, trigger_iloc - pre_weeks)
    pre_week_df = df_week.iloc[pre_start_iloc: trigger_iloc]  # 不含启动周
    launch_week  = df_week.iloc[trigger_iloc]                  # 启动周

    if len(pre_week_df) < 4:
        return result

    # 1. 振幅收敛：取前 pre_weeks 周振幅中位数 / 全历史振幅中位数
    hist_ampl   = df_week["amplitude_w"].dropna()
    pre_ampl    = pre_week_df["amplitude_w"].dropna()
    result["pre_amplitude_median"]  = round(float(pre_ampl.median()), 4) if len(pre_ampl) else None
    result["hist_amplitude_median"] = round(float(hist_ampl.median()), 4) if len(hist_ampl) else None
    if result["pre_amplitude_median"] and result["hist_amplitude_median"]:
        result["amplitude_convergence_ratio"] = round(
            result["pre_amplitude_median"] / result["hist_amplitude_median"], 3)

    # 2. 量能萎缩：前 pre_weeks 周均量 / 全历史均量
    pre_vol_mean  = float(pre_week_df["volume"].mean()) if len(pre_week_df) else None
    hist_vol_mean = float(df_week["volume"].mean())
    result["pre_vol_vs_hist"] = round(pre_vol_mean / hist_vol_mean, 3) if pre_vol_mean else None

    # 3. 启动周放量比（launch_vol / 前 pre_weeks 均量）
    launch_vol = float(launch_week["volume"])
    result["launch_vol_vs_pre_mean"] = round(launch_vol / pre_vol_mean, 2) if pre_vol_mean else None

    # 4. 启动周 VR（vs VOL_MA20）
    vol_ma20 = launch_week.get("vol_ma20", np.nan)
    result["launch_vr"] = round(launch_vol / vol_ma20, 2) if (pd.notna(vol_ma20) and vol_ma20 > 0) else None

    # 5. 启动前价格 vs MA120 / MA250
    pre_last = pre_week_df.iloc[-1]
    ma120 = pre_last.get("ma120", np.nan)
    ma250 = pre_last.get("ma250", np.nan)
    result["pre_close_vs_ma120"] = round(float(pre_last["close"]) / ma120, 3) if pd.notna(ma120) else None
    result["pre_close_vs_ma250"] = round(float(pre_last["close"]) / ma250, 3) if pd.notna(ma250) else None

    # 6. 均线粘合度（启动前最后 8 周 MA5/10/20/60 的标准差 / 收盘价）
    cohesion_window = df_week.iloc[max(0, trigger_iloc - 8): trigger_iloc]
    ma_cols = [c for c in ["ma5", "ma10", "ma20", "ma60"] if c in cohesion_window.columns]
    if ma_cols and len(cohesion_window) >= 4:
        ma_vals = cohesion_window[ma_cols].dropna(how="any")
        if len(ma_vals) >= 2:
            coh = (ma_vals.std(axis=1) / cohesion_window["close"].reindex(ma_vals.index)).mean()
            result["ma_cohesion_ratio"] = round(float(coh), 4) if pd.notna(coh) else None

    # 7. 底部确认：最低点 vs 启动前价格（当前价距底部涨幅）
    trough_close = float(df_week["close"].min())
    pre_close    = float(pre_last["close"])
    result["trough_close"] = round(trough_close, 2)
    result["pre_close"]    = round(pre_close, 2)
    result["pre_above_trough_pct"] = round((pre_close - trough_close) / trough_close * 100, 1)

    # 8. 启动周涨幅（周 K 内涨幅）
    launch_open  = float(launch_week["open"])
    launch_close = float(launch_week["close"])
    result["launch_week_gain_pct"] = round((launch_close - launch_open) / launch_open * 100, 1)

    # 9. 启动周收盘 vs MA120 突破情况
    launch_ma120 = launch_week.get("ma120", np.nan)
    result["launch_close_vs_ma120"] = round(launch_close / launch_ma120, 3) if pd.notna(launch_ma120) else None

    # ── 日线：取启动前 pre_days 个交易日 ────────────────────────────────────────
    if not df_daily.empty:
        calc_daily_ma(df_daily)

        # 找启动周开始日期（周 K 的第一个交易日 ≈ trigger_ts 前 7 天）
        trigger_approx = trigger_ts
        daily_before = df_daily[df_daily["date"] < trigger_approx]

        if len(daily_before) >= pre_days:
            pre_daily = daily_before.iloc[-pre_days:]

            # 日线量能萎缩：前 pre_days 均量 vs 全历史均量
            hist_daily_vol = float(df_daily["volume"].mean())
            pre_daily_vol  = float(pre_daily["volume"].mean())
            result["daily_pre_vol_vs_hist"] = round(pre_daily_vol / hist_daily_vol, 3)

            # 日线 MA 多头排列度（最后 5 日 MA5>MA20>MA60 的天数比例）
            last5 = pre_daily.iloc[-5:]
            bull_days = ((last5["ma5"] > last5["ma20"]) & (last5["ma20"] > last5["ma60"])).sum()
            result["daily_bull_ma_ratio"] = round(int(bull_days) / 5, 2)

            # 日线 K 线实体大小（平均实体比例）
            pre_daily["body_ratio"] = abs(pre_daily["close"] - pre_daily["open"]) / (
                pre_daily["high"] - pre_daily["low"] + 1e-6)
            result["daily_avg_body_ratio"] = round(float(pre_daily["body_ratio"].mean()), 3)

            # 日线均线粘合（前 20 日）
            last20_daily = pre_daily.iloc[-20:]
            dm_cols = [c for c in ["ma5", "ma10", "ma20", "ma60"] if c in last20_daily.columns]
            if dm_cols:
                dm_vals = last20_daily[dm_cols].dropna(how="any")
                if len(dm_vals) >= 5:
                    dcoh = (dm_vals.std(axis=1) / last20_daily["close"].reindex(dm_vals.index)).mean()
                    result["daily_ma_cohesion"] = round(float(dcoh), 4) if pd.notna(dcoh) else None

    return result


# ─── 主流程 ──────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backtest", default="output/phase4_full_20251231_v2_backtest.csv")
    parser.add_argument("--top", type=int, default=20, help="分析涨幅 TOP N")
    parser.add_argument("--pre-weeks", type=int, default=12)
    parser.add_argument("--pre-days",  type=int, default=40)
    args = parser.parse_args()

    # ── 读取回测结果，取 TOP N ────────────────────────────────────────────────
    df_bt = pd.read_csv(args.backtest, encoding="utf-8-sig")
    df_bt = df_bt.sort_values("ret", ascending=False).reset_index(drop=True)
    top_df = df_bt.head(args.top)[["instrument", "stock_name", "trigger_week", "ret",
                                    "score", "drawdown_ratio", "bear_weeks"]].copy()
    print(f"\n分析对象：涨幅 TOP {args.top}（{args.backtest}）")
    print(f"涨幅区间：{df_bt.attrs.get('ret_range', '2025-12-31 → 2026-03-27')}")

    # ── 加载全局交易日历 ─────────────────────────────────────────────────────
    print("加载交易日历...")
    trading_days = load_trading_days("2019-01-01", "2026-03-27")

    # ── 逐只分析 ─────────────────────────────────────────────────────────────
    records = []
    for _, row in top_df.iterrows():
        instrument = row["instrument"]
        name       = str(row.get("stock_name") or "")
        trigger    = str(row.get("trigger_week") or "")
        ret        = row["ret"]

        df_daily = load_daily(instrument, "2019-01-01", "2026-03-27")
        if df_daily.empty:
            print(f"  [{instrument}] 无日线数据，跳过")
            continue

        df_week = aggregate_weekly(df_daily, trading_days)
        df_week = calc_weekly_ma(df_week)

        feat = analyze_single(
            instrument, name, trigger,
            df_week, df_daily,
            pre_weeks=args.pre_weeks,
            pre_days=args.pre_days,
        )
        feat["ret"] = ret
        records.append(feat)
        print(f"  [{instrument}] {name[:8]} 涨幅={ret:.1f}%  启动周={trigger}")

    df_feat = pd.DataFrame(records)

    # ── 打印汇总特征表 ────────────────────────────────────────────────────────
    print(f"\n{'='*100}")
    print(f"  TOP {args.top} 启动前特征汇总（启动前 {args.pre_weeks} 周 / {args.pre_days} 日）")
    print(f"{'='*100}")

    display_cols = [
        "instrument", "name", "ret",
        "amplitude_convergence_ratio",  # 振幅收敛比（<1=收敛，越小越好）
        "pre_vol_vs_hist",              # 前期量能萎缩（<1=萎缩）
        "launch_vol_vs_pre_mean",       # 启动周放量倍数（相对前期均量）
        "launch_vr",                    # 启动周 VR（相对 VOL_MA20）
        "ma_cohesion_ratio",            # 周线均线粘合度（越小越粘合）
        "pre_close_vs_ma120",           # 启动前价格 / MA120
        "pre_close_vs_ma250",           # 启动前价格 / MA250
        "pre_above_trough_pct",         # 距底部已涨幅度%
        "launch_week_gain_pct",         # 启动周自身涨幅%
        "daily_pre_vol_vs_hist",        # 日线：前期量萎缩
        "daily_bull_ma_ratio",          # 日线：近5日均线多头排列比例
        "daily_ma_cohesion",            # 日线：均线粘合度
    ]
    display_cols = [c for c in display_cols if c in df_feat.columns]

    # 格式化输出
    pd.set_option("display.max_columns", 20)
    pd.set_option("display.width", 180)
    pd.set_option("display.float_format", lambda x: f"{x:.3f}" if pd.notna(x) else "N/A")
    print(df_feat[display_cols].to_string(index=False))

    # ── 统计均值与中位数 ─────────────────────────────────────────────────────
    numeric_cols = [c for c in display_cols if c not in ("instrument", "name")]
    stats = df_feat[numeric_cols].agg(["mean", "median", "min", "max"]).T
    stats.columns = ["均值", "中位数", "最小", "最大"]

    print(f"\n{'='*80}")
    print(f"  TOP {args.top} 特征统计（均值/中位数/区间）")
    print(f"{'='*80}")
    for col, row2 in stats.iterrows():
        mean_v   = f"{row2['均值']:.3f}"  if pd.notna(row2['均值'])   else "N/A"
        median_v = f"{row2['中位数']:.3f}" if pd.notna(row2['中位数']) else "N/A"
        min_v    = f"{row2['最小']:.3f}"  if pd.notna(row2['最小'])   else "N/A"
        max_v    = f"{row2['最大']:.3f}"  if pd.notna(row2['最大'])   else "N/A"
        print(f"  {col:<35} 均={mean_v:>8}  中={median_v:>8}  [{min_v}, {max_v}]")

    # ── 关键阈值建议 ─────────────────────────────────────────────────────────
    print(f"\n{'='*80}")
    print("  基于数据的关键阈值建议（参考中位数 ± 1 档）")
    print(f"{'='*80}")
    thresholds = {}

    def suggest(col, direction="lower_better"):
        if col not in df_feat.columns:
            return
        vals = df_feat[col].dropna()
        if vals.empty:
            return
        med = vals.median()
        p25 = vals.quantile(0.25)
        p75 = vals.quantile(0.75)
        thresholds[col] = {"median": med, "p25": p25, "p75": p75}
        if direction == "lower_better":
            print(f"  {col:<40} 中位数={med:.3f}  P25={p25:.3f}  P75={p75:.3f}  → 建议阈值 ≤ {p75:.3f}")
        else:
            print(f"  {col:<40} 中位数={med:.3f}  P25={p25:.3f}  P75={p75:.3f}  → 建议阈值 ≥ {p25:.3f}")

    suggest("amplitude_convergence_ratio",  "lower_better")   # 振幅要收敛
    suggest("pre_vol_vs_hist",              "lower_better")   # 量要萎缩
    suggest("launch_vol_vs_pre_mean",       "higher_better")  # 启动要放量
    suggest("launch_vr",                    "higher_better")  # 启动 VR 要大
    suggest("ma_cohesion_ratio",            "lower_better")   # 均线要粘合
    suggest("pre_above_trough_pct",         "lower_better")   # 距底不能太远
    suggest("daily_bull_ma_ratio",          "higher_better")  # 日线均线开始多头排列
    suggest("daily_ma_cohesion",            "lower_better")   # 日线均线粘合

    # ── 保存特征结果 ─────────────────────────────────────────────────────────
    out_path = "output/phase4_top20_features.csv"
    df_feat.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n特征数据已保存：{out_path}")


if __name__ == "__main__":
    main()
