"""
Phase 4 TOP20 vs BOTTOM20 特征对比分析

从回测结果中取涨幅最高的20只（TOP20）和最低的20只（BOTTOM20），
对两组股票进行全时段特征对比，找出真正能区分优劣候选的判别维度。

分析维度：
  基础形态维度（来自 screener 输出）：
    - score、drawdown_ratio、bear_weeks、base_weeks、trigger_week 距截止日天数
  涨停/脉冲维度（全时段日K）：
    - 大涨日总次数（≥9.5%）
    - 启动前 / 后分布
    - 最大连板天数
    - 启动前含大涨日的周线小高峰数量
  价格位置维度：
    - 启动前收盘 vs MA120（close_vs_ma120）
    - 回撤幅度（drawdown_ratio）

运行方式：
    python scripts/phase4_top_vs_bottom.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import text

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))
from scripts.db_session import engine

# ─── 配置 ─────────────────────────────────────────────────────────────────────
BACKTEST_CSV  = "output/phase4_full_20251231_v2_backtest.csv"
TOP_N         = 20
BOTTOM_N      = 20
BIG_GAIN_THRESH = 9.5   # 大涨日阈值（%），统一捕捉涨停/大涨，不区分板块
AS_OF_DATE    = "2025-12-31"   # screener 截止日（用于计算 trigger_recency）


# ─── 数据加载 ─────────────────────────────────────────────────────────────────
def load_backtest() -> pd.DataFrame:
    """加载回测结果，返回排序后的 DataFrame。"""
    df = pd.read_csv(BACKTEST_CSV)
    df["ret"] = pd.to_numeric(df["ret"], errors="coerce")
    df = df.dropna(subset=["ret"]).sort_values("ret", ascending=False).reset_index(drop=True)
    return df


def load_daily(instrument: str, start: str = "2019-01-01", end: str = "2026-03-27") -> pd.DataFrame:
    """加载日K，涨幅从收盘价序列直接计算（避免依赖库中可能有空值的字段）。"""
    sql = text(f"""
        SELECT date, open, high, low, close, volume
        FROM kline_qfq
        WHERE instrument = '{instrument}'
          AND date >= '{start}' AND date <= '{end}'
        ORDER BY date ASC
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    df["change_ratio"] = df["close"].pct_change() * 100
    df["change_ratio"] = df["change_ratio"].fillna(0)
    return df


def load_trading_days() -> pd.Series:
    sql = text("""
        SELECT trade_date FROM trading_calendar
        WHERE is_trading_day = true
          AND trade_date >= '2019-01-01'
          AND trade_date <= '2026-03-27'
        ORDER BY trade_date ASC
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    return pd.to_datetime(df["trade_date"])


def aggregate_weekly(df_daily: pd.DataFrame, trading_days: pd.Series) -> pd.DataFrame:
    """日K → 周K（ISO 自然周）。"""
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
        low=("low", "min"),   close=("close", "last"),
        volume=("volume", "sum"),
        max_daily_gain=("change_ratio", "max"),
    )
    agg["week_end_date"] = agg.index.map(last_td)
    agg["week_gain_pct"] = (agg["close"] - agg["open"]) / agg["open"] * 100
    agg = agg.dropna(subset=["week_end_date"]).sort_values("week_end_date").reset_index(drop=True)
    agg.set_index("week_end_date", inplace=True)
    return agg


# ─── 单股特征提取 ─────────────────────────────────────────────────────────────
def extract_features(row: pd.Series, df_daily: pd.DataFrame, df_week: pd.DataFrame) -> dict:
    """
    提取单只股票的全时段特征，用于 TOP/BOTTOM 对比。

    参数：
        row       - 回测 CSV 中的单行（含 screener 输出字段）
        df_daily  - 日K 数据
        df_week   - 周K 数据

    返回：
        dict，包含所有分析维度
    """
    instrument  = row["instrument"]
    trigger_str = str(row.get("trigger_week", ""))
    as_of       = pd.Timestamp(AS_OF_DATE)

    feat: dict = {
        "instrument": instrument,
        "name":        row.get("stock_name", ""),
        "ret":         row["ret"],
        "score":       row.get("score", np.nan),
        "drawdown_ratio": row.get("drawdown_ratio", np.nan),
        "bear_weeks":  row.get("bear_weeks", np.nan),
        "base_weeks":  row.get("base_weeks", np.nan),
        "close_vs_ma120": row.get("close_vs_ma120", np.nan),
    }

    # trigger_recency：trigger_week 距截止日的天数（越小越新，负数表示在截止日前）
    if trigger_str not in ("nan", "None", "", "NaT"):
        try:
            trigger_ts = pd.Timestamp(trigger_str)
            feat["trigger_recency_days"] = (as_of - trigger_ts).days
        except Exception:
            trigger_ts = None
            feat["trigger_recency_days"] = np.nan
    else:
        trigger_ts = None
        feat["trigger_recency_days"] = np.nan

    if df_daily.empty or df_week.empty:
        return feat

    # ── 全时段大涨日统计 ──────────────────────────────────────────────────────
    big_days = df_daily[df_daily["change_ratio"] >= BIG_GAIN_THRESH].copy()
    feat["big_days_total"] = len(big_days)

    # 启动前 / 后拆分（只有有 trigger 的才能拆分）
    if trigger_ts is not None:
        pre  = big_days[big_days["date"] < trigger_ts]
        post = big_days[big_days["date"] >= trigger_ts]
        feat["big_days_pre"]  = len(pre)
        feat["big_days_post"] = len(post)
    else:
        feat["big_days_pre"]  = len(big_days)   # 无trigger，全算"前"
        feat["big_days_post"] = 0

    # 最大连板天数（相邻大涨日间隔 ≤3 交易日）
    max_consec = 0
    cur_consec = 1
    sorted_days = big_days.sort_values("date").reset_index(drop=True)
    for i in range(1, len(sorted_days)):
        gap = (sorted_days.loc[i, "date"] - sorted_days.loc[i-1, "date"]).days
        if gap <= 3:
            cur_consec += 1
            max_consec = max(max_consec, cur_consec)
        else:
            cur_consec = 1
    feat["max_consec_days"] = max(max_consec, cur_consec if len(sorted_days) > 0 else 0)

    # ── 周线小高峰（启动前）统计 ──────────────────────────────────────────────
    closes = df_week["close"]
    local_peaks = []
    for i in range(2, len(closes) - 2):
        c = closes.iloc[i]
        if (c > closes.iloc[i-1] and c > closes.iloc[i-2] and
                c > closes.iloc[i+1] and c > closes.iloc[i+2]):
            local_peaks.append(df_week.index[i])

    ref_ts = trigger_ts if trigger_ts is not None else df_week.index[-1]
    pre_peaks = [p for p in local_peaks if p < ref_ts]
    post_peaks = [p for p in local_peaks if p >= ref_ts]
    feat["pre_peak_count"]  = len(pre_peaks)
    feat["post_peak_count"] = len(post_peaks)

    # 启动前小高峰中，含大涨日的数量（大涨日在小高峰前后5天内）
    pre_peak_with_big = 0
    for pk in pre_peaks:
        window = df_daily[
            (df_daily["date"] >= pk - pd.Timedelta(days=5)) &
            (df_daily["date"] <= pk + pd.Timedelta(days=2))
        ]
        if (window["change_ratio"] >= BIG_GAIN_THRESH).any():
            pre_peak_with_big += 1
    feat["pre_peak_with_big_day"] = pre_peak_with_big

    # 启动前 26 周的大涨日集中度（启动前半年内的大涨日 / 总大涨日）
    if trigger_ts is not None:
        cut26 = trigger_ts - pd.Timedelta(weeks=26)
        near_pre = big_days[
            (big_days["date"] >= cut26) & (big_days["date"] < trigger_ts)
        ]
        feat["big_days_last26w_pre"] = len(near_pre)
    else:
        feat["big_days_last26w_pre"] = 0

    # ── 启动前最大振幅收敛比（近12周 vs 全历史振幅中位数）─────────────────────
    try:
        df_week_arr = df_week.copy()
        df_week_arr["amplitude"] = (df_week_arr["high"] - df_week_arr["low"]) / df_week_arr["close"]
        hist_amp_med = df_week_arr["amplitude"].median()
        avail_before = df_week_arr[df_week_arr.index < ref_ts]
        if len(avail_before) >= 12:
            recent12_amp = avail_before.iloc[-12:]["amplitude"].mean()
            feat["amplitude_conv_ratio"] = round(recent12_amp / hist_amp_med, 3) if hist_amp_med > 0 else np.nan
        else:
            feat["amplitude_conv_ratio"] = np.nan
    except Exception:
        feat["amplitude_conv_ratio"] = np.nan

    return feat


# ─── 主流程 ──────────────────────────────────────────────────────────────────
def main():
    print("加载回测数据……")
    df_bt = load_backtest()
    top20_rows    = df_bt.head(TOP_N)
    bottom20_rows = df_bt.tail(BOTTOM_N)

    print("加载交易日历……")
    trading_days = load_trading_days()

    def process_group(rows: pd.DataFrame, label: str) -> list[dict]:
        records = []
        print(f"\n{'='*80}")
        print(f"  处理 {label}（共 {len(rows)} 只）")
        print(f"{'='*80}")
        for _, row in rows.iterrows():
            inst = row["instrument"]
            name = row.get("stock_name", "")
            ret  = row["ret"]
            print(f"  → {inst} {name}  ret={ret:.1f}%")
            df_daily = load_daily(inst)
            if df_daily.empty:
                print(f"    [跳过：无日K数据]")
                continue
            df_week = aggregate_weekly(df_daily, trading_days)
            feat = extract_features(row, df_daily, df_week)
            records.append(feat)
        return records

    top_records    = process_group(top20_rows,    "TOP20（涨幅最高）")
    bottom_records = process_group(bottom20_rows, "BOTTOM20（涨幅最低）")

    df_top    = pd.DataFrame(top_records)
    df_bottom = pd.DataFrame(bottom_records)

    # ── 逐只打印明细 ─────────────────────────────────────────────────────────
    def print_detail(df: pd.DataFrame, label: str):
        print(f"\n\n{'='*110}")
        print(f"  {label} 明细")
        print(f"{'='*110}")
        print(f"  {'代码':<12} {'名称':<8} {'涨幅':>7} {'Score':>6} "
              f"{'Trigger距今(天)':>14} {'大涨日总':>8} {'启动前':>6} {'启动后':>6} "
              f"{'最大连板':>8} {'启动前小高峰':>10} {'含大涨峰':>8} {'振幅收敛':>8}")
        print("  " + "─"*108)
        for _, r in df.iterrows():
            tr = f"{int(r['trigger_recency_days'])}" if pd.notna(r.get("trigger_recency_days")) else "-"
            ac = f"{r['amplitude_conv_ratio']:.3f}" if pd.notna(r.get("amplitude_conv_ratio")) else "-"
            print(
                f"  {r['instrument']:<12} {(r['name'] or '')[:8]:<8} {r['ret']:>7.1f}% "
                f"{int(r['score']) if pd.notna(r['score']) else '-':>6} "
                f"{tr:>14} "
                f"{int(r['big_days_total']):>8} "
                f"{int(r['big_days_pre']):>6} "
                f"{int(r['big_days_post']):>6} "
                f"{int(r['max_consec_days']):>8} "
                f"{int(r['pre_peak_count']):>10} "
                f"{int(r['pre_peak_with_big_day']):>8} "
                f"{ac:>8}"
            )

    print_detail(df_top,    "TOP20（涨幅最高）")
    print_detail(df_bottom, "BOTTOM20（涨幅最低）")

    # ── 关键维度对比汇总表 ────────────────────────────────────────────────────
    metrics = [
        ("ret",                    "涨幅（%）",                  "{:.1f}"),
        ("score",                  "Score（满100）",              "{:.0f}"),
        ("drawdown_ratio",         "回撤幅度",                    "{:.2f}"),
        ("bear_weeks",             "熊市周数",                    "{:.0f}"),
        ("base_weeks",             "筑底周数",                    "{:.0f}"),
        ("close_vs_ma120",         "启动前收盘/MA120",            "{:.3f}"),
        ("trigger_recency_days",   "Trigger距截止日（天）",       "{:.0f}"),
        ("big_days_total",         "大涨日总次数",                "{:.1f}"),
        ("big_days_pre",           "大涨日（启动前）",            "{:.1f}"),
        ("big_days_post",          "大涨日（启动后）",            "{:.1f}"),
        ("big_days_last26w_pre",   "启动前26周内大涨日",          "{:.1f}"),
        ("max_consec_days",        "最大连板天数",                "{:.1f}"),
        ("pre_peak_count",         "启动前周线小高峰数",          "{:.1f}"),
        ("pre_peak_with_big_day",  "含大涨日的小高峰数",          "{:.1f}"),
        ("amplitude_conv_ratio",   "振幅收敛比（近12周/历史）",   "{:.3f}"),
    ]

    print(f"\n\n{'='*80}")
    print("  TOP20 vs BOTTOM20 关键维度统计对比（均值 ± 中位数）")
    print(f"{'='*80}")
    print(f"\n  {'维度':<28} {'TOP20均值':>10} {'TOP20中位':>10} {'BOT20均值':>10} {'BOT20中位':>10}  {'差异方向':>10}")
    print("  " + "─"*78)

    comparison_rows = []
    for col, label, fmt in metrics:
        t_mean = df_top[col].mean()   if col in df_top.columns   else np.nan
        t_med  = df_top[col].median() if col in df_top.columns   else np.nan
        b_mean = df_bottom[col].mean()   if col in df_bottom.columns else np.nan
        b_med  = df_bottom[col].median() if col in df_bottom.columns else np.nan

        def fmt_val(v):
            if pd.isna(v):
                return "-"
            try:
                return fmt.format(v)
            except Exception:
                return str(round(v, 3))

        # 判断差异方向（TOP均值 > BOT均值 → TOP占优）
        if pd.notna(t_mean) and pd.notna(b_mean):
            diff = t_mean - b_mean
            if abs(diff) < 0.01 * max(abs(t_mean), abs(b_mean), 1):
                arrow = "≈ 相近"
            elif diff > 0:
                arrow = "TOP ↑ 高"
            else:
                arrow = "BOT ↑ 高"
        else:
            arrow = "-"

        print(f"  {label:<28} {fmt_val(t_mean):>10} {fmt_val(t_med):>10} "
              f"{fmt_val(b_mean):>10} {fmt_val(b_med):>10}  {arrow:>10}")

        comparison_rows.append({
            "metric": col, "label": label,
            "top_mean": t_mean, "top_med": t_med,
            "bot_mean": b_mean, "bot_med": b_med,
        })

    # ── 分布对比：各维度 TOP20 vs BOTTOM20 命中率 ─────────────────────────────
    print(f"\n\n{'='*80}")
    print("  分布特征对比（各条件命中率 TOP20 vs BOTTOM20）")
    print(f"{'='*80}")

    conditions = [
        ("有连板≥2天",           lambda df: (df["max_consec_days"] >= 2).mean()),
        ("启动前有大涨日",        lambda df: (df["big_days_pre"] > 0).mean()),
        ("启动前有小高峰",        lambda df: (df["pre_peak_count"] > 0).mean()),
        ("振幅收敛比≤0.95",      lambda df: (df["amplitude_conv_ratio"].dropna() <= 0.95).sum() / len(df)),
        ("Score≥70",             lambda df: (df["score"] >= 70).mean()),
        ("Trigger≤180天",        lambda df: (df["trigger_recency_days"].dropna() <= 180).sum() / len(df)),
        ("Trigger≤365天",        lambda df: (df["trigger_recency_days"].dropna() <= 365).sum() / len(df)),
        ("回撤30%~70%",          lambda df: df["drawdown_ratio"].apply(lambda x: 0.30 <= abs(x) <= 0.70 if pd.notna(x) else False).mean()),
        ("回撤>70%",             lambda df: df["drawdown_ratio"].apply(lambda x: abs(x) > 0.70 if pd.notna(x) else False).mean()),
        ("熊市≥104周（2年）",    lambda df: (df["bear_weeks"].dropna() >= 104).sum() / len(df)),
        ("筑底≥26周",            lambda df: (df["base_weeks"].dropna() >= 26).sum() / len(df)),
    ]

    print(f"\n  {'条件':<24} {'TOP20命中率':>11} {'BOT20命中率':>11}  {'区分度':>8}")
    print("  " + "─"*60)
    for cname, func in conditions:
        try:
            t_rate = func(df_top)
            b_rate = func(df_bottom)
            diff   = t_rate - b_rate
            arrow  = f"+{diff:+.0%}" if abs(diff) >= 0.05 else "≈相近"
            print(f"  {cname:<24} {t_rate:>11.0%} {b_rate:>11.0%}  {arrow:>8}")
        except Exception as e:
            print(f"  {cname:<24} [计算出错: {e}]")

    # ── 保存 ──────────────────────────────────────────────────────────────────
    df_top["group"]    = "TOP20"
    df_bottom["group"] = "BOTTOM20"
    combined = pd.concat([df_top, df_bottom], ignore_index=True)
    combined.to_csv("output/phase4_top_vs_bottom.csv", index=False, encoding="utf-8-sig")

    df_comp = pd.DataFrame(comparison_rows)
    df_comp.to_csv("output/phase4_comparison_stats.csv", index=False, encoding="utf-8-sig")

    print(f"\n\n结果已保存：")
    print(f"  output/phase4_top_vs_bottom.csv      （明细）")
    print(f"  output/phase4_comparison_stats.csv   （对比统计）")


if __name__ == "__main__":
    main()
