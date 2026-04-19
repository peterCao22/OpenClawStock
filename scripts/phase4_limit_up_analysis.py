"""
Phase 4 TOP20 涨停特征分析

分析 TOP20 股票在启动前（trigger_week 之前 N 周）是否出现过涨停或大涨日，
以及这些大涨日在周线上是否形成「小高峰」，作为启动信号的先行迹象。

分析维度：
  日线：
    - 启动前各时间窗口（近3/6/12个月）内涨停日（≥9.9%）次数
    - 连板次数（连续2日以上≥9.9%）
    - 大涨日（≥5%、≥7%）频率
    - 大涨日后是否回落（是否是真启动还是脉冲）
  周线：
    - 大涨日所在周的周涨幅（看周 K 上是否形成小高峰）
    - 「小高峰」定义：当周收盘 > 前后各2周的最高收盘（局部高点）
    - 小高峰出现时间 vs 真正启动时间的距离（领先性分析）
  特别关注 300191.SZ（潜能恒信/油气板块）

运行方式：
    python scripts/phase4_limit_up_analysis.py
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import text

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))
from scripts.db_session import engine


# ─── 涨跌幅阈值（按板块区分）──────────────────────────────────────────────────
def get_limit_thresh(instrument: str) -> float:
    """创业板/科创板涨停约20%，主板约10%。分析时用9.9%统一捕捉涨停日。"""
    code = instrument.split(".")[0]
    if code.startswith(("300", "301", "688", "689")):
        return 19.5   # 创业板/科创板
    return 9.5        # 主板（含科创板前几年按10%的）


# ─── 数据加载 ─────────────────────────────────────────────────────────────────
def load_daily(instrument: str, start: str = "2019-01-01", end: str = "2026-03-27") -> pd.DataFrame:
    sql = text(f"""
        SELECT date, open, high, low, close, volume
        FROM kline_qfq
        WHERE instrument = '{instrument}'
          AND date >= '{start}' AND date <= '{end}'
        ORDER BY date ASC
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    df["date"] = pd.to_datetime(df["date"])
    # 直接从收盘价计算涨幅，避免依赖数据库中可能有空值的 change_ratio 字段
    # 注意：前复权数据用相邻收盘价计算涨幅，与复权因子调整一致
    df["change_ratio"] = df["close"].pct_change() * 100
    df["change_ratio"] = df["change_ratio"].fillna(0)
    return df


def load_trading_days(start: str = "2019-01-01", end: str = "2026-03-27") -> pd.Series:
    sql = text(f"""
        SELECT trade_date FROM trading_calendar
        WHERE is_trading_day = true
          AND trade_date >= '{start}' AND trade_date <= '{end}'
        ORDER BY trade_date ASC
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    return pd.to_datetime(df["trade_date"])


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
        volume=("volume", "sum"),
        max_daily_gain=("change_ratio", "max"),   # 该周单日最大涨幅
    )
    agg["week_end_date"] = agg.index.map(last_td)
    agg["week_gain_pct"] = (agg["close"] - agg["open"]) / agg["open"] * 100
    agg = agg.dropna(subset=["week_end_date"]).sort_values("week_end_date").reset_index(drop=True)
    agg.set_index("week_end_date", inplace=True)
    return agg


# ─── 涨停特征提取 ─────────────────────────────────────────────────────────────
def analyze_limit_up(
    instrument: str,
    name: str,
    trigger_week_str: Optional[str],
    ret: float,
    df_daily: pd.DataFrame,
    df_week: pd.DataFrame,
    windows_weeks: list = [12, 26, 52],   # 分析启动前 12/26/52 周
) -> dict:
    result = {
        "instrument": instrument,
        "name": name,
        "trigger_week": trigger_week_str,
        "ret": ret,
    }

    thresh = get_limit_thresh(instrument)

    # ── 确定启动锚点日期 ─────────────────────────────────────────────────────
    if trigger_week_str and trigger_week_str not in ("nan", "None", ""):
        trigger_ts = pd.Timestamp(trigger_week_str)
    else:
        trigger_ts = df_week.index[-1] if not df_week.empty else df_daily["date"].iloc[-1]

    # 找 df_week 中最近的 <= trigger_ts 的索引
    avail_weeks = df_week.index[df_week.index <= trigger_ts]
    if avail_weeks.empty:
        return result
    trigger_ts_adj = avail_weeks[-1]

    # ── 日线：各窗口内涨停统计 ───────────────────────────────────────────────
    for w in windows_weeks:
        # 取 trigger_ts 前 w 周 ≈ w*5 个交易日
        cut = trigger_ts_adj - pd.Timedelta(weeks=w)
        window_daily = df_daily[
            (df_daily["date"] >= cut) & (df_daily["date"] < trigger_ts_adj)
        ].copy()

        if window_daily.empty:
            continue

        lim_days  = (window_daily["change_ratio"] >= thresh).sum()
        big5_days = (window_daily["change_ratio"] >= 5.0).sum()
        big7_days = (window_daily["change_ratio"] >= 7.0).sum()
        total_days = len(window_daily)

        result[f"w{w}_limit_up_days"]     = int(lim_days)
        result[f"w{w}_big5_days"]         = int(big5_days)
        result[f"w{w}_big7_days"]         = int(big7_days)
        result[f"w{w}_limit_up_freq"]     = round(lim_days / total_days, 3) if total_days else None

        # 连板检测（连续2日以上涨停）
        is_limit = (window_daily["change_ratio"] >= thresh).astype(int)
        consec = (is_limit.groupby((is_limit != is_limit.shift()).cumsum()).cumsum())
        max_consec = int(consec.max()) if not consec.empty else 0
        result[f"w{w}_max_consec_limit"]  = max_consec

        # 大涨日后次日是否回落（脉冲 or 持续）
        limit_idx = window_daily[window_daily["change_ratio"] >= thresh].index
        follow_up_count = 0
        for idx in limit_idx:
            pos = window_daily.index.get_loc(idx)
            if pos + 1 < len(window_daily):
                next_day_gain = window_daily["change_ratio"].iloc[pos + 1]
                if next_day_gain > 0:
                    follow_up_count += 1
        result[f"w{w}_limit_followup_ratio"] = (
            round(follow_up_count / lim_days, 2) if lim_days > 0 else None
        )

    # ── 周线：小高峰检测 ─────────────────────────────────────────────────────
    # 小高峰：某周收盘 > 前2周 & 后2周的收盘（局部极大值）
    closes = df_week["close"]
    local_peaks = []
    for i in range(2, len(closes) - 2):
        c = closes.iloc[i]
        if (c > closes.iloc[i-1] and c > closes.iloc[i-2] and
                c > closes.iloc[i+1] and c > closes.iloc[i+2]):
            local_peaks.append(df_week.index[i])

    result["total_local_peaks"] = len(local_peaks)

    # 找「启动前」最近一次小高峰及其与启动的距离
    pre_peaks = [p for p in local_peaks if p < trigger_ts_adj]
    if pre_peaks:
        last_pre_peak = pre_peaks[-1]
        weeks_before_trigger = (trigger_ts_adj - last_pre_peak).days // 7
        peak_close = float(df_week.loc[last_pre_peak, "close"])
        trigger_close = float(df_week.loc[trigger_ts_adj, "close"])
        result["last_pre_peak_date"]           = str(last_pre_peak.date())
        result["last_pre_peak_weeks_before"]   = int(weeks_before_trigger)
        result["last_pre_peak_vs_trigger_pct"] = round(
            (trigger_close - peak_close) / peak_close * 100, 1)
        # 小高峰当周是否含涨停日
        peak_week_daily = df_daily[
            (df_daily["date"] >= last_pre_peak - pd.Timedelta(days=7)) &
            (df_daily["date"] <= last_pre_peak)
        ]
        result["last_pre_peak_has_limit"] = bool(
            (peak_week_daily["change_ratio"] >= thresh).any()
        )

    # 启动前 52 周内含涨停日的周数（看是否有规律性小高峰）
    cut52 = trigger_ts_adj - pd.Timedelta(weeks=52)
    pre52_daily = df_daily[
        (df_daily["date"] >= cut52) & (df_daily["date"] < trigger_ts_adj)
    ]
    pre52_daily = pre52_daily.copy()
    pre52_daily["iso_year"] = pre52_daily["date"].dt.isocalendar().year.astype(int)
    pre52_daily["iso_week"] = pre52_daily["date"].dt.isocalendar().week.astype(int)
    pre52_daily["week_key"] = pre52_daily["iso_year"] * 100 + pre52_daily["iso_week"]
    weeks_with_limit = pre52_daily.groupby("week_key").apply(
        lambda g: (g["change_ratio"] >= thresh).any()
    ).sum()
    total_weeks_52 = pre52_daily["week_key"].nunique()
    result["pre52w_weeks_with_limit"]      = int(weeks_with_limit)
    result["pre52w_total_weeks"]           = int(total_weeks_52)
    result["pre52w_limit_week_ratio"]      = round(
        weeks_with_limit / total_weeks_52, 3) if total_weeks_52 else None

    # ── 启动周本身的特征 ─────────────────────────────────────────────────────
    if trigger_ts_adj in df_week.index:
        launch_row = df_week.loc[trigger_ts_adj]
        result["launch_week_gain_pct"]    = round(float(launch_row["week_gain_pct"]), 1)
        result["launch_max_daily_gain"]   = round(float(launch_row["max_daily_gain"]), 1)
        result["launch_has_limit_up_day"] = bool(launch_row["max_daily_gain"] >= thresh)

    return result


# ─── 主流程 ──────────────────────────────────────────────────────────────────
def load_group_from_csv(group: str, n: int = 20) -> list[tuple]:
    """
    从回测 CSV 中动态读取 top/bottom N 只股票。

    参数：
        group - "top" 或 "bottom"
        n     - 取几只

    返回：
        [(instrument, name, trigger_week, ret), ...]
    """
    csv_path = "output/phase4_full_20251231_v2_backtest.csv"
    df = pd.read_csv(csv_path)
    df["ret"] = pd.to_numeric(df["ret"], errors="coerce")
    df = df.dropna(subset=["ret"]).sort_values("ret", ascending=False).reset_index(drop=True)

    rows = df.head(n) if group == "top" else df.tail(n).iloc[::-1]

    result = []
    for _, r in rows.iterrows():
        trig = str(r.get("trigger_week", "nan"))
        if trig in ("nan", "NaT", "None", ""):
            trig = "nan"
        result.append((r["instrument"], r.get("stock_name", ""), trig, float(r["ret"])))
    return result


def print_group_detail(group_label: str, stock_list: list[tuple],
                       trading_days: pd.Series) -> pd.DataFrame:
    """
    逐只打印大涨日详细清单，并返回汇总 DataFrame。

    参数：
        group_label  - "TOP20" / "BOTTOM20" 等
        stock_list   - [(instrument, name, trigger, ret), ...]
        trading_days - 交易日序列

    返回：
        汇总 DataFrame
    """
    BIG_THRESH = 9.5

    print(f"\n{'='*100}")
    print(f"  {group_label} 全时段大涨日详细清单（≥{BIG_THRESH}%），并标注与启动周的相对位置")
    print(f"{'='*100}")

    summary_rows = []

    for instrument, name, trigger, ret in stock_list:
        thresh = get_limit_thresh(instrument)
        df_daily = load_daily(instrument)
        if df_daily.empty:
            continue
        df_week = aggregate_weekly(df_daily, trading_days)

        trigger_ts = pd.Timestamp(trigger) if trigger not in ("nan", "None", "") else None

        big_days = df_daily[df_daily["change_ratio"] >= BIG_THRESH].copy()

        print(f"\n  ── {instrument} {name}  回测涨幅={ret:.1f}%  启动周={trigger if trigger not in ('nan','') else '无'} ──")
        print(f"     涨停阈值={thresh}%  | 共 {len(big_days)} 个大涨日（≥{BIG_THRESH}%）")

        if big_days.empty:
            print("     （无大涨日）")
            summary_rows.append({"instrument": instrument, "name": name, "ret": ret,
                                  "trigger": trigger, "big_days_total": 0,
                                  "big_days_pre": 0, "big_days_post": 0,
                                  "consec_max": 0, "pre_peak_with_limit": False})
            continue

        # 时间区段标注
        def classify(d):
            if trigger_ts is None:
                return "全期"
            diff = (pd.Timestamp(d) - trigger_ts).days
            if diff < -180:   return f"启动前{abs(diff)//30}月+"
            elif diff < -30:  return f"启动前{abs(diff)//7}周"
            elif diff < 0:    return f"启动前{abs(diff)}天"
            elif diff == 0:   return "启动当天"
            else:             return f"启动后{diff}天"

        # 连板检测
        big_days = big_days.reset_index(drop=True)
        consec_max = cur = 1
        for i in range(1, len(big_days)):
            gap = (big_days.loc[i, "date"] - big_days.loc[i-1, "date"]).days
            cur = cur + 1 if gap <= 3 else 1
            consec_max = max(consec_max, cur)

        # 周线小高峰
        closes = df_week["close"]
        local_peak_dates = set()
        for i in range(2, len(closes) - 2):
            c = closes.iloc[i]
            if (c > closes.iloc[i-1] and c > closes.iloc[i-2] and
                    c > closes.iloc[i+1] and c > closes.iloc[i+2]):
                local_peak_dates.add(df_week.index[i])

        print(f"     {'日期':<13} {'涨幅%':>7} {'收盘':>8} {'时间分类':<18} "
              f"{'所在周涨幅%':>10} {'是否小高峰':>10} {'连板?':>6}")
        print(f"     {'─'*80}")

        pre_count = post_count = 0
        pre_peak_with_limit = False

        for _, row in big_days.iterrows():
            day = row["date"]
            cls = classify(day)
            if trigger_ts:
                if day < trigger_ts:  pre_count += 1
                else:                 post_count += 1

            wk_match = df_week[(df_week.index >= day - pd.Timedelta(days=6)) &
                               (df_week.index <= day + pd.Timedelta(days=6))]
            if not wk_match.empty:
                wk_date = wk_match.index[0]
                wk_gain = df_week.loc[wk_date, "week_gain_pct"]
                is_pk = wk_date in local_peak_dates
                if is_pk and trigger_ts and day < trigger_ts:
                    pre_peak_with_limit = True
                pk_str   = "★小高峰" if is_pk else "-"
                wk_str   = f"{wk_gain:+.1f}%"
            else:
                pk_str = wk_str = "-"

            idx = big_days[big_days["date"] == day].index[0]
            consec_str = ""
            if idx > 0 and (day - big_days.loc[idx-1, "date"]).days <= 3:
                consec_str = "连板↑"

            print(f"     {str(day.date()):<13} {row['change_ratio']:>7.2f}% "
                  f"{row['close']:>8.2f}  {cls:<18} {wk_str:>10} {pk_str:>10} {consec_str:>6}")

        summary_rows.append({
            "instrument": instrument, "name": name, "ret": ret, "trigger": trigger,
            "big_days_total": len(big_days),
            "big_days_pre":  pre_count,
            "big_days_post": post_count,
            "consec_max":    consec_max,
            "pre_peak_with_limit": pre_peak_with_limit,
        })

    # ── 汇总表 ────────────────────────────────────────────────────────────────
    df_sum = pd.DataFrame(summary_rows)
    n = len(df_sum)

    print(f"\n\n{'='*90}")
    print(f"  {group_label} 涨停规律汇总统计")
    print(f"{'='*90}")
    print(f"\n  {'代码':<12} {'名称':<8} {'涨幅':>7} {'大涨日总':>8} {'启动前':>6} "
          f"{'启动后':>6} {'最大连板':>8} {'启动前小高峰含大涨?':>18}")
    print("  " + "─"*78)
    for _, r in df_sum.iterrows():
        pk = "✓" if r.get("pre_peak_with_limit") else "✗"
        print(f"  {r['instrument']:<12} {(r['name'] or '')[:8]:<8} {r['ret']:>7.1f}%"
              f"  {int(r['big_days_total']):>8}"
              f"  {int(r['big_days_pre']):>6}"
              f"  {int(r['big_days_post']):>6}"
              f"  {int(r['consec_max']):>8}"
              f"  {pk:>18}")

    has_pre    = (df_sum["big_days_pre"] > 0).sum()
    has_post   = (df_sum["big_days_post"] > 0).sum()
    has_consec = (df_sum["consec_max"] >= 2).sum()
    has_pk     = df_sum["pre_peak_with_limit"].sum()

    print(f"\n  启动前有大涨日        ：{has_pre}/{n}（{has_pre/n*100:.0f}%）")
    print(f"  启动后有大涨日        ：{has_post}/{n}（{has_post/n*100:.0f}%）")
    print(f"  有2天以上连板         ：{has_consec}/{n}（{has_consec/n*100:.0f}%）")
    print(f"  启动前小高峰含大涨日  ：{has_pk}/{n}（{has_pk/n*100:.0f}%）")
    print(f"\n  大涨日均值（总）  ：{df_sum['big_days_total'].mean():.1f} 次")
    print(f"  大涨日均值（启动前）  ：{df_sum['big_days_pre'].mean():.1f} 次")
    print(f"  大涨日均值（启动后）  ：{df_sum['big_days_post'].mean():.1f} 次")

    return df_sum


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Phase4 大涨日 + 周线小高峰分析")
    parser.add_argument("--group", choices=["top", "bottom", "both"], default="top",
                        help="分析 top / bottom / both（默认 top）")
    parser.add_argument("--n", type=int, default=20, help="取前/后 N 只（默认 20）")
    args = parser.parse_args()

    trading_days = load_trading_days()
    all_dfs = {}

    if args.group in ("top", "both"):
        stock_list = load_group_from_csv("top", args.n)
        df_top = print_group_detail(f"TOP{args.n}", stock_list, trading_days)
        df_top["group"] = f"TOP{args.n}"
        all_dfs["top"] = df_top

    if args.group in ("bottom", "both"):
        stock_list = load_group_from_csv("bottom", args.n)
        df_bot = print_group_detail(f"BOTTOM{args.n}", stock_list, trading_days)
        df_bot["group"] = f"BOTTOM{args.n}"
        all_dfs["bottom"] = df_bot

    # 保存
    combined = pd.concat(list(all_dfs.values()), ignore_index=True)
    out_file = f"output/phase4_limit_up_{args.group}{args.n}.csv"
    combined.to_csv(out_file, index=False, encoding="utf-8-sig")
    print(f"\n详细数据已保存：{out_file}")


if __name__ == "__main__":
    main()
