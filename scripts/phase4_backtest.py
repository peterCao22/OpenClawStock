"""
Phase 4 回测校验脚本

逻辑：
  1. 读取 phase4 筛选结果（as-of 2025-12-31 的候选池）
  2. 从 kline_qfq 查每只股票在 2025-12-31（或其后最近交易日）的收盘价
     以及 2026-03-27（或其前最近交易日）的收盘价
  3. 计算区间涨幅，按涨幅倒序排名
  4. 对涨幅 TOP N 与 Bottom N 分组，对比各维度特征均值，
     总结「哪些特征的股票涨得更好」

运行方式：
    python scripts/phase4_backtest.py
    python scripts/phase4_backtest.py --candidates output/phase4_full_20251231_v2.json
    python scripts/phase4_backtest.py --top 50 --bottom 50
    python scripts/phase4_backtest.py --candidates output/phase4_merged_20240524.json \\
        --start-date 2024-05-24 --end-date 2024-08-26
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import text

import sys, os
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.db_session import engine

# ── 配置 ─────────────────────────────────────────────────────────────────────
DEFAULT_CANDIDATES  = "output/phase4_full_20251231_v2.json"
PRICE_START_DATE    = "2025-12-31"   # 买入基准日（截至日）
PRICE_END_DATE      = "2026-03-27"   # 卖出基准日（计算涨幅截止）
LOOKBACK_DAYS       = 5              # 若基准日无数据，往前/后各找 N 个交易日


def load_candidates(path: str) -> pd.DataFrame:
    """加载 phase4 筛选结果 JSON，返回 DataFrame。"""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    df = pd.DataFrame(data["candidates"])
    print(f"候选池加载完成：{len(df)} 只股票（as-of {data['as_of']}）")
    return df


def fetch_price_near(instruments: list, target_date: str, direction: str = "backward") -> pd.DataFrame:
    """
    批量获取每只股票最近可用交易日的收盘价。

    参数：
        instruments: 股票代码列表
        target_date: 目标日期 'YYYY-MM-DD'
        direction:   'backward'=往前找最近交易日（用于结束日），
                     'forward'=往后找最近交易日（用于开始日）
    返回：
        DataFrame，含 instrument、close、actual_date 列
    """
    codes_str = ",".join(f"'{c}'" for c in instruments)

    if direction == "backward":
        # 取 <= target_date 的最新一条
        sql = text(f"""
            SELECT DISTINCT ON (instrument)
                instrument, date AS actual_date, close
            FROM kline_qfq
            WHERE instrument IN ({codes_str})
              AND date <= '{target_date}'
            ORDER BY instrument, date DESC
        """)
    else:
        # 取 >= target_date 的最早一条
        sql = text(f"""
            SELECT DISTINCT ON (instrument)
                instrument, date AS actual_date, close
            FROM kline_qfq
            WHERE instrument IN ({codes_str})
              AND date >= '{target_date}'
            ORDER BY instrument, date ASC
        """)

    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    return df


def calc_returns(
    df_candidates: pd.DataFrame,
    price_start: str,
    price_end: str,
) -> pd.DataFrame:
    """
    计算每只候选股在 [price_start, price_end] 区间的涨幅。

    参数：
        df_candidates: 候选 DataFrame（含 instrument）
        price_start: 买入基准日（取该日及之后最近交易日收盘）
        price_end: 卖出基准日（取该日及之前最近交易日收盘）
    返回：
        含 ret（涨幅 %）与 close_start、close_end 的 DataFrame
    """
    instruments = df_candidates["instrument"].tolist()
    batch = 500   # 分批查询避免 SQL 过长

    start_prices = []
    end_prices   = []

    for i in range(0, len(instruments), batch):
        batch_instr = instruments[i: i + batch]
        start_prices.append(fetch_price_near(batch_instr, price_start, "forward"))
        end_prices.append(fetch_price_near(batch_instr, price_end, "backward"))

    df_start = pd.concat(start_prices, ignore_index=True).rename(
        columns={"close": "close_start", "actual_date": "date_start"})
    df_end   = pd.concat(end_prices, ignore_index=True).rename(
        columns={"close": "close_end", "actual_date": "date_end"})

    df = df_candidates.merge(df_start, on="instrument", how="left")
    df = df.merge(df_end,   on="instrument", how="left")

    # 计算区间涨幅（%）
    df["ret"] = (df["close_end"] - df["close_start"]) / df["close_start"] * 100
    df["ret"] = df["ret"].round(2)

    # 过滤掉无法获取价格的
    before = len(df)
    df = df.dropna(subset=["ret"])
    after  = len(df)
    if before - after > 0:
        print(f"  ⚠ {before - after} 只因无价格数据被剔除")

    return df


def analyze_groups(df: pd.DataFrame, top_n: int, bottom_n: int) -> None:
    """
    对涨幅 TOP N 与 Bottom N 的特征均值进行对比分析。

    涵盖维度：score、drawdown_ratio、bear_weeks、base_weeks、vr、
              close_vs_ma120、close_vs_ma250、trigger_week 分布等。
    """
    df_sorted = df.sort_values("ret", ascending=False).reset_index(drop=True)
    df_sorted["rank"] = df_sorted.index + 1

    top    = df_sorted.head(top_n)
    bottom = df_sorted.tail(bottom_n)

    num_cols = ["score", "drawdown_ratio", "bear_weeks",
                "base_weeks", "vr", "close_vs_ma120", "close_vs_ma250", "ret"]

    def fmt_mean(series):
        v = series.dropna().mean()
        return f"{v:.2f}" if pd.notna(v) else "N/A"

    print(f"\n{'='*70}")
    print(f"  特征对比：TOP {top_n} vs BOTTOM {bottom_n}（共 {len(df_sorted)} 只）")
    print(f"{'='*70}")
    header = f"{'维度':<22} {'TOP均值':>10} {'BTM均值':>10} {'全体均值':>10}"
    print(header)
    print("-" * 55)
    for col in num_cols:
        if col in df_sorted.columns:
            label = col
            all_mean = fmt_mean(df_sorted[col])
            top_mean = fmt_mean(top[col])
            btm_mean = fmt_mean(bottom[col])
            print(f"  {label:<20} {top_mean:>10} {btm_mean:>10} {all_mean:>10}")

    # bool flag 命中率对比
    bool_cols = ["drawdown_in_range", "bear_duration_ok", "base_formed",
                 "launch_ma", "launch_volume_vr", "launch_volume_base"]
    print()
    print(f"  {'布尔条件命中率':<20} {'TOP':>10} {'BTM':>10} {'全体':>10}")
    print("-" * 55)
    for col in bool_cols:
        if col in df_sorted.columns:
            all_r = df_sorted[col].mean() * 100
            top_r = top[col].mean() * 100
            btm_r = bottom[col].mean() * 100
            print(f"  {col:<20} {top_r:>9.1f}% {btm_r:>9.1f}% {all_r:>9.1f}%")

    # trigger_week 分布（TOP vs Bottom 各有几只有/没有启动信号）
    print()
    print("  trigger_week 有/无：")
    for grp, name in [(top, "TOP"), (bottom, "BTM")]:
        has_trigger = grp["trigger_week"].notna().sum()
        print(f"    {name}: 有启动信号 {has_trigger}/{len(grp)} 只")

    # score 分布
    print()
    print(f"  Score 分布（TOP {top_n}）：")
    print("    " + str(top["score"].value_counts().sort_index(ascending=False).to_dict()))
    print(f"  Score 分布（BTM {bottom_n}）：")
    print("    " + str(bottom["score"].value_counts().sort_index(ascending=False).to_dict()))


def print_top_list(
    df: pd.DataFrame,
    n: int,
    start_date: str,
    end_date: str,
) -> None:
    """打印涨幅排行榜。"""
    df_sorted = df.sort_values("ret", ascending=False).reset_index(drop=True)
    print(f"\n{'='*90}")
    print(f"  涨幅排名 TOP {n}（{start_date} → {end_date}）")
    print(f"{'='*90}")
    print(f"  {'排名':>4} {'代码':<12} {'名称':<10} {'涨幅%':>7} "
          f"{'score':>6} {'跌幅':>7} {'熊市周':>6} {'筑底周':>7} "
          f"{'VR':>5} {'启动周':>12}")
    print("  " + "-" * 86)
    for i, row in df_sorted.head(n).iterrows():
        tw   = str(row.get("trigger_week") or "-")[:10]
        vr   = f"{row['vr']:.2f}"    if pd.notna(row.get("vr"))   else "-"
        dr   = f"{row['drawdown_ratio']:.1%}" if pd.notna(row.get("drawdown_ratio")) else "-"
        bw   = str(int(row["bear_weeks"])) if pd.notna(row.get("bear_weeks")) else "-"
        basew = str(int(row["base_weeks"])) if pd.notna(row.get("base_weeks")) else "-"
        name = (row.get("stock_name") or "")[:8]
        print(f"  {i+1:>4} {row['instrument']:<12} {name:<10} "
              f"{row['ret']:>7.2f}% {int(row['score']):>6} "
              f"{dr:>7} {bw:>6} {basew:>7} "
              f"{vr:>5} {tw:>12}")

    print(f"\n  涨幅排名 BOTTOM {n}")
    print("  " + "-" * 86)
    for i, row in df_sorted.tail(n).iterrows():
        tw    = str(row.get("trigger_week") or "-")[:10]
        vr    = f"{row['vr']:.2f}" if pd.notna(row.get("vr")) else "-"
        dr    = f"{row['drawdown_ratio']:.1%}" if pd.notna(row.get("drawdown_ratio")) else "-"
        bw    = str(int(row["bear_weeks"])) if pd.notna(row.get("bear_weeks")) else "-"
        basew = str(int(row["base_weeks"])) if pd.notna(row.get("base_weeks")) else "-"
        name  = (row.get("stock_name") or "")[:8]
        print(f"  {i+1:>4} {row['instrument']:<12} {name:<10} "
              f"{row['ret']:>7.2f}% {int(row['score']):>6} "
              f"{dr:>7} {bw:>6} {basew:>7} "
              f"{vr:>5} {tw:>12}")


def print_summary_stats(df: pd.DataFrame, start_date: str, end_date: str) -> None:
    """打印全体收益分布统计。"""
    df_sorted = df.sort_values("ret", ascending=False).reset_index(drop=True)
    print(f"\n{'='*60}")
    print(f"  收益分布统计（{start_date} → {end_date}）")
    print(f"{'='*60}")
    ret = df_sorted["ret"].dropna()
    print(f"  总只数       : {len(ret)}")
    print(f"  上涨（>0%）  : {(ret > 0).sum()} 只 ({(ret > 0).mean()*100:.1f}%)")
    print(f"  上涨>10%     : {(ret > 10).sum()} 只 ({(ret > 10).mean()*100:.1f}%)")
    print(f"  上涨>20%     : {(ret > 20).sum()} 只 ({(ret > 20).mean()*100:.1f}%)")
    print(f"  上涨>30%     : {(ret > 30).sum()} 只 ({(ret > 30).mean()*100:.1f}%)")
    print(f"  下跌（<0%）  : {(ret < 0).sum()} 只 ({(ret < 0).mean()*100:.1f}%)")
    print(f"  下跌>-10%    : {(ret < -10).sum()} 只 ({(ret < -10).mean()*100:.1f}%)")
    print(f"  均值         : {ret.mean():.2f}%")
    print(f"  中位数       : {ret.median():.2f}%")
    print(f"  最大涨幅     : {ret.max():.2f}%")
    print(f"  最大跌幅     : {ret.min():.2f}%")

    # 按 score 分组的均值收益
    print(f"\n  按 score 分组的平均收益：")
    score_grp = df_sorted.groupby("score")["ret"].agg(["mean", "count"]).sort_index(ascending=False)
    for sc, row2 in score_grp.iterrows():
        print(f"    score={sc:3d}: 均值={row2['mean']:+6.2f}%  共 {int(row2['count'])} 只")

    # 按「是否有 trigger_week」分组
    print(f"\n  按「是否有启动信号」分组的平均收益：")
    df_sorted["has_trigger"] = df_sorted["trigger_week"].notna()
    grp2 = df_sorted.groupby("has_trigger")["ret"].agg(["mean", "count"])
    for has, row2 in grp2.iterrows():
        label = "有启动信号" if has else "无启动信号"
        print(f"    {label}: 均值={row2['mean']:+6.2f}%  共 {int(row2['count'])} 只")


def main():
    parser = argparse.ArgumentParser(description="Phase 4 回测校验")
    parser.add_argument("--candidates", default=DEFAULT_CANDIDATES)
    parser.add_argument(
        "--start-date",
        default=PRICE_START_DATE,
        help=f"区间起点（默认 {PRICE_START_DATE}，取该日及之后最近交易日收盘）",
    )
    parser.add_argument(
        "--end-date",
        default=PRICE_END_DATE,
        help=f"区间终点（默认 {PRICE_END_DATE}，取该日及之前最近交易日收盘）",
    )
    parser.add_argument("--top",    type=int, default=50, help="展示涨幅 TOP N")
    parser.add_argument("--bottom", type=int, default=20, help="展示涨幅 Bottom N")
    args = parser.parse_args()

    print(f"\n回测区间：{args.start_date} → {args.end_date}")
    df_cands = load_candidates(args.candidates)

    print("\n正在批量拉取起止价格...")
    df = calc_returns(df_cands, args.start_date, args.end_date)

    print_summary_stats(df, args.start_date, args.end_date)
    print_top_list(df, args.top, args.start_date, args.end_date)
    analyze_groups(df, args.top, args.bottom)

    # 保存带涨幅的完整结果
    out_path = Path(args.candidates).with_suffix("").as_posix() + "_backtest.csv"
    df.sort_values("ret", ascending=False).to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n完整回测结果已保存：{out_path}")


if __name__ == "__main__":
    main()
