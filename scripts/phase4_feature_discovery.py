"""
Phase 4 特征发现脚本：对 TOP20 / BOTTOM20 计算新特征，找真正能区分的维度

新特征候选：
  1. post_trigger_big_days_asof  : trigger → as_of 之间大涨日数（触发后实际动能）
  2. post_trigger_return_asof    : trigger → as_of 价格涨幅（累积走势）
  3. current_close_vs_ma20w      : as_of 当周收盘 vs MA20（近期均线方向）
  4. current_close_vs_ma120w     : as_of 当周收盘 vs MA120（当前位置）
  5. recent_3m_big_days          : as_of 前3个月大涨日数（近期活跃度）
  6. recent_6m_big_days          : as_of 前6个月大涨日数

运行：python scripts/phase4_feature_discovery.py
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

BACKTEST_CSV = "output/phase4_full_20251231_v2_backtest.csv"
AS_OF = pd.Timestamp("2025-12-31")
TOP_N = 20
BOTTOM_N = 20
BIG_GAIN_THRESH = 9.5  # %

# ── 加载回测分组 ───────────────────────────────────────────────────────────────
def load_groups():
    df = pd.read_csv(BACKTEST_CSV)
    df["ret"] = pd.to_numeric(df["ret"], errors="coerce")
    df = df.dropna(subset=["ret"]).sort_values("ret", ascending=False).reset_index(drop=True)
    top = df.head(TOP_N).copy()
    bot = df.tail(BOTTOM_N).copy()
    return top, bot

# ── 加载日K ────────────────────────────────────────────────────────────────────
def load_daily(inst: str) -> pd.DataFrame:
    sql = text(f"""
        SELECT date, close, volume FROM kline_qfq
        WHERE instrument='{inst}' AND date >= '2019-01-01' AND date <= '2025-12-31'
        ORDER BY date ASC
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    df["date"] = pd.to_datetime(df["date"])
    df["chg"] = df["close"].pct_change() * 100
    return df

# ── 加载周K指标（只需 close + MA） ──────────────────────────────────────────────
def load_weekly_ma(inst: str) -> pd.DataFrame:
    """返回周K，含 MA20 和 MA120，只用于读取 as_of 当周数值"""
    sql = text(f"""
        SELECT date, close FROM kline_qfq
        WHERE instrument='{inst}' AND date >= '2019-01-01' AND date <= '2025-12-31'
        ORDER BY date ASC
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    df["date"] = pd.to_datetime(df["date"])
    # 按 ISO 周聚合
    df["week_key"] = (
        df["date"].dt.isocalendar().year.astype(int) * 100
        + df["date"].dt.isocalendar().week.astype(int)
    )
    wk = df.groupby("week_key").agg(close=("close", "last")).reset_index()
    wk = wk.sort_values("week_key").reset_index(drop=True)
    wk["ma20"]  = wk["close"].rolling(20, min_periods=20).mean()
    wk["ma120"] = wk["close"].rolling(120, min_periods=120).mean()
    return wk

# ── 特征计算 ────────────────────────────────────────────────────────────────────
def compute_features(row: pd.Series) -> dict:
    inst = row["instrument"]
    trigger_str = str(row.get("trigger_week", "nan"))
    ret = float(row["ret"])

    df = load_daily(inst)
    if df.empty:
        return {}

    # 1. 近期大涨日
    recent_3m = df[df["date"] >= AS_OF - pd.Timedelta(days=91)].copy()
    recent_6m = df[df["date"] >= AS_OF - pd.Timedelta(days=182)].copy()
    recent_3m_big = int((recent_3m["chg"] >= BIG_GAIN_THRESH).sum())
    recent_6m_big = int((recent_6m["chg"] >= BIG_GAIN_THRESH).sum())

    # 2. post-trigger 特征（仅当有 trigger 时）
    post_big = np.nan
    post_ret = np.nan
    trigger_close = np.nan
    if trigger_str not in ("nan", "NaT", "None", ""):
        trig_ts = pd.Timestamp(trigger_str)
        # post_trigger_big_days_asof：trigger → as_of 大涨日
        post_df = df[(df["date"] > trig_ts) & (df["date"] <= AS_OF)].copy()
        post_big = int((post_df["chg"] >= BIG_GAIN_THRESH).sum()) if len(post_df) else 0
        # trigger 附近收盘（取 trigger 日期前后3个交易日的最近收盘）
        near = df[df["date"] <= trig_ts].tail(1)
        if not near.empty:
            trigger_close = float(near["close"].iloc[-1])
            current_close_val = float(df.iloc[-1]["close"])
            post_ret = (current_close_val - trigger_close) / trigger_close * 100

    # 3. 当前 close vs MA20/MA120（周K）
    wk = load_weekly_ma(inst)
    last_wk = wk.dropna(subset=["ma20", "ma120"])
    if not last_wk.empty:
        lr = last_wk.iloc[-1]
        cur_close = lr["close"]
        c_ma20  = round(cur_close / lr["ma20"], 4)
        c_ma120 = round(cur_close / lr["ma120"], 4)
    else:
        c_ma20, c_ma120 = np.nan, np.nan

    return {
        "instrument":        inst,
        "stock_name":        row.get("stock_name", ""),
        "ret":               ret,
        "trigger_week":      trigger_str,
        "trigger_recency_d": (AS_OF - pd.Timestamp(trigger_str)).days
                             if trigger_str not in ("nan", "NaT", "None", "") else np.nan,
        "post_trigger_big":  post_big,        # trigger→as_of 大涨日数
        "post_trigger_ret%": round(post_ret, 1) if not np.isnan(post_ret) else np.nan,
        "recent_3m_big":     recent_3m_big,   # 近3个月大涨日
        "recent_6m_big":     recent_6m_big,   # 近6个月大涨日
        "cur_close_vs_ma20": c_ma20,          # 当前收盘/MA20周
        "cur_close_vs_ma120":c_ma120,         # 当前收盘/MA120周
    }

# ── 主流程 ──────────────────────────────────────────────────────────────────────
def main():
    top, bot = load_groups()

    print(f"\n处理 TOP{TOP_N}...")
    top_feats = []
    for _, r in top.iterrows():
        print(f"  {r['instrument']} {r.get('stock_name','')} ret={r['ret']:.1f}%", end="\r")
        f = compute_features(r)
        if f:
            top_feats.append(f)
    df_top = pd.DataFrame(top_feats)
    df_top["group"] = "TOP"

    print(f"\n处理 BOT{BOTTOM_N}...")
    bot_feats = []
    for _, r in bot.iterrows():
        print(f"  {r['instrument']} {r.get('stock_name','')} ret={r['ret']:.1f}%", end="\r")
        f = compute_features(r)
        if f:
            bot_feats.append(f)
    df_bot = pd.DataFrame(bot_feats)
    df_bot["group"] = "BOT"

    df_all = pd.concat([df_top, df_bot], ignore_index=True)

    # ── 对比输出 ────────────────────────────────────────────────────────────────
    metrics = ["post_trigger_big", "post_trigger_ret%", "recent_3m_big",
               "recent_6m_big", "cur_close_vs_ma20", "cur_close_vs_ma120",
               "trigger_recency_d"]

    print(f"\n{'='*80}")
    print(f"  TOP{TOP_N} vs BOT{BOTTOM_N} 新特征均值/中位数对比")
    print(f"{'='*80}")
    print(f"  {'维度':<25} {'TOP均':>8} {'TOP中':>8} {'BOT均':>8} {'BOT中':>8}  {'方向'}")
    print(f"  {'-'*70}")
    for m in metrics:
        tv = df_top[m].dropna()
        bv = df_bot[m].dropna()
        if tv.empty or bv.empty:
            continue
        tm, ti, bm, bi = tv.mean(), tv.median(), bv.mean(), bv.median()
        direction = "TOP↑" if tm > bm else "BOT↑"
        print(f"  {m:<25} {tm:8.2f} {ti:8.2f} {bm:8.2f} {bi:8.2f}  {direction}")

    print(f"\n{'='*80}")
    print(f"  明细对比（按涨幅倒序）")
    print(f"{'='*80}")
    print(f"  {'代码':12} {'名称':8} {'涨幅%':>8} {'回撤触发天':>10} "
          f"{'触发后大涨':>10} {'触发后涨%':>10} {'近3月大涨':>9} {'近6月大涨':>9} "
          f"{'现/MA20w':>9} {'现/MA120w':>10}")
    print(f"  {'-'*110}")
    for g, label in [(df_top, "TOP"), (df_bot, "BOT")]:
        print(f"  ── {label} ──")
        for _, row in g.sort_values("ret", ascending=False).iterrows():
            print(f"  {row['instrument']:12} {str(row['stock_name']):8s} {row['ret']:8.1f}% "
                  f"{row.get('trigger_recency_d', float('nan')):10.0f} "
                  f"{row.get('post_trigger_big', float('nan')):10.0f} "
                  f"{row.get('post_trigger_ret%', float('nan')):10.1f} "
                  f"{row.get('recent_3m_big', float('nan')):9.0f} "
                  f"{row.get('recent_6m_big', float('nan')):9.0f} "
                  f"{row.get('cur_close_vs_ma20', float('nan')):9.3f} "
                  f"{row.get('cur_close_vs_ma120', float('nan')):10.3f}")

    # 分布命中率
    print(f"\n{'='*80}")
    print(f"  分布命中率对比")
    print(f"{'='*80}")
    checks = [
        ("post_trigger_big ≥ 3",    lambda r: r.get("post_trigger_big", 0) >= 3),
        ("post_trigger_big ≥ 1",    lambda r: r.get("post_trigger_big", 0) >= 1),
        ("post_trigger_ret% ≥ 20",  lambda r: r.get("post_trigger_ret%", -999) >= 20),
        ("post_trigger_ret% ≥ 50",  lambda r: r.get("post_trigger_ret%", -999) >= 50),
        ("recent_3m_big ≥ 2",       lambda r: r.get("recent_3m_big", 0) >= 2),
        ("recent_6m_big ≥ 3",       lambda r: r.get("recent_6m_big", 0) >= 3),
        ("cur/MA20 ≥ 1.05",         lambda r: (r.get("cur_close_vs_ma20") or 0) >= 1.05),
        ("cur/MA120 ≥ 1.20",        lambda r: (r.get("cur_close_vs_ma120") or 0) >= 1.20),
        ("cur/MA120 in 1.0~1.5",    lambda r: 1.0 <= (r.get("cur_close_vs_ma120") or 0) <= 1.5),
    ]
    for cname, cfunc in checks:
        t_hit = sum(1 for _, r in df_top.iterrows() if cfunc(r)) / max(len(df_top), 1)
        b_hit = sum(1 for _, r in df_bot.iterrows() if cfunc(r)) / max(len(df_bot), 1)
        diff = t_hit - b_hit
        marker = "★★" if abs(diff) >= 0.25 else ("★" if abs(diff) >= 0.15 else "  ")
        print(f"  {marker} {cname:<30} TOP={t_hit:.0%}  BOT={b_hit:.0%}  差={diff:+.0%}")

    out_path = "output/phase4_feature_discovery.csv"
    df_all.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n结果已保存：{out_path}")

if __name__ == "__main__":
    main()
