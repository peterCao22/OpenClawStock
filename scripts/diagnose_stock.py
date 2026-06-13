"""单票诊断 + 接入建议脚本。

== 定位 ==
针对选股结果（``results/top_*.json``）里的某一只票，结合 ``kline_qfq`` 日 K 线做：
  1) 选股评分回看：从结果 JSON 取该票的 W 底窗口、月线加分、质量标签等；
  2) 锚点后表现：复用 ``rerank_top_stocks`` 的未来 N 日累计涨幅 / 涨停统计；
  3) 当前量价诊断：最新价、MA5/10/20、距锚点涨幅、距近期高点回撤、距 3 年高点、量能；
  4) 接入建议：给出“是否接入 / 接入区间 / 触发时机 / 止损位”及理由。

接入逻辑为启发式规则（非投资建议），核心思想是“顺势 + 不追高 + 回踩支撑分批”：
  - 跌破 MA20（中期趋势支撑）           → 回避 / 已破位止损；
  - 近 5 日涨幅过大且仍贴近阶段高点     → 观望，等回踩，不追高；
  - 回踩到 MA10 附近且站稳 MA20         → 逢低接入区（首选）；
  - 强势整理 / 突破未走远               → 突破或回踩均可，轻仓参与。

== 运行方式 ==
    python scripts/diagnose_stock.py 002138.SZ
    python scripts/diagnose_stock.py 002138.SZ --json results/top_50_stocks_2026-06-03.json --days 5

注意:
    - 本脚本只做形态/量价的客观刻画与规则化提示，不构成投资建议；阈值都在 RULES 区域，
      可按你自己的交易体系调整。
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from scripts.db_session import engine
from scripts.rerank_top_stocks import get_limit_thresh, get_w1_end_date

# ─── 规则阈值（可按自己的交易体系调整）────────────────────────────────────────
OVERHEAT_RUN5 = 20.0      # 近 5 日累计涨幅超过该值视为“短期过热”
NEAR_HIGH_GAP = 3.0       # 距阶段高点回撤小于该绝对值(%)视为“仍在高位”
PULLBACK_MA10_BUFFER = 1.03  # 收盘价 ≤ MA10*该系数 视为“回踩到 MA10 附近”
STOP_BELOW_MA20 = 0.97    # 止损参考：MA20 下方 3%
LOOKBACK_DAYS = 120       # 取近多少根日 K 计算均线与结构


# ─── 取数 ────────────────────────────────────────────────────────────────────
def load_recent_daily(instrument: str, lookback: int = LOOKBACK_DAYS) -> pd.DataFrame:
    """取该票最近 ``lookback`` 根日 K（升序），并补充 MA5/10/20、单日涨幅。

    参数:
        instrument: 标的代码。
        lookback:   取多少根 bar（用于均线计算，需 ≥20）。

    返回:
        含 date/open/high/low/close/volume + ma5/ma10/ma20 + chg_pct 的 DataFrame。
    """
    sql = (
        "SELECT date, open, high, low, close, volume FROM kline_qfq "
        "WHERE instrument = %(inst)s ORDER BY date DESC LIMIT %(lim)s"
    )
    df = pd.read_sql(sql, engine, params={"inst": instrument, "lim": lookback})
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    for w in (5, 10, 20):
        df[f"ma{w}"] = df["close"].rolling(w).mean()
    df["chg_pct"] = df["close"].pct_change() * 100
    return df


def load_stock_record(json_path: Optional[str], instrument: str) -> Optional[dict]:
    """从结果 JSON 中按代码取该票的选股记录（含评分与窗口）。找不到返回 None。"""
    if not json_path:
        return None
    p = Path(json_path)
    if not p.exists():
        return None
    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)
    for s in data:
        if s.get("instrument") == instrument:
            return s
    return None


# ─── 锚点后表现（与 rerank 口径一致）─────────────────────────────────────────
def forward_perf(df: pd.DataFrame, instrument: str, anchor_date: str, days: int) -> dict:
    """基于已加载的日 K，计算锚点日后 N 个交易日累计涨幅与涨停次数。

    与 rerank 口径一致：基准=锚点收盘，窗口=其后最多 days 根 bar，
    涨停按板块阈值（相邻收盘价涨幅）判定。
    """
    anchor_ts = pd.Timestamp(anchor_date)
    sub = df[df["date"] >= anchor_ts].reset_index(drop=True)
    out = {"anchor_close": None, "cum_return": None, "limit_up_cnt": 0,
           "days_used": 0, "daily": [], "end_date": None}
    if sub.empty:
        return out
    thresh = get_limit_thresh(instrument)
    closes = sub["close"].astype(float)
    anchor_close = float(closes.iloc[0])
    window = closes.iloc[1: 1 + days]
    n = len(window)
    out["anchor_close"] = round(anchor_close, 3)
    out["days_used"] = n
    if n == 0 or anchor_close == 0:
        return out
    daily_ret = closes.pct_change().iloc[1: 1 + n] * 100
    out["cum_return"] = round((float(closes.iloc[n]) / anchor_close - 1) * 100, 2)
    out["limit_up_cnt"] = int((daily_ret >= thresh).sum())
    out["daily"] = [round(float(x), 2) for x in daily_ret.tolist()]
    out["end_date"] = str(sub["date"].iloc[n].date())
    return out


# ─── 接入建议规则 ────────────────────────────────────────────────────────────
def make_entry_advice(df: pd.DataFrame, rec: Optional[dict]) -> dict:
    """根据最新量价 + 选股记录，给出接入建议。

    返回 dict: verdict(判定) / entry_zone(接入区间) / timing(时机) /
              stop_loss(止损位) / reasons(理由列表) / metrics(关键指标)。
    """
    last = df.iloc[-1]
    close = float(last["close"])
    ma5, ma10, ma20 = float(last["ma5"]), float(last["ma10"]), float(last["ma20"])

    # 近 5 日累计涨幅（用收盘价）
    run5 = None
    if len(df) >= 6:
        run5 = round((close / float(df["close"].iloc[-6]) - 1) * 100, 2)

    # 阶段高点（近 10 日最高收盘）与当前回撤
    recent_high = float(df["high"].iloc[-10:].max())
    pullback_from_high = round((close / recent_high - 1) * 100, 2)

    # 量能：最新量 / 近 5 日均量
    vol_ratio = round(float(last["volume"]) / float(df["volume"].iloc[-6:-1].mean()), 2) \
        if len(df) >= 6 else None

    reasons = []
    # ── 规则判定（从坏到好依次拦截）──────────────────────────────────────────
    if close < ma20:
        verdict = "回避 / 已破位"
        entry_zone = None
        timing = "跌破 MA20 中期趋势支撑，等重新站回 MA20 且放量再观察"
        reasons.append(f"收盘 {close:.2f} 已跌破 MA20({ma20:.2f})，中期趋势走弱")
    elif run5 is not None and run5 >= OVERHEAT_RUN5 and pullback_from_high > -NEAR_HIGH_GAP:
        verdict = "观望，不追高"
        entry_zone = (round(ma10, 2), round(ma20, 2))
        timing = "短期涨幅过大且仍贴近阶段高点，等回踩 MA10/MA20 不破再分批介入"
        reasons.append(f"近5日累计 +{run5:.1f}%，距阶段高点仅 {pullback_from_high:.1f}%，追高风险大")
    elif close <= ma10 * PULLBACK_MA10_BUFFER and close >= ma20:
        verdict = "逢低接入区（首选）"
        entry_zone = (round(ma20, 2), round(ma10, 2))
        timing = "已回踩至 MA10 附近且站稳 MA20，可在该区间分批接入"
        reasons.append(f"收盘 {close:.2f} 回踩 MA10({ma10:.2f}) 附近、未破 MA20({ma20:.2f})")
    else:
        verdict = "轻仓参与 / 突破再加"
        entry_zone = (round(ma10, 2), round(close, 2))
        timing = "趋势仍多头排列，回踩 MA10 可低吸，放量突破阶段高点可加仓"
        reasons.append("均线多头、未过热，处于可参与区间")

    # 止损位：MA20 下方缓冲，或反弹日收盘价（取较高者更稳）
    stop = round(ma20 * STOP_BELOW_MA20, 2)
    if rec and rec.get("rebound_date"):
        reasons.append(f"形态参考：{rec.get('pattern','')}，反弹日 {rec.get('rebound_date')}")

    # 趋势附注
    trend = "多头排列" if ma5 >= ma10 >= ma20 else ("均线纠缠" if ma5 >= ma20 else "空头排列")
    reasons.append(f"均线：MA5 {ma5:.2f} / MA10 {ma10:.2f} / MA20 {ma20:.2f}（{trend}）")
    if vol_ratio is not None:
        reasons.append(f"最新量能为近5日均量的 {vol_ratio} 倍")

    return {
        "verdict": verdict,
        "entry_zone": entry_zone,
        "timing": timing,
        "stop_loss": stop,
        "reasons": reasons,
        "metrics": {
            "last_close": round(close, 2), "ma5": round(ma5, 2),
            "ma10": round(ma10, 2), "ma20": round(ma20, 2),
            "run5_pct": run5, "recent_high": round(recent_high, 2),
            "pullback_from_high_pct": pullback_from_high, "vol_ratio": vol_ratio,
        },
    }


# ─── 输出 ────────────────────────────────────────────────────────────────────
def print_report(instrument: str, rec: Optional[dict], fwd: dict,
                 advice: dict, df: pd.DataFrame, days: int, anchor: str) -> None:
    """打印诊断报告（终端友好）。"""
    name = rec.get("name") if rec else ""
    print("=" * 66)
    print(f" 单票诊断  {instrument}  {name}")
    print("=" * 66)

    if rec:
        print("\n【选股评分回看】")
        print(f"  形态: {rec.get('pattern','-')} | windows: {rec.get('windows_info','-')}")
        print(f"  total_score={rec.get('total_score')}  final_score={rec.get('final_score')} "
              f"monthly_bonus={rec.get('monthly_bonus')}  quality_bonus={rec.get('quality_bonus')}")
        if rec.get("quality_tags"):
            print(f"  质量标签: {', '.join(rec['quality_tags'])}")
        m = rec.get("monthly") or {}
        if m:
            print(f"  月线: 距3年高点 {rec.get('close_vs_3y_peak')}  "
                  f"近6月涨幅 {m.get('six_month_return')}  量能扩张 {m.get('vol_expand_ratio')}")

    print(f"\n【锚点后 {days} 日表现】 锚点 {anchor}")
    print(f"  锚点收盘={fwd['anchor_close']}  截至 {fwd['end_date']}  "
          f"累计涨幅={fwd['cum_return']}%  涨停={fwd['limit_up_cnt']} 次  "
          f"(有效 {fwd['days_used']} 日: {fwd['daily']})")

    mt = advice["metrics"]
    print("\n【当前量价诊断】")
    print(f"  最新收盘={mt['last_close']}  近5日涨幅={mt['run5_pct']}%  "
          f"距阶段高点({mt['recent_high']})={mt['pullback_from_high_pct']}%  量比={mt['vol_ratio']}")

    print("\n【接入建议】")
    print(f"  >> 判定: {advice['verdict']}")
    if advice["entry_zone"]:
        lo, hi = advice["entry_zone"]
        print(f"  >> 接入区间: {lo} ~ {hi}")
    print(f"  >> 时机: {advice['timing']}")
    print(f"  >> 止损位: {advice['stop_loss']}")
    print("  >> 依据:")
    for r in advice["reasons"]:
        print(f"      - {r}")
    print("\n  * 以上为规则化形态/量价提示，非投资建议。")


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("instrument", help="标的代码，如 002138.SZ")
    parser.add_argument("--json", default="results/top_50_stocks_2026-06-03.json",
                        help="选股结果 JSON（用于回看评分与锚点日）")
    parser.add_argument("--days", type=int, default=5, help="锚点后统计的交易日数（默认 5）")
    args = parser.parse_args()

    df = load_recent_daily(args.instrument)
    if df.empty or len(df) < 20:
        sys.exit(f"[error] {args.instrument} 日K数据不足，无法诊断")

    rec = load_stock_record(args.json, args.instrument)
    anchor = get_w1_end_date(rec) if rec else str(df["date"].iloc[-args.days - 1].date())
    fwd = forward_perf(df, args.instrument, anchor, args.days)
    advice = make_entry_advice(df, rec)
    print_report(args.instrument, rec, fwd, advice, df, args.days, anchor)


if __name__ == "__main__":
    main()
