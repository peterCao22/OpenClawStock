"""大盘环境评分模块（可独立使用，已接入 quant_picker 择时过滤）。

== 定位 ==
基于 ``index_bar1d``（上证 000001.SH / 沪深300 000300.SH），为任一交易日 T 计算一个
0~100 的“市场环境分”，并给出环境分级与出手建议。它把
``analyze_market_env.py`` 验证出的有效择时信号固化成一个可复用、可回测的打分函数，
供你日常参考、回测，将来若验证稳健再决定是否接入 ``quant_picker`` 的出手控制。

== 评分构成（权重）==
依据历史验证（可持有胜率单调性最强的信号优先）：
  - 沪深300 近20日动量 ``hs_ret20``      （权重 0.40，核心）
  - 上证 MA20 斜率 ``sse_ma20_slope``   （权重 0.25）
  - 上证 距60日高点回撤 ``sse_dd60``    （权重 0.20，非单调：浅调 -2~-6% 最佳）
  - 上证 是否站上 MA20 ``sse_above_ma20``（权重 0.15，趋势确认，较噪声故权重低）

各子项映射到 [0,1] 后加权求和 ×100。分级：
  ≥70 积极 / 50~70 中性 / 30~50 谨慎(减量) / <30 弱势(回避)

== 运行方式 ==
    # 打印某日（或最新可用日）的环境分
    python scripts/market_env_score.py --date 2026-04-17
    python scripts/market_env_score.py            # 用库中最新交易日

    # 回测：对验证样本里的各选股日打分，与历史胜率对照
    python scripts/market_env_score.py --backtest

注意:
    - index_bar1d 若未同步到 T，则用 ≤T 的最后一根（会提示数据滞后）；实盘前请先
      ``python scripts/sync_moma_data.py --index-bar`` 刷新。
    - 评分阈值集中在 SUBSCORE 区域，可按你的交易体系调整；目前样本仅 6 个市场环境，
      属“方向性强线索”，阈值不宜过度精调。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from scripts.analyze_market_env import load_index_features

WEIGHTS = {"momentum": 0.40, "slope": 0.25, "drawdown": 0.20, "trend": 0.15}


# ─── 子项打分（分段映射到 [0,1]）────────────────────────────────────────────
def _score_momentum(ret20: Optional[float]) -> Optional[float]:
    """沪深300 近20日动量 → 子分。越强越好，单调。"""
    if ret20 is None or pd.isna(ret20):
        return None
    if ret20 >= 3:
        return 1.0
    if ret20 >= 2:
        return 0.8
    if ret20 >= 0:
        return 0.5
    if ret20 >= -2:
        return 0.3
    return 0.0


def _score_slope(slope: Optional[float]) -> Optional[float]:
    """上证 MA20 斜率 → 子分。下行是明确减分项。"""
    if slope is None or pd.isna(slope):
        return None
    if slope >= 0.5:
        return 1.0
    if slope >= 0.3:
        return 0.8
    if slope > -0.3:
        return 0.5
    if slope > -0.7:
        return 0.2
    return 0.0


def _score_drawdown(dd60: Optional[float]) -> Optional[float]:
    """上证 距60日高点回撤 → 子分。非单调：浅调(-2~-6%)最佳，深调/亢奋次之。"""
    if dd60 is None or pd.isna(dd60):
        return None
    if -6 <= dd60 <= -2:
        return 1.0          # 浅回调甜点区
    if dd60 > -2:
        return 0.6          # 贴近高点（亢奋，弹性变小）
    if dd60 >= -10:
        return 0.3          # 中等回调
    return 0.0              # 深跌


def _score_trend(above_ma20: Optional[bool]) -> Optional[float]:
    """上证是否站上 MA20 → 子分（趋势确认）。"""
    if above_ma20 is None or pd.isna(above_ma20):
        return None
    return 1.0 if bool(above_ma20) else 0.2


# ─── 单日环境特征 + 评分 ─────────────────────────────────────────────────────
def env_features_at(date: Optional[str] = None) -> dict:
    """取某交易日（≤date 的最后一根；date 为空取最新）的大盘环境特征。

    返回 dict 含 sse_* / hs_ret20 / used_date / stale（是否滞后于请求日）。
    """
    sse = load_index_features("000001.SH")
    hs = load_index_features("000300.SH")
    ts = pd.Timestamp(date) if date else sse["date"].iloc[-1]

    def pick(df):
        sub = df[df["date"] <= ts]
        return sub.iloc[-1] if not sub.empty else None

    r_sse, r_hs = pick(sse), pick(hs)
    if r_sse is None or r_hs is None:
        return {}
    used_date = min(r_sse["date"], r_hs["date"])
    return {
        "used_date": str(used_date.date()),
        "stale": bool(date and used_date < ts),
        "sse_above_ma20": bool(r_sse["above_ma20"]) if pd.notna(r_sse["ma20"]) else None,
        "sse_ma20_slope": round(float(r_sse["ma20_slope"]), 2) if pd.notna(r_sse["ma20_slope"]) else None,
        "sse_dd_from_high60": round(float(r_sse["dd_from_high60"]), 2) if pd.notna(r_sse["dd_from_high60"]) else None,
        "sse_ret20": round(float(r_sse["ret20"]), 2) if pd.notna(r_sse["ret20"]) else None,
        "hs_ret20": round(float(r_hs["ret20"]), 2) if pd.notna(r_hs["ret20"]) else None,
    }


def env_score(feat: dict) -> dict:
    """由环境特征计算复合环境分（0~100）+ 分级 + 出手建议。

    参数:
        feat: ``env_features_at`` 的返回。

    返回:
        dict: score / regime / action / suggest_ratio（建议出手比例 0~1）/ components。
    """
    subs = {
        "momentum": _score_momentum(feat.get("hs_ret20")),
        "slope": _score_slope(feat.get("sse_ma20_slope")),
        "drawdown": _score_drawdown(feat.get("sse_dd_from_high60")),
        "trend": _score_trend(feat.get("sse_above_ma20")),
    }
    # 仅对有效子项按权重归一，避免某项缺失时整体被拉低
    avail = {k: v for k, v in subs.items() if v is not None}
    if not avail:
        return {"score": None, "regime": "数据不足", "action": "-", "suggest_ratio": None, "components": subs}
    w_sum = sum(WEIGHTS[k] for k in avail)
    score = sum(avail[k] * WEIGHTS[k] for k in avail) / w_sum * 100

    if score >= 70:
        regime, action, ratio = "积极", "正常/积极出手", 1.0
    elif score >= 50:
        regime, action, ratio = "中性", "正常出手，控制仓位", 0.7
    elif score >= 30:
        regime, action, ratio = "谨慎", "减量出手、提高入选门槛", 0.4
    else:
        regime, action, ratio = "弱势", "回避/空仓观望", 0.1

    return {"score": round(score, 1), "regime": regime, "action": action,
            "suggest_ratio": ratio, "components": {k: round(v, 2) if v is not None else None
                                                   for k, v in subs.items()}}


# ─── 打印 ────────────────────────────────────────────────────────────────────
def print_single(date: Optional[str]) -> None:
    feat = env_features_at(date)
    if not feat:
        print("[error] 无可用指数数据"); return
    res = env_score(feat)
    print("=" * 56)
    print(f" 大盘环境评分  目标日={date or '最新'}  取数日={feat['used_date']}"
          + ("  [⚠ 数据滞后，建议先 --index-bar 刷新]" if feat.get("stale") else ""))
    print("=" * 56)
    print(f"  沪深300近20日动量 = {feat.get('hs_ret20')}%   (子分 {res['components']['momentum']})")
    print(f"  上证 MA20 斜率    = {feat.get('sse_ma20_slope')}%  (子分 {res['components']['slope']})")
    print(f"  上证 距60日高点    = {feat.get('sse_dd_from_high60')}%  (子分 {res['components']['drawdown']})")
    print(f"  上证 站上MA20      = {feat.get('sse_above_ma20')}     (子分 {res['components']['trend']})")
    print("-" * 56)
    print(f"  >> 环境分 = {res['score']} / 100   分级: {res['regime']}")
    print(f"  >> 建议: {res['action']}（建议出手比例 ~{res['suggest_ratio']}）")
    print("\n  * 规则化环境提示，非投资建议。")


def print_backtest(csv: str) -> None:
    """对验证样本里的各选股日打分，并与历史胜率对照（验证评分有效性）。"""
    df = pd.read_csv(csv)
    print("=" * 92)
    print(" 回测：各选股日 环境分 vs 历史胜率（H=10）")
    print("=" * 92)
    print(f"  {'选股日':<12}{'环境分':>7}{'分级':>6}{'建议比例':>8}   |  历史实际(可持有/冲一波/末收益)")
    rows = []
    for dt, g in df.groupby("select_date"):
        feat = env_features_at(dt)
        res = env_score(feat) if feat else {"score": None}
        used = g[g["days_used_10"] >= 10].dropna(subset=["success_hold_10"])
        if len(used) == 0:
            actual = "n=0（未走满）"
            hold = None
        else:
            hold = used["success_hold_10"].mean() * 100
            mg = used["success_mg_10"].mean() * 100
            ret = used["ret_10"].mean()
            actual = f"可持有={hold:4.1f}%  冲一波={mg:4.1f}%  末收益={ret:+5.1f}%"
        sc = res.get("score")
        print(f"  {dt:<12}{(sc if sc is not None else '-'):>7}{res.get('regime','-'):>6}"
              f"{(res.get('suggest_ratio') if res.get('suggest_ratio') is not None else '-'):>8}   |  {actual}")
        if sc is not None and hold is not None:
            rows.append((sc, hold))
    if len(rows) >= 3:
        corr = pd.DataFrame(rows, columns=["score", "hold"]).corr().iloc[0, 1]
        print(f"\n  环境分 与 可持有胜率 的相关系数: {corr:+.2f}  （样本日 n={len(rows)}，仅作线索）")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--date", default=None, help="目标交易日 YYYY-MM-DD（默认库中最新）")
    parser.add_argument("--backtest", action="store_true", help="对验证样本各选股日打分并与胜率对照")
    parser.add_argument("--csv", default="output/selection_validation_regen.csv", help="回测用验证样本 CSV")
    args = parser.parse_args()

    if args.backtest:
        print_backtest(args.csv)
    else:
        print_single(args.date)


if __name__ == "__main__":
    main()
