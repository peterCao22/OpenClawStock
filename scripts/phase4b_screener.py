"""
Phase 4B 筛选器：新鲜突破型（Fresh Active Breakout）

与 phase4_weekly_screener.py（v1.4，蓄势回踩型）互补，专门捕捉：
  - 触发 30-180 天内（新鲜信号，正在延续突破）
  - 当前收盘仍高于 MA20（活跃突破状态，而非回踩）
  - 当前收盘显著高于 MA120（长期趋势已确立，延伸中）
  - 近期有活跃大涨日（动量持续的正向信号）

典型目标：
  300749.SZ 顶固集创 (+169%, trigger 68d, cur/MA20=1.202, cur/MA120=1.580)
  300385.SZ 雪浪环境 (+166%, trigger 117d, cur/MA20=1.092, cur/MA120=1.227)
  688308.SH 欧科亿   (+151%, trigger 96d,  cur/MA20=1.204, cur/MA120=1.438)

数据源：与 Phase 4A（v1.4）相同的 kline_qfq 日K线 + 周K线转换
运行：python scripts/phase4b_screener.py --as-of 2025-12-31 [--top-n 100]

注意：
  - Phase 4A 与 Phase 4B 分别捕捉不同模式，结果可合并使用
  - Phase 4B 的"活跃=加分"与 4A 的"安静=加分"逻辑完全相反，勿混淆
  - 历史验证基准：v1.1 全市场 1454 候选股的 BOT20 中有高活跃型噪音（000796、603069），
    所以 4B 的精度天然低于 4A，需配合 Phase 2/3 实时信号做最终确认
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

# ── 路径 & 导入 ───────────────────────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

# 复用 Phase 4A 的公共工具（周K计算、MA指标、detect_launch 等）
from scripts.phase4_weekly_screener import (
    DATA_START,
    load_trading_calendar,
    load_all_instruments,
    load_kline_for_instrument,
    aggregate_to_weekly,
    calc_weekly_indicators,
    find_historical_peak,
    detect_bear_and_base,
    detect_launch,
    calc_recent_big_days,
    calc_post_trigger_ret,
    calc_ret_n_trading_days_pct,
    weekly_ma_alignment_score_from_series,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = ROOT_DIR / "output"

# ══════════════════════════════════════════════════════════════════════════════
# 一、Phase 4B 可配置参数
# ══════════════════════════════════════════════════════════════════════════════

# ── B 型专用硬过滤 ─────────────────────────────────────────────────────────────
# 触发天数窗口：30-180天（太新=信号未成熟；太老=不再是"新鲜"突破）
B_TRIGGER_MIN_DAYS = 30    # 触发后至少满此天数，信号才算有一定验证
B_TRIGGER_MAX_DAYS = 180   # 超过此天数归 Phase 4A（蓄势回踩型）管理

# 当前位置要求：必须高于 MA20（仍在活跃突破状态）
B_CUR_MA20_MIN = 1.05      # 收盘 ≥ MA20×1.05，确认未回踩到 MA20 下方

# MA120 扩张区间（突破后趋势延伸的合理范围）
B_CUR_MA120_MIN = 1.15     # 收盘 ≥ MA120×1.15，确认长期趋势已建立
B_CUR_MA120_MAX = 3.00     # 收盘 ≤ MA120×3.00，避免极端超买（ST等炒作）

# 触发后累积涨幅（确认突破有效，不是当天脉冲后立即回落）
B_POST_TRIGGER_RET_MIN =  5.0   # % 至少上涨 5%，确认突破不是假动作
B_POST_TRIGGER_RET_MAX = 100.0  # % 超过 100% 说明已暴涨完毕，超买排除

# 需要历史大熊市跌幅结构（和 Phase 4A 共用，确保"左侧有故事"）
DRAWDOWN_MIN = 0.30
DRAWDOWN_MAX = 0.80

# ── B 型评分阈值 ───────────────────────────────────────────────────────────────
# trigger_recency 评分：B 型中更新的信号=更好（与 4A 相反）
B_TRIGGER_FRESH_MAX  = 90   # 天，≤ 此值为"极新鲜"，满分
B_TRIGGER_FRESH_MID  = 120  # 天，≤ 此值为"较新鲜"，半分

# cur/MA20 突破强度（越高说明动量越强）
B_MA20_STRONG = 1.20   # ≥ 此值为强势突破（满分）
B_MA20_OK     = 1.05   # ≥ 此值为有效突破（半分）

# cur/MA120 扩张程度（太高=超买，中等=健康延伸）
B_MA120_IDEAL_MIN = 1.20   # 理想下界
B_MA120_IDEAL_MAX = 2.00   # 理想上界（满分区间）

# 近3月大涨日（B 型中"有活跃=好信号"，与 4A 相反）
B_RECENT_3M_ACTIVE_MIN = 2   # ≥ 此值为活跃动量（满分）
B_RECENT_3M_OK         = 1   # ≥ 此值有轻微动量（半分）

# 触发后累积涨幅理想区间
B_POST_RET_IDEAL_MIN = 10    # % 最低要有实质性突破
B_POST_RET_IDEAL_MAX = 60    # % 超过 60% 说明已大部分兑现

# Score 权重（满分 100 分）
# B 型的评分维度与 4A 完全相反：活跃=加分、新鲜=加分、MA20 上方=加分
B_SCORE_WEIGHTS = {
    "cur_ma20_active":      30,  # ★★ 当前高于MA20（突破有效性核心判断）
    "cur_ma120_extended":   20,  # ★★ 当前MA120扩张区间（趋势建立程度）
    "trigger_fresh":        15,  # ★  触发较新（30-120天，动量持续性）
    "recent_3m_active":     15,  # ★  近3月有大涨日（动量活跃信号）
    "post_trigger_ret_ok":  10,  # ★  触发后累积涨幅适中（有效但未透支）
    "drawdown_history":      7,  # 左侧有历史大跌（结构确认，非纯炒作）
    "pre_peak_surge":        3,  # 历史峰值验证（数据受限，权重低）
}  # 合计 100 分

# 默认输出 TOP-N
DEFAULT_TOP_N = 100


# ══════════════════════════════════════════════════════════════════════════════
# 二、评分函数
# ══════════════════════════════════════════════════════════════════════════════

def score_candidate_b(flags: Dict) -> int:
    """
    Phase 4B（新鲜突破型）评分函数，与 4A 评分逻辑完全相反。

    核心逻辑：
        1. cur_ma20_active (+30)：当前 close/MA20 ≥ 1.20 为强势（满分），≥1.05 中分
        2. cur_ma120_extended (+20)：当前 close/MA120 在 1.20~2.00（健康延伸）
        3. trigger_fresh (+15)：触发距今 ≤ 90 天满分，≤ 120 天半分（越新越好）
        4. recent_3m_active (+15)：近3月大涨日 ≥ 2 满分（越活跃越好，与4A相反）
        5. post_trigger_ret_ok (+10)：触发后涨幅 10%~60% 为理想区间
        6. drawdown_history (+7)：历史回撤 30%~80%（确认有左侧结构）
        7. pre_peak_surge (+3)：历史大牛确认

    参数：
        flags: screen_single_instrument_b 汇总的字段字典
    返回：
        int，score（0~100 分）
    """
    total = 0

    # 1. cur/MA20 突破强度（越高于MA20说明突破越有力，正向信号）
    cur_ma20 = flags.get("current_close_vs_ma20w")
    if cur_ma20 is not None:
        if cur_ma20 >= B_MA20_STRONG:
            total += B_SCORE_WEIGHTS["cur_ma20_active"]          # +30 强势突破
        elif cur_ma20 >= B_MA20_OK:
            total += B_SCORE_WEIGHTS["cur_ma20_active"] // 2     # +15 有效突破

    # 2. cur/MA120 延伸程度（1.20-2.00 为理想动量延伸区间）
    cur_ma120 = flags.get("current_close_vs_ma120w")
    if cur_ma120 is not None:
        if B_MA120_IDEAL_MIN <= cur_ma120 <= B_MA120_IDEAL_MAX:
            total += B_SCORE_WEIGHTS["cur_ma120_extended"]       # +20 理想区间
        elif cur_ma120 > B_MA120_IDEAL_MAX:
            total += B_SCORE_WEIGHTS["cur_ma120_extended"] // 2  # +10 稍过高，仍有动量

    # 3. trigger_recency 新鲜度（B型中越新越好，与4A相反）
    recency = flags.get("trigger_recency_days")
    if recency is not None:
        if recency <= B_TRIGGER_FRESH_MAX:
            total += B_SCORE_WEIGHTS["trigger_fresh"]            # +15 极新鲜
        elif recency <= B_TRIGGER_FRESH_MID:
            total += B_SCORE_WEIGHTS["trigger_fresh"] // 2      # +7 较新鲜

    # 4. 近3月大涨日（B型中活跃=正向信号）
    r3m = flags.get("recent_3m_big_days", 0)
    if r3m >= B_RECENT_3M_ACTIVE_MIN:
        total += B_SCORE_WEIGHTS["recent_3m_active"]             # +15 活跃动量
    elif r3m >= B_RECENT_3M_OK:
        total += B_SCORE_WEIGHTS["recent_3m_active"] // 2       # +7 轻微动量

    # 5. 触发后累积涨幅（B型以10-60%为理想区间，确认突破有效）
    ptr = flags.get("post_trigger_ret_pct")
    if ptr is not None:
        if B_POST_RET_IDEAL_MIN <= ptr <= B_POST_RET_IDEAL_MAX:
            total += B_SCORE_WEIGHTS["post_trigger_ret_ok"]      # +10 理想涨幅
        elif ptr > B_POST_RET_IDEAL_MAX:
            total += B_SCORE_WEIGHTS["post_trigger_ret_ok"] // 2 # +5 涨幅偏高

    # 6. 历史大熊市回撤（结构确认，排除纯热炒新股）
    dr = flags.get("drawdown_ratio") or 0.0
    if DRAWDOWN_MIN <= dr <= DRAWDOWN_MAX:
        total += B_SCORE_WEIGHTS["drawdown_history"]              # +7

    # 7. pre_peak_surge（历史大牛验证）
    if flags.get("pre_peak_surge", False):
        total += B_SCORE_WEIGHTS["pre_peak_surge"]               # +3

    return total


# ══════════════════════════════════════════════════════════════════════════════
# 三、单股筛选
# ══════════════════════════════════════════════════════════════════════════════

def screen_single_b(
    instrument: str,
    name: str,
    df_daily: pd.DataFrame,
    trading_cal: pd.DatetimeIndex,
    as_of: str,
) -> Optional[Dict]:
    """
    Phase 4B 单股筛选：检测新鲜突破型形态。

    硬过滤（B1-B6）与 Phase 4A 的 H1-H6 不同：
        B1: 必须有 trigger_week（与 H5 相同）
        B2: trigger_recency 在 30-180 天（新鲜窗口）
        B3: 当前 close/MA20 ≥ 1.05（仍在活跃突破状态）
        B4: 当前 close/MA120 在 1.15-3.00（突破后延伸，非极端）
        B5: 触发后累积涨幅 ≥ 5%（有效突破，非假动作）
        B6: 历史有足够回撤（DRAWDOWN_MIN~DRAWDOWN_MAX），排除新股/纯炒作

    参数：
        instrument: 股票代码
        name: 股票名称
        df_daily: 该股的日K数据（date/close/volume，已按日期升序）
        trading_cal: 交易日历（DatetimeIndex）
        as_of: 截止日期字符串
    返回：
        满足条件时返回候选字典；否则返回 None
    """
    as_of_ts = pd.Timestamp(as_of)

    # 周K聚合 & 指标计算（复用 Phase 4A 的周聚合逻辑）
    df_week = aggregate_to_weekly(df_daily, trading_cal)
    if df_week is None or len(df_week) < 130:
        return None
    df_week = calc_weekly_indicators(df_week)

    # 检测历史峰值（用于 drawdown_ratio / pre_peak_surge 计算）
    # find_historical_peak 返回 (peak_iloc, peak_close, pre_peak_surge)，注意是3个值
    peak_iloc, peak_close, pre_peak_surge = find_historical_peak(df_week)
    if peak_iloc is None:
        return None

    # 检测熊市底部结构（主要用于 drawdown_ratio / bear_weeks）
    bear_base = detect_bear_and_base(df_week, peak_iloc, peak_close)
    if bear_base is None:
        return None

    # B6：历史回撤范围硬过滤（确保有真实的熊市左侧结构）
    dr = bear_base.get("drawdown_ratio") or 0.0
    if not (DRAWDOWN_MIN <= dr <= DRAWDOWN_MAX):
        return None

    # 检测启动/触发信号
    base_end_iloc = bear_base.get("base_end_iloc") or (
        bear_base.get("trough_iloc") or peak_iloc
    )
    vol_med_base = bear_base.get("vol_med_base") or 0.0
    launch = detect_launch(df_week, base_end_iloc, vol_med_base)

    # B1：必须有触发信号
    trigger_ts = launch.get("launch_week")
    if trigger_ts is None:
        return None

    # B2：触发天数在 30-180 天窗口（新鲜突破型的核心条件）
    trigger_recency_days = (as_of_ts - trigger_ts).days
    if not (B_TRIGGER_MIN_DAYS <= trigger_recency_days <= B_TRIGGER_MAX_DAYS):
        return None

    # 计算 as-of 当周指标
    last_wk = df_week.iloc[-1]
    last_close  = float(last_wk["close"])
    last_ma20   = float(last_wk["ma20"])  if not pd.isna(last_wk.get("ma20",  float("nan"))) else None
    last_ma120  = float(last_wk["ma120"]) if not pd.isna(last_wk.get("ma120", float("nan"))) else None
    cur_ma20w   = round(last_close / last_ma20,  4) if last_ma20  else None
    cur_ma120w  = round(last_close / last_ma120, 4) if last_ma120 else None

    # 与 4A 一致的监控导出排序辅助字段（不参与 B 型 score）
    last_ma60 = (
        float(last_wk["ma60"])
        if pd.notna(last_wk.get("ma60", float("nan")))
        else None
    )
    current_close_vs_ma60w = round(last_close / last_ma60, 4) if last_ma60 else None
    sma18 = df_week["close"].rolling(18, min_periods=18).mean().iloc[-1]
    current_close_vs_ma18w = (
        round(last_close / float(sma18), 4)
        if pd.notna(sma18) and float(sma18) > 0
        else None
    )
    weekly_ma_alignment_score = weekly_ma_alignment_score_from_series(last_wk)
    ret_5d_pct = calc_ret_n_trading_days_pct(df_daily, as_of_ts, n=5)

    # B3：当前 close/MA20 ≥ 1.05（仍在 MA20 上方，突破有效）
    if cur_ma20w is None or cur_ma20w < B_CUR_MA20_MIN:
        return None

    # B4：当前 close/MA120 在合理扩张区间
    if cur_ma120w is None or not (B_CUR_MA120_MIN <= cur_ma120w <= B_CUR_MA120_MAX):
        return None

    # 计算触发后到 as_of 的累积涨幅
    post_trigger_ret = calc_post_trigger_ret(df_daily, trigger_ts, as_of_ts)

    # B5：触发后有实质性涨幅（排除假突破或立即回落）
    if post_trigger_ret is None or post_trigger_ret < B_POST_TRIGGER_RET_MIN:
        return None

    # B 型不额外做"触发后过高"的过滤（B型允许已涨50-80%，仍有延续动能）
    if post_trigger_ret > B_POST_TRIGGER_RET_MAX:
        return None

    # 近3月大涨日（B型正向指标）
    recent_3m_big = calc_recent_big_days(df_daily, as_of_ts, lookback_days=91)

    # 评分
    all_flags = {
        **bear_base,
        "pre_peak_surge":           pre_peak_surge,
        "trigger_recency_days":     trigger_recency_days,
        "current_close_vs_ma20w":   cur_ma20w,
        "current_close_vs_ma120w":  cur_ma120w,
        "recent_3m_big_days":       recent_3m_big,
        "post_trigger_ret_pct":     post_trigger_ret,
    }
    score = score_candidate_b(all_flags)

    # 低分候选直接排除（节省输出空间）
    if score < 30:
        return None

    trigger_week_str = str(trigger_ts.date())
    trough_date = (
        str(df_week.index[bear_base["trough_iloc"]].date())
        if bear_base.get("trough_iloc") is not None else None
    )

    return {
        "instrument":              instrument,
        "stock_name":              name,
        "score":                   score,
        "mode":                    "4B_fresh_breakout",
        # 当前状态（核心判断维度）
        "current_close_vs_ma20w":  cur_ma20w,
        "current_close_vs_ma120w": cur_ma120w,
        "recent_3m_big_days":      recent_3m_big,
        "post_trigger_ret_pct":    post_trigger_ret,
        "trigger_recency_days":    trigger_recency_days,
        # 触发信号
        "trigger_week":            trigger_week_str,
        "vr":                      launch["vr"],
        "launch_ma":               launch["launch_ma"],
        # 历史结构
        "drawdown_ratio":          bear_base["drawdown_ratio"],
        "bear_weeks":              bear_base["bear_weeks"],
        "trough_date":             trough_date,
        "pre_peak_surge":          pre_peak_surge,
        "current_close_vs_ma60w":  current_close_vs_ma60w,
        "current_close_vs_ma18w":  current_close_vs_ma18w,
        "weekly_ma_alignment_score": weekly_ma_alignment_score,
        "ret_5d_pct":              ret_5d_pct,
    }


# ══════════════════════════════════════════════════════════════════════════════
# 四、全市场主流程
# ══════════════════════════════════════════════════════════════════════════════

def run_screener_b(
    as_of: str,
    instruments: Optional[List[str]] = None,
    output_path: Optional[Path] = None,
    top_n: int = DEFAULT_TOP_N,
) -> None:
    """
    Phase 4B 全市场筛选主流程。

    参数：
        as_of: 截止日期字符串（如 "2025-12-31"）
        instruments: 指定股票列表；None 则扫描全市场
        output_path: 输出 JSON 路径；None 则自动生成
        top_n: 输出前 N 名（按分数排序）；≤0 则输出全部
    """
    if output_path is None:
        date_str = as_of.replace("-", "")
        output_path = OUTPUT_DIR / f"phase4b_candidates_{date_str}.json"

    logger.info(f"Phase 4B 筛选启动 | as_of={as_of} | output={output_path}")

    # 加载交易日历
    logger.info("加载交易日历..")
    trading_cal = load_trading_calendar(DATA_START, as_of)
    logger.info(f"交易日历加载完成：{len(trading_cal)} 个交易日（{DATA_START} ~ {as_of}）")

    # 加载股票列表
    df_stocks = load_all_instruments(as_of)
    stock_list = list(df_stocks.itertuples(index=False, name=None))  # (instrument, name) 列表
    if instruments:
        stock_list = [(inst, name) for inst, name in stock_list if inst in instruments]
        logger.info(f"按 --instruments 过滤后剩余 {len(stock_list)} 只股票")

    stats = {"total": len(stock_list), "screened": 0, "skipped_no_data": 0, "hit": 0}
    candidates = []

    for i, (inst, name) in enumerate(stock_list, 1):
        if i % 200 == 0:
            logger.info(f"进度 {i}/{stats['total']} | 命中 {stats['hit']} | 跳过（无数据）")

        # 加载日K数据（复用 load_kline_for_instrument）
        df_daily = load_kline_for_instrument(inst, DATA_START, as_of)
        if df_daily.empty:
            stats["skipped_no_data"] += 1
            continue

        stats["screened"] += 1

        result = screen_single_b(inst, name, df_daily, trading_cal, as_of)
        if result:
            candidates.append(result)
            stats["hit"] += 1

    # 排序：score 降序，同分时触发天数越小（越新鲜）越靠前
    candidates.sort(
        key=lambda x: (x["score"], -x.get("trigger_recency_days", 9999)),
        reverse=True,
    )
    total_before_topn = len(candidates)
    if top_n > 0 and len(candidates) > top_n:
        candidates = candidates[:top_n]
    logger.info(
        f"score 排序完成 | 全部命中={total_before_topn} | "
        f"输出 top_n={top_n if top_n > 0 else '全部'} → {len(candidates)} 只"
    )

    # 写出 JSON
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "as_of":        as_of,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode":         "Phase 4B - Fresh Active Breakout",
        "stats":        {**stats, "total_hit_before_topn": total_before_topn, "output_topn": len(candidates)},
        "params": {
            "version":               "4B-v1.0",
            "B_TRIGGER_MIN_DAYS":    B_TRIGGER_MIN_DAYS,
            "B_TRIGGER_MAX_DAYS":    B_TRIGGER_MAX_DAYS,
            "B_CUR_MA20_MIN":        B_CUR_MA20_MIN,
            "B_CUR_MA120_MIN":       B_CUR_MA120_MIN,
            "B_CUR_MA120_MAX":       B_CUR_MA120_MAX,
            "B_POST_TRIGGER_RET_MIN": B_POST_TRIGGER_RET_MIN,
            "B_POST_TRIGGER_RET_MAX": B_POST_TRIGGER_RET_MAX,
            "B_SCORE_WEIGHTS":       B_SCORE_WEIGHTS,
            "top_n":                 top_n,
        },
        "candidates": candidates,
    }
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)

    logger.info(f"筛选完成 | 命中 {stats['hit']} 只候选股 | 结果已写入：{output_path}")
    logger.info(
        f"统计：总计={stats['total']} 已扫描={stats['screened']} "
        f"无数据跳过={stats['skipped_no_data']} 命中={stats['hit']}"
    )


# ══════════════════════════════════════════════════════════════════════════════
# 五、命令行入口
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    """命令行入口，解析参数后调用 run_screener_b。"""
    parser = argparse.ArgumentParser(
        description="Phase 4B 筛选器：新鲜突破型（Fresh Active Breakout）"
    )
    parser.add_argument(
        "--as-of",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="截止日期，格式 YYYY-MM-DD（默认今日）",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="输出 JSON 路径（默认自动生成）",
    )
    parser.add_argument(
        "--instruments",
        default=None,
        help="逗号分隔的股票代码列表，用于单股/小批量调试",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=DEFAULT_TOP_N,
        help=f"输出前 N 名（默认 {DEFAULT_TOP_N}）；≤0 输出全部",
    )
    args = parser.parse_args()

    instruments = (
        [x.strip() for x in args.instruments.split(",")]
        if args.instruments
        else None
    )
    output_path = Path(args.output) if args.output else None

    run_screener_b(
        as_of=args.as_of,
        instruments=instruments,
        output_path=output_path,
        top_n=args.top_n,
    )


if __name__ == "__main__":
    main()
