"""
Phase 4: 周线级别「大牛顶—长熊—筑底—启动」形态全市场筛选脚本

功能说明：
  - 从 kline_qfq（前复权日K线）+ trading_calendar（交易日历）出发，
    按 ISO 自然周聚合成周 K（内存中处理，v1 不建周 K 线物理表）。
  - 对全市场（或指定股票）依次识别：历史峰值 → 长熊回撤 → 筑底 → 启动+放量。
  - 对每只候选股计算简单加权 score（v1.5：含「启动后大涨日」动量项），按分排序后输出 JSON 文件。

运行方式：
    # 全市场截至今天
    python scripts/phase4_weekly_screener.py

    # 截至指定日期（用于历史回测）
    python scripts/phase4_weekly_screener.py --as-of 2025-12-31

    # 只跑单股调试
    python scripts/phase4_weekly_screener.py --instruments 002865.SZ

    # 使用 --output 自定义输出路径
    python scripts/phase4_weekly_screener.py --output output/my_result.json

注意事项：
  - 周界采用 ISO 自然周（周一到周日），以该周最后一个交易日为 week_end_date 索引。
  - 日 K 中的 ma5/ma10/ma20/ma60 为日线口径，本脚本不使用；
    所有周线 MA/VOL_MA 均由周 K 序列在脚本内重新计算。
  - 历史起点依赖 kline_qfq 实际数据；建议全市场数据覆盖到 2019-01-01 以上。
  - 全市场约 5000 只股票，首次运行可能耗时数分钟，请勿中断。
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import text

# ── 路径初始化 ────────────────────────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from scripts.db_session import engine  # noqa: E402

# ── 日志 ─────────────────────────────────────────────────────────────────────
LOG_DIR = ROOT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(
            LOG_DIR / f"phase4_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
            encoding="utf-8",
        ),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# 一、可配置参数（v1.5：在 v1.4 基础上加入启动后大涨日动量；硬过滤口径不变）
# ══════════════════════════════════════════════════════════════════════════════

# 数据最早起点
DATA_START = "2019-01-01"

# 峰值回撤阈值（H1 硬过滤 30%~80%）
# 注：30%~70% 为满分区间，70%~80% 为半分区间（负向信号但不绝对排除）
DRAWDOWN_MIN = 0.30
DRAWDOWN_MAX = 0.80
DRAWDOWN_SCORE_MAX = 0.70  # 满分上限

# 熊市时长（H2 硬过滤 78~208 周）
BEAR_WEEKS_MIN = 78
BEAR_WEEKS_MAX = 208

# 筑底检测参数
BASE_MIN_WEEKS = 20
BASE_VOLATILITY_PERCENTILE = 40
BASE_MA_COHESION_THRESH = 0.06

# 启动均线
LAUNCH_MA_SHORT = 120
LAUNCH_MA_LONG  = 250

# 启动放量基础门槛（H5 新增：无 trigger 直接排除，此处仅保留用于 detect_launch 内部判断）
VR_THRESH = 1.0
VOL_K_BASE_MULTIPLIER = 1.5

# H3：trigger_recency 硬过滤（>540 天排除）
TRIGGER_RECENCY_HARD_CUTOFF = 540   # 天，约 18 个月

# H4：启动前 26 周大涨日上限
PRE_TRIGGER_BIG_DAYS_MAX = 4    # 超过此值直接排除（假启动脉冲特征）
PRE_TRIGGER_BIG_DAYS_QUIET = 2  # ≤ 此值为安静期，加满分

# pre_peak_surge：左侧大牛验证
PRE_PEAK_SURGE_K = 2.5   # 峰值 ≥ 峰值前 N 年最低价 × k 倍
PRE_PEAK_SURGE_N = 3     # 回溯年数

# ── v1.3 关键新特征参数（基于 TOP20 vs BOT20 实证数据发现）────────────────────
# 核心洞察：TOP20 在 as-of 日是"低于MA20、高于MA120"的蓄势状态（回踩未超买）
# BOT20 在 as-of 日反而是"高于MA20"的过热状态

# 当前 close vs 周线 MA20（as-of 当周）
# TOP20 中位数 0.98（低于MA20）；BOT20 中位数 1.09（高于MA20）
CURRENT_MA20_QUIET_MAX = 1.05   # ≤ 此值为"安静蓄势"区间（满分）
CURRENT_MA20_OK_MAX    = 1.15   # ≤ 此值为"可接受"区间（半分）

# 当前 close vs 周线 MA120（as-of 当周）
# v1.4 采用渐进式评分：满分区间不变（0.95~1.20），扩展区间得半分
#   0.95~1.20：健康区间，满分（+20）
#   0.90~0.95：深度回踩，半分（+10）—— 捕捉 600821=0.932、600026=0.937
#   1.20~1.40：高位延伸，半分（+10）—— 捕捉 603115=1.395
#   < 0.90 或 > 1.40：0 分（H6 边界仍为 0.90）
CURRENT_MA120_UPTREND_MIN = 0.90  # H6 硬过滤边界（低于此值排除）
CURRENT_MA120_IDEAL_MIN   = 0.95  # 满分区间下界
CURRENT_MA120_IDEAL_MAX   = 1.20  # 满分区间上界
CURRENT_MA120_EXT_MAX     = 1.40  # 半分扩展区间上界

# 近3个月大涨日（as-of 前91天）
# TOP20 中位=0；BOT20 中位=0.5 → 近3月安静是正向信号
RECENT_3M_QUIET_MAX = 1     # ≤ 此值得满分（近3月几乎不活跃）
RECENT_3M_OK_MAX    = 3     # ≤ 此值得半分

# 触发后到 as-of 的价格涨幅
# TOP20 中位=6.25%；BOT20 中位=15% → BOT 涨太多后逆转！适度上涨是正向信号
POST_TRIGGER_RET_IDEAL_MIN = -10   # %
POST_TRIGGER_RET_IDEAL_MAX =  30   # %（超过30%算较多，减半分）

# trigger_recency 成熟度
# v1.4 采用三档渐进评分（不压低满分门槛，只在 90-180 天段加中间档）：
#   ≥ 180 天：满分（+15），趋势已充分验证
#   120~180 天：3/4 分（+11），—— 捕捉 300257=166d、300461=138d
#   60~120 天：半分（+7），—— 捕捉 600026=103d
#   < 60 天：0 分
# BOT20 入围几只触发均 > 200 天，三档改动不影响其分数
TRIGGER_MATURE_MIN    = 60   # 天，< 此值太新
TRIGGER_MATURE_MID    = 120  # 天，≥ 此值得 3/4 分（v1.4 新增）
TRIGGER_MATURE_BONUS  = 180  # 天，≥ 此值得满分（保持不变）

# VR 评分阈值
VR_SCORE_HIGH = 2.0
VR_SCORE_MID  = 1.5

# v1.5：启动后大涨日（trigger 当日至 as-of，单日涨幅≥阈值的天数）
# 与 pre_trigger 对称：表征「启动之后」是否持续出现大阳线；TOP20 均值显著高于 BOT20（见 phase_4.1）
POST_TRIGGER_BIG_THRESH_PCT = 9.5
POST_TRIGGER_BIG_DAYS_FULL = 8   # ≥ 此值拿满分档（对齐强势样本常见量级）
POST_TRIGGER_BIG_DAYS_MID1 = 6
POST_TRIGGER_BIG_DAYS_MID2 = 4
POST_TRIGGER_BIG_DAYS_LOW = 2

# Score 权重（v1.5，满分 100 分；动量项与蓄势项平衡，避免过度挤压头部龙头）
SCORE_WEIGHTS = {
    "cur_ma20_quiet":         28,  # 蓄势回踩仍是最强先验
    "cur_ma120_uptrend":      20,
    "trigger_recency_mature": 15,
    "recent_3m_quiet":        13,
    "post_trigger_return":     9,
    "post_trigger_momentum":   6,  # 启动后大涨日（略轻权，减轻对 20240524 宽池排序扰动）
    "drawdown_in_range":       6,
    "pre_peak_surge":          3,
}  # 合计 100 分

# 默认输出前 N 名（可通过 --top-n 覆盖）
DEFAULT_TOP_N = 250

# 输出目录
OUTPUT_DIR = ROOT_DIR / "output"


# ══════════════════════════════════════════════════════════════════════════════
# 二、数据读取
# ══════════════════════════════════════════════════════════════════════════════

def load_trading_calendar(start: str, end: str) -> pd.Series:
    """
    从 trading_calendar 加载指定区间内所有交易日，返回有序 Series（datetime64）。

    参数：
        start: 起始日期，格式 'YYYY-MM-DD'
        end:   截止日期，格式 'YYYY-MM-DD'
    返回：
        按升序排列的交易日 pd.Series（dtype datetime64[ns]）
    """
    sql = text(f"""
        SELECT trade_date
        FROM trading_calendar
        WHERE is_trading_day = true
          AND trade_date >= '{start}'
          AND trade_date <= '{end}'
        ORDER BY trade_date ASC
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    return pd.to_datetime(df["trade_date"])


def load_all_instruments(as_of: str) -> pd.DataFrame:
    """
    从 stock_list 读取全市场可用股票，过滤 ST、退市标的。

    参数：
        as_of: 截至日期字符串（目前仅用于日志标注，过滤逻辑基于名称）
    返回：
        DataFrame，含 instrument（代码）、name（名称）列
    注意：
        - 过滤逻辑与 Phase 2 quant_picker 保持一致，避免不同阶段样本口径差异。
    """
    sql = text("""
        SELECT instrument, name
        FROM stock_list
        WHERE name IS NOT NULL
          AND name != ''
          AND name NOT LIKE '%ST%'
          AND name NOT LIKE '%退%'
        ORDER BY instrument ASC
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn)


def load_kline_for_instrument(instrument: str, start: str, end: str) -> pd.DataFrame:
    """
    读取单只股票指定区间日 K 数据（前复权）。

    参数：
        instrument: 股票代码，如 '002865.SZ'
        start:      起始日期 'YYYY-MM-DD'
        end:        截止日期 'YYYY-MM-DD'
    返回：
        DataFrame，按日期升序排列，含 date/open/high/low/close/volume 列
        若无数据则返回空 DataFrame
    """
    sql = text(f"""
        SELECT date, open, high, low, close, volume, amount
        FROM kline_qfq
        WHERE instrument = '{instrument}'
          AND date >= '{start}'
          AND date <= '{end}'
        ORDER BY date ASC
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    df["date"] = pd.to_datetime(df["date"])
    return df


# ══════════════════════════════════════════════════════════════════════════════
# 三、周 K 聚合（ISO 自然周，week_end_date = 该周最后一个交易日）
# ══════════════════════════════════════════════════════════════════════════════

def aggregate_to_weekly(df_daily: pd.DataFrame, trading_days: pd.Series) -> pd.DataFrame:
    """
    将日 K DataFrame 聚合为周 K，周界采用 ISO 自然周。

    聚合规则：
        - open:   该周第一个交易日的 open
        - high:   该周最高价
        - low:    该周最低价
        - close:  该周最后一个交易日的 close
        - volume: 周内日成交量之和（单位与日 K 一致，已折算为「股」）
        - amount: 周内成交额之和（如有）
        - week_end_date: 该周最后一个交易日（作为索引 key）

    参数：
        df_daily:     日 K DataFrame（须含 date/open/high/low/close/volume 列）
        trading_days: 全局交易日历 Series（用于确定每周的实际最后一个交易日）
    返回：
        周 K DataFrame，index 为 week_end_date，按升序排列
    注意：
        - 以 trading_days 为锚，确保每周取「实际最后交易日」而非自然周末；
          这样能正确处理节假日导致的短周（只有 1~2 个交易日）。
        - 不足 1 根完整周 K 的末尾碎片仍保留，便于实时监控最新一周状态。
    """
    if df_daily.empty:
        return pd.DataFrame()

    df = df_daily.copy()
    df["date"] = pd.to_datetime(df["date"])

    # 用 ISO 周号（year + week）给每个交易日打标签
    # isocalendar() 返回 (year, week, weekday)；用 year*100+week 作为分组 key
    df["iso_year"] = df["date"].dt.isocalendar().year.astype(int)
    df["iso_week"] = df["date"].dt.isocalendar().week.astype(int)
    df["week_key"] = df["iso_year"] * 100 + df["iso_week"]

    # 用同样方法给全局交易日历打 week_key，便于找每周最后一个交易日
    td_df = pd.DataFrame({"trade_date": trading_days})
    td_df["iso_year"] = td_df["trade_date"].dt.isocalendar().year.astype(int)
    td_df["iso_week"] = td_df["trade_date"].dt.isocalendar().week.astype(int)
    td_df["week_key"] = td_df["iso_year"] * 100 + td_df["iso_week"]
    # 每个 week_key 的最后一个交易日（即该周收盘日）
    last_td_per_week = td_df.groupby("week_key")["trade_date"].max()

    # 聚合 OHLCV
    agg = df.groupby("week_key").agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
        amount=("amount", "sum"),
    )

    # 映射 week_end_date（使用交易日历确定收盘日，而非自然周末）
    agg["week_end_date"] = agg.index.map(last_td_per_week)

    # 部分 week_key 可能因数据缺失无法映射（如仅在 df_daily 出现却不在 trading_days 中）；
    # 这极少发生，但保险起见丢掉，避免 NaT 污染后续计算
    agg = agg.dropna(subset=["week_end_date"])
    agg = agg.sort_values("week_end_date").reset_index(drop=True)
    agg.set_index("week_end_date", inplace=True)

    return agg


# ══════════════════════════════════════════════════════════════════════════════
# 四、周线技术指标
# ══════════════════════════════════════════════════════════════════════════════

def calc_weekly_indicators(df_week: pd.DataFrame) -> pd.DataFrame:
    """
    在周 K DataFrame 上计算所需均线与量能指标。

    计算指标：
        - ma5, ma10, ma20, ma60, ma120, ma250  （基于周收盘价）
        - vol_ma5, vol_ma10, vol_ma20           （基于周成交量）
    参数：
        df_week: 周 K DataFrame（index=week_end_date，含 close/volume 列）
    返回：
        原 DataFrame（inplace 添加指标列）
    注意：
        - 窗口期不足时为 NaN，调用方需处理。
        - 此处均为简单移动平均（SMA）；后续如需 EMA 可按需扩展。
    """
    c = df_week["close"]
    v = df_week["volume"]

    for n in [5, 10, 20, 60, 120, 250]:
        # min_periods=n 确保前 n-1 根为 NaN，避免用不足 n 周数据的伪均值
        df_week[f"ma{n}"] = c.rolling(n, min_periods=n).mean()

    for n in [5, 10, 20]:
        df_week[f"vol_ma{n}"] = v.rolling(n, min_periods=n).mean()

    # 周振幅：提前计算以便 detect_bear_and_base 中的切片子集也能访问
    df_week["amplitude_w"] = (df_week["high"] - df_week["low"]) / df_week["close"]

    return df_week


# ══════════════════════════════════════════════════════════════════════════════
# 五、单股规则引擎
# ══════════════════════════════════════════════════════════════════════════════

def find_historical_peak(df_week: pd.DataFrame) -> Tuple[Optional[int], float, bool]:
    """
    在周 K 序列中找历史峰值，并检验是否为「真实大牛顶」（pre_peak_surge）。

    参数：
        df_week: 周 K DataFrame
    返回：
        (peak_iloc, peak_close, pre_peak_surge)
            peak_iloc:      峰值所在行的整数位置
            peak_close:     峰值收盘价
            pre_peak_surge: bool，峰值是否 ≥ 峰值前 N 年最低价 × k 倍
                            用于排除「2021年小牛」vs「真正大牛」的混淆
    注意：
        - 若峰值前数据不足 N 年，pre_peak_surge 保守设为 False。
    """
    if df_week.empty:
        return None, 0.0, False

    peak_label = df_week["close"].idxmax()
    peak_iloc = df_week.index.get_loc(peak_label)
    peak_close = float(df_week["close"].iloc[peak_iloc])

    # 检验 pre_peak_surge：峰值前 PRE_PEAK_SURGE_N 年的最低收盘价
    lookback_weeks = PRE_PEAK_SURGE_N * 52
    pre_peak_start = max(0, peak_iloc - lookback_weeks)
    pre_peak_series = df_week["close"].iloc[pre_peak_start: peak_iloc]

    if len(pre_peak_series) < 26:
        # 峰值前数据不足半年，无法判断，保守返回 False
        pre_peak_surge = False
    else:
        pre_peak_low = float(pre_peak_series.min())
        pre_peak_surge = (pre_peak_low > 0) and (peak_close >= pre_peak_low * PRE_PEAK_SURGE_K)

    return peak_iloc, peak_close, pre_peak_surge


def detect_bear_and_base(
    df_week: pd.DataFrame,
    peak_iloc: int,
    peak_close: float,
) -> Dict:
    """
    在峰值之后识别：熊市回撤（长度与幅度）+ 筑底区间。

    参数：
        df_week:    完整周 K DataFrame（含指标列）
        peak_iloc:  峰值行整数位置
        peak_close: 峰值收盘价
    返回：
        dict，含以下 key（全部可能为 None）：
            bear_weeks         : 下跌时长（周数）
            drawdown_ratio     : 最大回撤（0~1，例如 0.65 = 跌了65%）
            trough_iloc        : 最低点行位置
            trough_close       : 最低点收盘价
            base_start_iloc    : 筑底区间起始行位置
            base_end_iloc      : 筑底区间结束行位置（即启动前一周或等于 as_of 行）
            base_weeks         : 筑底区间长度（周数）
            vol_med_base       : 筑底区间内周成交量中位数
            drawdown_in_range  : bool，回撤是否在 [DRAWDOWN_MIN, DRAWDOWN_MAX]
            bear_duration_ok   : bool，下跌时长是否在 [BEAR_WEEKS_MIN, BEAR_WEEKS_MAX]
            base_formed        : bool，是否检测到有效筑底
    注意：
        - 「筑底」采用 v1 近似：从最低点（trough）开始，向后寻找满足
          「波动收敛 + 均线粘合」条件的连续区间。
        - 若均线数据不足（窗口期内 NaN 较多），base_formed 降级为 False。
    """
    result = {
        "bear_weeks": None, "drawdown_ratio": None,
        "trough_iloc": None, "trough_close": None,
        "base_start_iloc": None, "base_end_iloc": None,
        "base_weeks": None, "vol_med_base": None,
        "drawdown_in_range": False, "bear_duration_ok": False,
        "base_formed": False,
    }

    after_peak = df_week.iloc[peak_iloc + 1:]
    if len(after_peak) < BEAR_WEEKS_MIN:
        # 峰值后数据太少，不足以形成有效长熊
        return result

    # ── 5.1 找全局最低点（峰值后序列中的收盘价最低点）──────────────────────
    trough_rel_iloc = int(after_peak["close"].idxmin()) \
        if isinstance(after_peak["close"].idxmin(), int) else None
    trough_label = after_peak["close"].idxmin()
    trough_rel_iloc = after_peak.index.get_loc(trough_label)  # 相对 df_week 的 iloc
    trough_iloc = peak_iloc + 1 + list(after_peak.index).index(trough_label)
    trough_close = float(after_peak.loc[trough_label, "close"])

    bear_weeks = trough_iloc - peak_iloc
    drawdown = (peak_close - trough_close) / peak_close

    result["bear_weeks"] = bear_weeks
    result["drawdown_ratio"] = round(drawdown, 4)
    result["trough_iloc"] = trough_iloc
    result["trough_close"] = trough_close

    # ── 5.2 检验回撤与时长 ────────────────────────────────────────────────────
    result["drawdown_in_range"] = DRAWDOWN_MIN <= drawdown <= DRAWDOWN_MAX
    result["bear_duration_ok"] = BEAR_WEEKS_MIN <= bear_weeks <= BEAR_WEEKS_MAX

    # ── 5.3 识别筑底区间 ──────────────────────────────────────────────────────
    # 取最低点之后的数据作为候选筑底区间
    after_trough = df_week.iloc[trough_iloc:]
    if len(after_trough) < BASE_MIN_WEEKS:
        return result

    # 用滚动窗口（步长 1，窗口宽 BASE_MIN_WEEKS）找第一个满足两个条件的段：
    #   (a) 振幅收敛：窗口内 amplitude 的中位数 < 熊市段振幅的 BASE_VOLATILITY_PERCENTILE 分位数
    #   (b) 均线粘合：窗口内每周各 MA5/10/20/60 之间的相对离散度 < BASE_MA_COHESION_THRESH

    # 熊市段振幅参考分位数（用峰值→最低点区间；amplitude_w 已在 calc_weekly_indicators 中计算）
    bear_ampl = df_week["amplitude_w"].iloc[peak_iloc: trough_iloc + 1]
    if bear_ampl.empty or bear_ampl.isna().all():
        return result
    bear_ampl_thresh = float(np.nanpercentile(bear_ampl.dropna(), BASE_VOLATILITY_PERCENTILE))

    # 找最长筑底区间（v1.1 改进：不在第一个满足窗口停止，而是找最长连续合格段）
    # 先逐行打标：每个 trough 后的行是否满足筑底条件（振幅 + 均线粘合）
    qualify_flags = []
    ma_cols = [c for c in ["ma5", "ma10", "ma20", "ma60"] if c in after_trough.columns]

    for i in range(len(after_trough)):
        row = after_trough.iloc[i]
        ampl = row.get("amplitude_w", np.nan)
        # 振幅收敛：单行振幅 < 熊市振幅阈值
        ampl_ok = (not pd.isna(ampl)) and (ampl < bear_ampl_thresh * 1.2)
        qualify_flags.append(ampl_ok)

    # 在 qualify_flags 基础上，滑动 BASE_MIN_WEEKS 窗口找第一个「整窗口合格」的起点，
    # 然后尽量延伸终点（连续合格即继续）
    base_start = None
    base_end   = None

    for start_i in range(len(after_trough) - BASE_MIN_WEEKS + 1):
        window = after_trough.iloc[start_i: start_i + BASE_MIN_WEEKS]

        # (a) 整窗口振幅中位数检验
        ampl_median = window["amplitude_w"].median()
        if pd.isna(ampl_median) or ampl_median >= bear_ampl_thresh:
            continue

        # (b) 均线粘合检验（可选）
        if ma_cols:
            ma_matrix = window[ma_cols].dropna(how="any")
            if len(ma_matrix) >= BASE_MIN_WEEKS // 2:
                cohesion = (
                    ma_matrix.std(axis=1) / window["close"].reindex(ma_matrix.index)
                ).mean()
                if not pd.isna(cohesion) and cohesion >= BASE_MA_COHESION_THRESH:
                    continue

        # 找到合格起点，向后延伸寻找最长连续合格区间
        base_start = trough_iloc + start_i
        end_i = start_i + BASE_MIN_WEEKS - 1

        for ext_i in range(start_i + BASE_MIN_WEEKS, len(after_trough)):
            ext_ampl = after_trough.iloc[ext_i].get("amplitude_w", np.nan)
            # 放宽延伸条件（只要振幅不突然飙升，就继续纳入筑底期）
            if (not pd.isna(ext_ampl)) and (ext_ampl < bear_ampl_thresh * 1.5):
                end_i = ext_i
            else:
                break

        base_end = trough_iloc + end_i
        break

    if base_start is not None:
        base_weeks = base_end - base_start + 1
        base_vol_series = df_week["volume"].iloc[base_start: base_end + 1]
        vol_med_base = float(base_vol_series.median())

        result["base_start_iloc"] = base_start
        result["base_end_iloc"]   = base_end
        result["base_weeks"]      = base_weeks
        result["vol_med_base"]    = vol_med_base
        result["base_formed"]     = True

    return result


def detect_launch(
    df_week: pd.DataFrame,
    base_end_iloc: int,
    vol_med_base: float,
) -> Dict:
    """
    在筑底区间之后，识别「启动」信号：
        - 收盘突破 MA120 或 MA250（满足其一，且在近 LAUNCH_LOOKBACK 周内发生）
        - 同步满足放量组合：VR >= VR_THRESH 且 当周量 >= VOL_K_BASE_MULTIPLIER × vol_med_base

    参数：
        df_week:       完整周 K DataFrame（含指标列）
        base_end_iloc: 筑底区间结束行的 iloc 位置
        vol_med_base:  筑底期周成交量中位数（放量基准）
    返回：
        dict，含：
            launch_week       : 启动周的 week_end_date（Timestamp or None）
            trigger_iloc      : 启动周的 iloc 位置（or None）
            close_vs_ma120    : 启动周收盘 / MA120（or None）
            close_vs_ma250    : 启动周收盘 / MA250（or None）
            vr                : 启动周 VR=volume/vol_ma20（or None）
            launch_ma         : bool，是否满足均线突破条件
            launch_volume_vr  : bool，是否满足 VR 放量条件
            launch_volume_base: bool，是否满足相对筑底量放量条件
    注意：
        - LAUNCH_LOOKBACK 控制往后看多少周寻找启动信号；
          目前设置为 52 周（约 1 年），平衡「不错过慢启动」与「避免过久前信号」。
        - 若 MA120/MA250 因数据窗口不足为 NaN，对应条件视为未满足，不报错。
    """
    LAUNCH_LOOKBACK = 52  # 从筑底结束后，最多向后看多少周寻找启动（约 1 年）

    result = {
        "launch_week": None, "trigger_iloc": None,
        "close_vs_ma120": None, "close_vs_ma250": None, "vr": None,
        "launch_ma": False, "launch_volume_vr": False, "launch_volume_base": False,
    }

    # 候选区间：筑底结束后的行
    candidate = df_week.iloc[base_end_iloc + 1: base_end_iloc + 1 + LAUNCH_LOOKBACK]
    if candidate.empty:
        return result

    for label, row in candidate.iterrows():
        close = row["close"]

        # 均线突破条件（MA120 或 MA250 满足其一）
        ma120 = row.get("ma120", np.nan)
        ma250 = row.get("ma250", np.nan)
        cond_ma120 = (not pd.isna(ma120)) and (close > ma120)
        cond_ma250 = (not pd.isna(ma250)) and (close > ma250)
        launch_ma = cond_ma120 or cond_ma250

        if not launch_ma:
            continue  # 均线条件是必要条件，不满足跳过

        # 放量条件
        vol = row["volume"]
        vol_ma20 = row.get("vol_ma20", np.nan)
        vr = (vol / vol_ma20) if (not pd.isna(vol_ma20) and vol_ma20 > 0) else np.nan
        cond_vr   = (not pd.isna(vr)) and (vr >= VR_THRESH)
        cond_base = (vol_med_base > 0) and (vol >= VOL_K_BASE_MULTIPLIER * vol_med_base)

        # 同时满足两个放量条件的才记录为「强启动」；
        # 若只满足其一，仍记录但放量 flag 为部分满足（v1 保留灵活性，score 会体现差距）
        trigger_iloc = df_week.index.get_loc(label)

        result["launch_week"]        = label
        result["trigger_iloc"]       = trigger_iloc
        result["close_vs_ma120"]     = round(close / ma120, 4) if not pd.isna(ma120) else None
        result["close_vs_ma250"]     = round(close / ma250, 4) if not pd.isna(ma250) else None
        result["vr"]                 = round(float(vr), 4) if not pd.isna(vr) else None
        result["launch_ma"]          = True
        result["launch_volume_vr"]   = bool(cond_vr)
        result["launch_volume_base"] = bool(cond_base)
        # 取第一个满足均线突破的周作为启动周
        break

    return result


def score_candidate(flags: Dict) -> int:
    """
    根据多条件 flag 计算加权 score（v1.5）。

    在 v1.4 基础上增加「启动后大涨日」动量项（post_trigger_momentum），其余维度口径不变。
    满分仍 100；权重从 MA20/近3月/触发后涨幅/回撤/pre_peak 略腾挪，避免总分膨胀。

    分项要点：
        1. cur_ma20_quiet（28）：close/MA20w 蓄势
        2. cur_ma120_uptrend（20/10）：MA120 渐进评分
        3. trigger_recency_mature（15/11/7）：触发成熟度三档
        4. recent_3m_quiet（13/6）：近3月安静度
        5. post_trigger_return（9/4）：触发后累计涨幅适中
        6. post_trigger_momentum（6/4/3/1）：启动后大涨日密度（v1.5 新增，分档随权重缩放）
        7. drawdown_in_range（6/3）：回撤区间
        8. pre_peak_surge（3）：左侧大牛

    参数：
        flags: screen_single_instrument 汇总的所有字段字典（须含 post_trigger_big_days）
    返回：
        int，score（0~100 分）
    """
    total = 0

    # 1. 当前 close/MA20w（蓄势回踩状态，TOP20的最强正向信号）
    cur_ma20 = flags.get("current_close_vs_ma20w")
    if cur_ma20 is not None:
        if cur_ma20 <= CURRENT_MA20_QUIET_MAX:
            # ≤1.05：处于MA20下方或刚好附近，最理想蓄势位置
            total += SCORE_WEIGHTS["cur_ma20_quiet"]              # +30
        elif cur_ma20 <= CURRENT_MA20_OK_MAX:
            # 1.05~1.15：略高于MA20，可接受
            total += SCORE_WEIGHTS["cur_ma20_quiet"] // 2         # +15

    # 2. 当前 close/MA120w（渐进式评分，v1.4）
    # 满分区间 0.95-1.20 不变；扩展区间 0.90-0.95 和 1.20-1.40 得半分
    # 这样不会膨胀高分档，只精准帮助深度回踩或高位延伸的边缘好股
    cur_ma120 = flags.get("current_close_vs_ma120w")
    if cur_ma120 is not None:
        if CURRENT_MA120_IDEAL_MIN <= cur_ma120 <= CURRENT_MA120_IDEAL_MAX:
            # 0.95~1.20：理想健康区间，满分
            total += SCORE_WEIGHTS["cur_ma120_uptrend"]           # +20
        elif (CURRENT_MA120_UPTREND_MIN <= cur_ma120 < CURRENT_MA120_IDEAL_MIN
              or CURRENT_MA120_IDEAL_MAX < cur_ma120 <= CURRENT_MA120_EXT_MAX):
            # 0.90~0.95（深度回踩）或 1.20~1.40（高位延伸）：半分
            total += SCORE_WEIGHTS["cur_ma120_uptrend"] // 2      # +10

    # 3. trigger_recency 三档渐进评分（v1.4）
    # ≥180天: 满分；120-180天: 3/4分；60-120天: 半分；<60天: 0
    recency = flags.get("trigger_recency_days")
    if recency is not None:
        if recency >= TRIGGER_MATURE_BONUS:
            total += SCORE_WEIGHTS["trigger_recency_mature"]      # +15
        elif recency >= TRIGGER_MATURE_MID:
            total += int(SCORE_WEIGHTS["trigger_recency_mature"] * 0.75)  # +11
        elif recency >= TRIGGER_MATURE_MIN:
            total += SCORE_WEIGHTS["trigger_recency_mature"] // 2 # +7

    # 4. 近3个月大涨日（安静=蓄势待发的正向信号）
    r3m = flags.get("recent_3m_big_days", 999)
    if r3m <= RECENT_3M_QUIET_MAX:
        total += SCORE_WEIGHTS["recent_3m_quiet"]                 # +15 极度安静
    elif r3m <= RECENT_3M_OK_MAX:
        total += SCORE_WEIGHTS["recent_3m_quiet"] // 2            # +7 基本安静

    # 5. 触发后到 as-of 的累积涨幅（适中最佳，BOT20涨太多后逆转）
    ptr = flags.get("post_trigger_ret_pct")
    if ptr is not None:
        if POST_TRIGGER_RET_IDEAL_MIN <= ptr <= POST_TRIGGER_RET_IDEAL_MAX:
            total += SCORE_WEIGHTS["post_trigger_return"]         # +10 理想区间
        elif POST_TRIGGER_RET_IDEAL_MAX < ptr <= 60:
            total += SCORE_WEIGHTS["post_trigger_return"] // 2    # +5 涨幅偏多
        elif -30 <= ptr < POST_TRIGGER_RET_IDEAL_MIN:
            total += SCORE_WEIGHTS["post_trigger_return"] // 2    # +5 轻微回踩

    # 6. 回撤幅度（历史结构验证）
    dr = flags.get("drawdown_ratio") or 0.0
    if DRAWDOWN_MIN <= dr <= DRAWDOWN_SCORE_MAX:
        total += SCORE_WEIGHTS["drawdown_in_range"]               # +7 满分
    elif DRAWDOWN_SCORE_MAX < dr <= DRAWDOWN_MAX:
        total += SCORE_WEIGHTS["drawdown_in_range"] // 2          # +3 半分

    # 7. pre_peak_surge（bool，左侧大牛确认）
    if flags.get("pre_peak_surge", False):
        total += SCORE_WEIGHTS["pre_peak_surge"]

    # 8. 启动后大涨日密度（v1.5）：分档按当前 wm 比例缩放，满分档仍对齐 POST_TRIGGER_BIG_DAYS_FULL
    ptbd = flags.get("post_trigger_big_days")
    if ptbd is not None:
        wm = SCORE_WEIGHTS["post_trigger_momentum"]
        if ptbd >= POST_TRIGGER_BIG_DAYS_FULL:
            total += wm
        elif ptbd >= POST_TRIGGER_BIG_DAYS_MID1:
            total += (wm * 5 + 7) // 8
        elif ptbd >= POST_TRIGGER_BIG_DAYS_MID2:
            total += (wm * 3 + 7) // 8
        elif ptbd >= POST_TRIGGER_BIG_DAYS_LOW:
            total += max(1, (wm * 1 + 7) // 8) if wm >= 2 else 0

    return total


def calc_recent_big_days(
    df_daily: pd.DataFrame,
    as_of: pd.Timestamp,
    lookback_days: int = 91,
    thresh_pct: float = 9.5,
) -> int:
    """
    计算 as_of 日期前 lookback_days 天内的大涨日数量（近期活跃度指标）。

    依据 TOP20 vs BOTTOM20 实证：TOP20 近3月中位=0（极安静），BOT20 中位=0.5。
    近期安静是"蓄势待发"的正向信号。

    参数：
        df_daily: 含 date / chg 的日K DataFrame
        as_of: 截止日期
        lookback_days: 向前看的天数（默认91天=约3个月）
        thresh_pct: 大涨判断阈值（%）
    返回：
        int，大涨日计数
    """
    cutoff = as_of - pd.Timedelta(days=lookback_days)
    window = df_daily[(df_daily["date"] > cutoff) & (df_daily["date"] <= as_of)]
    if window.empty:
        return 0
    chg = window["close"].pct_change() * 100
    return int((chg >= thresh_pct).sum())


def calc_post_trigger_ret(
    df_daily: pd.DataFrame,
    trigger_date: pd.Timestamp,
    as_of: pd.Timestamp,
) -> float | None:
    """
    计算从 trigger_week 到 as_of 日期的累积价格涨幅（%）。

    依据 TOP20 vs BOTTOM20 实证：TOP20 中位=6.25%（适中），BOT20 中位=15%（已透支）。
    适中涨幅（-10%~30%）说明仍有上涨空间；涨太多后逆转风险高。

    参数：
        df_daily: 含 date / close 的日K DataFrame
        trigger_date: 触发日期（trigger_week 的某日）
        as_of: 截止日期
    返回：
        float，涨幅百分比；若数据不足则返回 None
    """
    before = df_daily[df_daily["date"] <= trigger_date].tail(1)
    after  = df_daily[df_daily["date"] <= as_of].tail(1)
    if before.empty or after.empty:
        return None
    p0 = float(before["close"].iloc[-1])
    p1 = float(after["close"].iloc[-1])
    if p0 <= 0:
        return None
    return round((p1 - p0) / p0 * 100, 2)


def calc_ret_n_trading_days_pct(
    df_daily: pd.DataFrame,
    as_of: pd.Timestamp,
    n: int = 5,
) -> float | None:
    """
    计算截至 as_of（含）最近一根日 K 相对往前第 n 个交易日的涨跌幅（%）。

    口径：用 n=5 近似「最近一周」的交易日涨幅（A 股通常一周 5 个交易日）。

    参数：
        df_daily: 含 date、close 列的日 K，按日期升序
        as_of: 截止日期（与筛选 as_of 对齐）
        n: 回溯交易日根数，默认 5
    返回：
        百分比涨幅；数据不足时 None
    """
    if df_daily.empty or n < 1:
        return None
    sub = df_daily[df_daily["date"] <= as_of].sort_values("date")
    if len(sub) < n + 1:
        return None
    c0 = float(sub["close"].iloc[-(n + 1)])
    c1 = float(sub["close"].iloc[-1])
    if c0 <= 0:
        return None
    return round((c1 - c0) / c0 * 100, 2)


def weekly_ma_alignment_score_from_series(last_wk: pd.Series) -> int:
    """
    用最新一根周 K 的均线结构给「多头排列」打分（0～4）。

    计分规则（每项满足 +1）：
        - MA5 > MA10
        - MA10 > MA20
        - MA20 > MA60（短中期均线抬头）
        - 收盘 ≥ MA60（站在周线 MA60 上方，弱势股不得分）

    参数：
        last_wk: 周 K 最后一行（含 close、ma5、ma10、ma20、ma60）
    返回：
        整数分数；关键均线缺失时该项不计分
    """
    score = 0
    try:
        close = float(last_wk["close"])
        pairs = [
            ("ma5", "ma10"),
            ("ma10", "ma20"),
            ("ma20", "ma60"),
        ]
        for a, b in pairs:
            va, vb = last_wk.get(a), last_wk.get(b)
            if pd.notna(va) and pd.notna(vb) and float(va) > float(vb):
                score += 1
        m60 = last_wk.get("ma60")
        if pd.notna(m60) and close >= float(m60):
            score += 1
    except (TypeError, ValueError, KeyError):
        return 0
    return score


def calc_pre_trigger_big_days(
    df_daily: pd.DataFrame,
    trigger_date: pd.Timestamp,
    lookback_weeks: int = 26,
    thresh_pct: float = 9.5,
) -> int:
    """
    计算启动周前 N 周内大涨日（涨幅 ≥ thresh_pct%）次数。

    用于 H4（启动前频繁脉冲 = 假启动信号）和 quiet_pre_trigger 加分评估。

    参数：
        df_daily:       日K DataFrame（须含 date、close 列，涨幅由此计算）
        trigger_date:   启动周日期
        lookback_weeks: 往前看几周
        thresh_pct:     大涨日阈值（%）

    返回：
        大涨日次数（int）
    """
    cutoff = trigger_date - pd.Timedelta(weeks=lookback_weeks)
    window = df_daily[
        (df_daily["date"] >= cutoff) & (df_daily["date"] < trigger_date)
    ].copy()
    if window.empty:
        return 0
    # 从收盘价序列直接计算涨幅，避免依赖库中可能有空值的字段
    window = window.reset_index(drop=True)
    window["chg"] = window["close"].pct_change() * 100
    return int((window["chg"] >= thresh_pct).sum())


def calc_post_trigger_big_days(
    df_daily: pd.DataFrame,
    trigger_date: pd.Timestamp,
    as_of: pd.Timestamp,
    thresh_pct: float = POST_TRIGGER_BIG_THRESH_PCT,
) -> int:
    """
    统计从启动锚点日（含）到 as-of（含）之间，日涨幅≥thresh_pct% 的交易日数量。

    口径与 calc_pre_trigger_big_days 一致（用 close 环比），但窗口在 trigger 之后：
    用于区分「启动后仍持续走强/换手」与「启动后沉寂」；与 phase_4.1 中 TOP vs BOT 的
    「启动后大涨日」差异相对应。注意：trigger_date 取周线启动周最后一个交易日，与 score 其他项对齐。

    参数：
        df_daily: 日K，须含 date、close
        trigger_date: 启动周索引日（与 detect_launch 返回的 launch_week 一致）
        as_of: 截面截止日（含）
        thresh_pct: 大涨阈值（%），默认与启动前/近3月一致，便于横向对比
    返回：
        int，大涨日计数；无重叠区间时返回 0
    """
    window = df_daily[
        (df_daily["date"] >= trigger_date) & (df_daily["date"] <= as_of)
    ].copy()
    if window.empty:
        return 0
    window = window.reset_index(drop=True)
    window["chg"] = window["close"].pct_change() * 100
    # 首行 pct_change 为 NaN，不计入大涨（避免把跨窗边界噪声算进去）
    return int((window["chg"] >= thresh_pct).sum())


def screen_single_instrument(
    instrument: str,
    name: str,
    df_daily: pd.DataFrame,
    trading_days: pd.Series,
    as_of: str,
) -> Optional[Dict]:
    """
    对单只股票运行完整筛选流程（v1.1），返回结构化候选结果或 None。

    参数：
        instrument:   股票代码
        name:         股票名称
        df_daily:     该股日 K DataFrame（已从数据库读取）
        trading_days: 全局交易日历 Series
        as_of:        截止日期字符串（用于 trigger_recency 计算）
    返回：
        dict（入选时）或 None（数据不足 / 不满足硬过滤条件时）
    硬过滤（H1-H4）任一不满足直接返回 None：
        H1: drawdown_ratio 30%~70%
        H2: bear_weeks 78~208 周
        H3: trigger_recency ≤ 365 天
        H4: 启动前26周大涨日 ≤ PRE_TRIGGER_BIG_DAYS_MAX（默认4次）
    """
    # ── 数据充足性检查 ──────────────────────────────────────────────────────
    if df_daily.empty or len(df_daily) < 250:
        return None

    # ── 日 K 聚合为周 K ──────────────────────────────────────────────────────
    df_week = aggregate_to_weekly(df_daily, trading_days)
    if len(df_week) < BEAR_WEEKS_MIN + BASE_MIN_WEEKS + 10:
        return None

    # ── 计算周线指标 ─────────────────────────────────────────────────────────
    df_week = calc_weekly_indicators(df_week)

    # ── 峰值定位 + pre_peak_surge ─────────────────────────────────────────────
    peak_iloc, peak_close, pre_peak_surge = find_historical_peak(df_week)
    if peak_iloc is None or peak_iloc >= len(df_week) - BEAR_WEEKS_MIN:
        return None
    peak_date = str(df_week.index[peak_iloc].date())

    # ── 熊市 + 筑底 ──────────────────────────────────────────────────────────
    bear_base = detect_bear_and_base(df_week, peak_iloc, peak_close)

    # ── H1：回撤幅度硬过滤（30%～70%）──────────────────────────────────────
    dr = bear_base.get("drawdown_ratio")
    if dr is None or not (DRAWDOWN_MIN <= dr <= DRAWDOWN_MAX):
        return None

    # ── H2：熊市时长硬过滤（78～208 周）────────────────────────────────────
    bw = bear_base.get("bear_weeks")
    if bw is None or not (BEAR_WEEKS_MIN <= bw <= BEAR_WEEKS_MAX):
        return None

    # ── 启动检测 ──────────────────────────────────────────────────────────────
    base_end_iloc = bear_base.get("base_end_iloc") or (
        bear_base.get("trough_iloc") or peak_iloc
    )
    vol_med_base = bear_base.get("vol_med_base") or 0.0
    launch = detect_launch(df_week, base_end_iloc, vol_med_base)

    # ── H5：必须有 trigger 信号（v1.2 新增硬过滤）────────────────────────────
    # 数据显示：无 trigger 的股票缺乏放量确认，保留会带来大量噪音
    as_of_ts = pd.Timestamp(as_of)
    trigger_ts = launch.get("launch_week")

    if trigger_ts is None:
        return None  # 无触发信号，排除

    # ── H3：trigger_recency 硬过滤（距截止日超过 540 天排除）────────────────
    trigger_recency_days = (as_of_ts - trigger_ts).days
    if trigger_recency_days > TRIGGER_RECENCY_HARD_CUTOFF:
        return None  # 超过 18 个月的陈旧信号排除

    # ── H4：启动前 26 周大涨日频率硬过滤 ────────────────────────────────────
    pre_trigger_big_days = calc_pre_trigger_big_days(df_daily, trigger_ts)
    if pre_trigger_big_days > PRE_TRIGGER_BIG_DAYS_MAX:
        return None  # 启动前频繁脉冲，假启动特征，排除
    quiet_pre_trigger = (pre_trigger_big_days <= PRE_TRIGGER_BIG_DAYS_QUIET)

    # ── 计算 v1.3 新特征（as-of 当日状态）────────────────────────────────────
    # 当前周线 close vs MA20 / MA120（蓄势状态判断的核心指标）
    last_wk = df_week.iloc[-1]
    last_close = float(last_wk["close"])
    last_ma20  = float(last_wk["ma20"])  if (not pd.isna(last_wk.get("ma20", float("nan")))) else None
    last_ma120 = float(last_wk["ma120"]) if (not pd.isna(last_wk.get("ma120", float("nan")))) else None
    current_close_vs_ma20w  = round(last_close / last_ma20,  4) if last_ma20  else None
    current_close_vs_ma120w = round(last_close / last_ma120, 4) if last_ma120 else None

    # 监控名单导出排序：MA60 周线、18 周均线≈90 个交易日位置、周线多头排列得分、近 5 交易日涨幅
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

    # H6：当前收盘跌破 MA120×0.90 → 长期趋势破坏，排除（已成为 BOTTOM 的基本格局）
    if current_close_vs_ma120w is not None and current_close_vs_ma120w < 0.90:
        return None

    # 近3个月大涨日（蓄势安静度指标）
    recent_3m_big_days = calc_recent_big_days(df_daily, as_of_ts, lookback_days=91)

    # 触发后到 as-of 的累积涨幅
    post_trigger_ret_pct = calc_post_trigger_ret(df_daily, trigger_ts, as_of_ts)

    # 启动后大涨日（v1.5）：与「启动前安静」独立，刻画触发后的持续强势程度
    post_trigger_big_days = calc_post_trigger_big_days(df_daily, trigger_ts, as_of_ts)

    # ── Score 计算（v1.5）────────────────────────────────────────────────────
    all_flags = {
        **bear_base,
        **launch,
        "pre_peak_surge":           pre_peak_surge,
        "quiet_pre_trigger":        quiet_pre_trigger,
        "pre_trigger_big_days":     pre_trigger_big_days,
        "trigger_recency_days":     trigger_recency_days,
        # v1.3 新特征
        "current_close_vs_ma20w":   current_close_vs_ma20w,
        "current_close_vs_ma120w":  current_close_vs_ma120w,
        "recent_3m_big_days":       recent_3m_big_days,
        "post_trigger_ret_pct":     post_trigger_ret_pct,
        "post_trigger_big_days":    post_trigger_big_days,
    }
    score = score_candidate(all_flags)

    if score == 0:
        return None

    # ── 格式化输出 ────────────────────────────────────────────────────────────
    trough_date = (
        str(df_week.index[bear_base["trough_iloc"]].date())
        if bear_base.get("trough_iloc") is not None else None
    )
    base_start_date = (
        str(df_week.index[bear_base["base_start_iloc"]].date())
        if bear_base.get("base_start_iloc") is not None else None
    )
    base_end_date = (
        str(df_week.index[bear_base["base_end_iloc"]].date())
        if bear_base.get("base_end_iloc") is not None else None
    )
    launch_week_str = (
        str(trigger_ts.date()) if trigger_ts is not None else None
    )

    return {
        "instrument":               instrument,
        "stock_name":               name,
        "score":                    score,
        # 峰值
        "T_peak":                   peak_date,
        "peak_close":               round(peak_close, 4),
        "pre_peak_surge":           pre_peak_surge,
        # 熊市
        "bear_weeks":               bear_base["bear_weeks"],
        "drawdown_ratio":           bear_base["drawdown_ratio"],
        "trough_date":              trough_date,
        "trough_close":             bear_base["trough_close"],
        # 条件 flags
        "drawdown_in_range":        bear_base["drawdown_in_range"],
        "bear_duration_ok":         bear_base["bear_duration_ok"],
        # 筑底
        "base_start":               base_start_date,
        "base_end":                 base_end_date,
        "base_weeks":               bear_base["base_weeks"],
        "vol_med_base":             bear_base["vol_med_base"],
        "base_formed":              bear_base["base_formed"],
        # 启动
        "trigger_week":             launch_week_str,
        "trigger_recency_days":     trigger_recency_days,
        "close_vs_ma120":           launch["close_vs_ma120"],
        "close_vs_ma250":           launch["close_vs_ma250"],
        "vr":                       launch["vr"],
        "launch_ma":                launch["launch_ma"],
        "launch_volume_vr":         launch["launch_volume_vr"],
        "launch_volume_base":       launch["launch_volume_base"],
        # v1.1
        "pre_trigger_big_days":     pre_trigger_big_days,
        "quiet_pre_trigger":        quiet_pre_trigger,
        # v1.3 新特征（as-of 当日状态，核心区分维度）
        "current_close_vs_ma20w":   current_close_vs_ma20w,
        "current_close_vs_ma120w":  current_close_vs_ma120w,
        "recent_3m_big_days":       recent_3m_big_days,
        "post_trigger_ret_pct":     post_trigger_ret_pct,
        "post_trigger_big_days":    post_trigger_big_days,
        # 监控导出排序辅助字段（不参与 score）
        "current_close_vs_ma60w":   current_close_vs_ma60w,
        "current_close_vs_ma18w":   current_close_vs_ma18w,
        "weekly_ma_alignment_score": weekly_ma_alignment_score,
        "ret_5d_pct":               ret_5d_pct,
    }


# ══════════════════════════════════════════════════════════════════════════════
# 六、全市场主流程
# ══════════════════════════════════════════════════════════════════════════════

def run_screener(
    as_of: str,
    instruments_filter: Optional[List[str]],
    output_path: Path,
    top_n: int = DEFAULT_TOP_N,
) -> None:
    """
    执行全市场（或子集）筛选，按 score 降序排列后取前 top_n 只输出。

    参数：
        as_of:              截至日期（'YYYY-MM-DD'）
        instruments_filter: 若非 None，仅处理指定股票代码（用于调试）
        output_path:        输出 JSON 文件的完整路径
        top_n:              最终输出前 N 名（默认 DEFAULT_TOP_N）；设为 0 则输出全部
    """
    logger.info(f"Phase 4 筛选启动 | as_of={as_of} | output={output_path}")

    # ── 加载全局交易日历 ─────────────────────────────────────────────────────
    logger.info("加载交易日历...")
    trading_days = load_trading_calendar(DATA_START, as_of)
    logger.info(f"交易日历加载完成：{len(trading_days)} 个交易日（{DATA_START} ~ {as_of}）")

    # ── 加载股票列表 ─────────────────────────────────────────────────────────
    df_stocks = load_all_instruments(as_of)
    if instruments_filter:
        df_stocks = df_stocks[df_stocks["instrument"].isin(instruments_filter)]
        logger.info(f"按 --instruments 过滤后剩余 {len(df_stocks)} 只股票")
    else:
        logger.info(f"全市场共 {len(df_stocks)} 只股票（已过滤 ST/退市）")

    candidates: List[Dict] = []
    stats = {"total": len(df_stocks), "skipped_no_data": 0, "screened": 0, "hit": 0}

    for idx, row in df_stocks.iterrows():
        instrument = row["instrument"]
        name       = row["name"]

        # ── 读取单股日 K ──────────────────────────────────────────────────────
        df_daily = load_kline_for_instrument(instrument, DATA_START, as_of)

        if df_daily.empty:
            stats["skipped_no_data"] += 1
            continue

        stats["screened"] += 1

        # ── 运行单股筛选 ──────────────────────────────────────────────────────
        try:
            result = screen_single_instrument(instrument, name, df_daily, trading_days, as_of)
        except Exception as e:
            logger.warning(f"[{instrument}] 筛选异常，已跳过：{e}")
            continue

        if result is not None:
            candidates.append(result)
            stats["hit"] += 1

        # 每 200 只打一次进度日志，避免日志过多
        if stats["screened"] % 200 == 0:
            logger.info(
                f"进度 {stats['screened']}/{stats['total']} | "
                f"命中 {stats['hit']} | 跳过（无数据）{stats['skipped_no_data']}"
            )

    # ── 按 score 降序排列，可选截取前 top_n ──────────────────────────────────
    # 主排序：score 降序；次要排序：trigger_recency_days 降序（同分时老信号优先）
    # 这确保同分段内 trigger 越老（越经过时间验证）的股票排在前面
    candidates.sort(
        key=lambda x: (x["score"], x.get("trigger_recency_days") or 0),
        reverse=True,
    )
    total_before_topn = len(candidates)
    if top_n > 0 and len(candidates) > top_n:
        candidates = candidates[:top_n]
    logger.info(
        f"score 排序完成 | 全部命中={total_before_topn} | "
        f"输出 top_n={top_n if top_n > 0 else '全部'} → {len(candidates)} 只"
    )

    # ── 写出 JSON ────────────────────────────────────────────────────────────
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "as_of":        as_of,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "stats":        {**stats, "total_hit_before_topn": total_before_topn, "output_topn": len(candidates)},
        "params": {
            "version":                    "v1.5",
            "DATA_START":                 DATA_START,
            "DRAWDOWN_MIN":               DRAWDOWN_MIN,
            "DRAWDOWN_MAX":               DRAWDOWN_MAX,
            "BEAR_WEEKS_MIN":             BEAR_WEEKS_MIN,
            "BEAR_WEEKS_MAX":             BEAR_WEEKS_MAX,
            "BASE_MIN_WEEKS":             BASE_MIN_WEEKS,
            "VR_THRESH":                  VR_THRESH,
            "VOL_K_BASE_MULTIPLIER":      VOL_K_BASE_MULTIPLIER,
            "TRIGGER_RECENCY_HARD_CUTOFF": TRIGGER_RECENCY_HARD_CUTOFF,
            "TRIGGER_RECENCY_MATURE_MIN":  TRIGGER_MATURE_MIN,
            "TRIGGER_RECENCY_MATURE_BONUS": TRIGGER_MATURE_BONUS,
            "PRE_TRIGGER_BIG_DAYS_MAX":   PRE_TRIGGER_BIG_DAYS_MAX,
            "PRE_PEAK_SURGE_K":           PRE_PEAK_SURGE_K,
            "PRE_PEAK_SURGE_N":           PRE_PEAK_SURGE_N,
            "POST_TRIGGER_BIG_THRESH_PCT": POST_TRIGGER_BIG_THRESH_PCT,
            "POST_TRIGGER_BIG_DAYS_FULL":  POST_TRIGGER_BIG_DAYS_FULL,
            "POST_TRIGGER_BIG_DAYS_MID1":  POST_TRIGGER_BIG_DAYS_MID1,
            "POST_TRIGGER_BIG_DAYS_MID2":  POST_TRIGGER_BIG_DAYS_MID2,
            "POST_TRIGGER_BIG_DAYS_LOW":   POST_TRIGGER_BIG_DAYS_LOW,
            "SCORE_WEIGHTS":              SCORE_WEIGHTS,
            "top_n":                      top_n,
        },
        "candidates":   candidates,
    }
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)

    logger.info(
        f"筛选完成 | 命中 {stats['hit']} 只候选股 | "
        f"结果已写入：{output_path}"
    )
    logger.info(f"统计：总计={stats['total']} 已扫描={stats['screened']} "
                f"无数据跳过={stats['skipped_no_data']} 命中={stats['hit']}")


# ══════════════════════════════════════════════════════════════════════════════
# 七、命令行入口
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    """脚本入口，解析 CLI 参数后调用 run_screener。"""
    parser = argparse.ArgumentParser(
        description="Phase 4 v1.5: 周线级别「大牛顶—长熊—筑底—启动」全市场筛选（含启动后大涨日动量）"
    )
    parser.add_argument(
        "--as-of",
        type=str,
        default=date.today().strftime("%Y-%m-%d"),
        help="截至哪天的数据（YYYY-MM-DD），默认为今天",
    )
    parser.add_argument(
        "--instruments",
        nargs="+",
        help="仅筛选指定股票代码（空格分隔），不填则全市场扫描",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="输出 JSON 文件路径；不填则自动生成",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=DEFAULT_TOP_N,
        help=f"按 score 排序后只输出前 N 只（默认 {DEFAULT_TOP_N}，设为 0 输出全部）",
    )
    args = parser.parse_args()

    as_of = args.as_of
    output_path = (
        Path(args.output)
        if args.output
        else OUTPUT_DIR / f"phase4_candidates_{as_of.replace('-', '')}.json"
    )

    run_screener(
        as_of=as_of,
        instruments_filter=args.instruments,
        output_path=output_path,
        top_n=args.top_n,
    )


if __name__ == "__main__":
    main()
