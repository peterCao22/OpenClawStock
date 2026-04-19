"""
量化选股（Phase 2）：基于本地数据库的多层筛选 + 月线/周线趋势加权流水线。

功能概述：
    1. 从 ``trading_calendar`` 取 T 日及以前约 850 个交易日（覆盖动态四窗口 / 形态识别 /
       微观分 / 日周线趋势所需的 ~3.5 年），确定日线拉取区间；月线层数据**改为从派生月表
       ``kline_qfq_monthly`` 直接读取**（参见 ``scripts/build_kline_qfq_monthly.py``），
       避免每次扫全市场都拉 9 年日线（923w 行 → 350w 行，主取数提速约 8x）；
    2. 从 ``kline_qfq`` 联表 ``stock_list`` 读取前复权日线，排除名称含 ST/退 的品种；
    3. 对每只股票按「动态四窗口」切分（约每月一段，从 T 向前递归），计算每窗高低点、
       方向、振幅、涨跌停计数等特征；
    4. ``identify_pattern`` 识别 N / W / H / V 四类技术形态（创业板/科创板与主板阈值不同）；
    5. ``check_risk`` 过滤短期过热、弱势反弹等高风险情形；
    6. ``score_stock_micro`` 在最新窗口起点之后做 A（拉升）→ B（回踩）→ C（再起）微观打分；
    7. ``check_daily_weekly_trend`` 校验日线/周线趋势是否健康（硬过滤）；
    8. ``evaluate_monthly_rules`` 综合月线层规则（"长期均线"统一为 **月线 60 月均线 / 5 年线**，记为 ``ma60_m``）：
        - 硬过滤：3 年峰值（方案 Y，A/B 任一通过）、近 6 月未站 ma60_m、突破后久未再突破、
                  近 3 月未站回前期高点；
        - 加分项：
            * 1.a 月线连涨（硬版连续 ≥3 月 +15 / 软版 6 月里 ≥4 月 +10，取大）；
            * 1.b 有效突破 ma60_m 的梯度加分（双重判定：月收 ≥ ma60_m 且 月最高 ≥ 3 年前最高 × 0.8/1.0）；
            * 1.d 底部量能放大（阈值 1.3）；
            * 1.e 长期稳健站上（最近 6 月 ≥5 月月收 ≥ ma60_m）；
            * 1.f 6 月主升浪（涨幅 ∈ [30%, 120%]）；
            * 1.g 有效突破前 2~3 月连涨；
            * 附加：连续 2 月月收 ≥ ma60_m 且 2 月累计涨幅 ≤ 40%。
    9. 综合 ``final_score = total_score + monthly_bonus``，降序取 Top-K，写入
       ``results/top_{K}_stocks_{T}.json``。

数据口径：
    - 日期为 ``YYYY-MM-DD`` 字符串与交易日对齐；K 线为前复权（表 ``kline_qfq``）。
    - **「长期均线」统一指月线 60 月均线（5 年线，``ma60_m``）**；之前用过的"日线 MA120"已弃用，
      因为软件月线视图下的 MA120 标签实际是"120 月线（10 年线）"，库内数据不足以稳定计算。
    - 月线由派生月表 ``kline_qfq_monthly`` 提供已完整月数据；T 所在月作为"未走完的当月"
      按 T 日截止从日线现场聚合，O=月首开、H=月内最高、L=月内最低、C=月末收、
      V=月内累计成交量；``ma60_m`` 在月表里预存，T 当月的 ``ma60_m`` 现场算（前 59 月 close
      + T 当月 close）/ 60。月表与日线一致性由 ``update_kline_qfq.py`` 增量保证。
    - 形态与风控中的百分比阈值为经验规则，修改会影响召回率与误报率。

运行方式（建议在仓库根目录执行，以便 ``logs/``、``results/`` 相对路径一致）::

    python scripts/quant_picker.py
    python scripts/quant_picker.py --t_date 2025-10-31 --top_k 30
    python scripts/quant_picker.py --debug_stock 002082.SZ --t_date 2025-10-31

注意：
    - 依赖 ``scripts.db_session`` 中的数据库连接；无数据或交易日不足时会记录错误并提前返回。
    - ``--debug_stock`` 仅分析单票并打出窗口/形态/月线/趋势/评分日志，便于对照 K 线排查规则。
    - 日线主取数缩回 ~850 交易日（约 3.5 年）；月线层数据走 ``kline_qfq_monthly`` 派生表，
      首次使用前需 ``python scripts/build_kline_qfq_monthly.py --rebuild`` 全量回填。
"""
import logging
import os
import sys
import argparse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.db_session import engine

# 日志落盘到 logs/，同时输出到控制台，便于长时间批量选股时留痕
if not os.path.exists('logs'):
    os.makedirs('logs')
    
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/quant_picker_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def get_trading_days(t_date=None, count=850):
    """
    查询 T 日（含）以前的连续交易日列表，从早到晚排序。

    参数:
        t_date: 锚定交易日，格式 ``YYYY-MM-%d``；默认当前机器日期（未必是交易日）。
        count: 向前取多少个交易日，默认 ``850``（约 3.5 年）。
            主流程仅需要满足"动态四窗口 + 形态识别 + 微观分 + 日/周线趋势"的回看跨度；
            **月线层数据已迁移到派生月表 ``kline_qfq_monthly``**，不再依赖该窗口长度，
            因此较旧版本的 2300 大幅缩减，主取数 IO 显著下降。

    返回:
        ``list[str]``，元素为 ``YYYY-MM-%d``，按时间升序。

    注意:
        若库中交易日记录不足，返回列表可能短于 ``count``；上层需判断长度。
    """
    if not t_date:
        t_date = datetime.now().strftime('%Y-%m-%d')
        
    query = f"SELECT trade_date FROM trading_calendar WHERE is_trading_day = true AND trade_date <= '{t_date}' "
    query += f"ORDER BY trade_date DESC LIMIT {count}"
    
    df = pd.read_sql(query, engine)
    dates = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d').tolist()
    dates.reverse()  # SQL 为 DESC，反转为从早到晚，便于后续按时间切片
    return dates

def get_kline_data(start_date, end_date):
    """
    读取指定日期区间内的前复权日线，并剔除名称中含 ST、退 的股票。

    参数:
        start_date: 区间起始日 ``YYYY-MM-%d``（含）。
        end_date: 区间结束日 ``YYYY-MM-%d``（含）。

    返回:
        ``pandas.DataFrame``，含 ``kline_qfq`` 字段及 ``stock_name``；无数据时为空表。

    注意:
        停牌日是否出现在结果中取决于库表记录；名称规则过滤不等于风险评级。
    """
    query = f"""
        SELECT k.*, s.name as stock_name
        FROM kline_qfq k
        JOIN stock_list s ON k.instrument = s.instrument
        WHERE k.date >= '{start_date}' AND k.date <= '{end_date}'
        AND s.name NOT LIKE '%%ST%%'
        AND s.name NOT LIKE '%%退%%'
        ORDER BY k.instrument, k.date ASC
    """
    df = pd.read_sql(query, engine)
    return df

def get_stock_type_thresholds(instrument):
    """
    按证券代码前缀区分板块，返回形态识别与风控用的经验阈值（百分比口径）。

    参数:
        instrument: 证券代码，如 ``300001.SZ``、``688001.SH`` 等。

    返回:
        ``dict``，含 ``limit_up`` / ``limit_down`` / ``surge_trigger`` / ``flag_pole`` /
        ``overheat`` / ``is_cyb``（创业板/科创板为 True）。

    注意:
        主板与双创的涨停幅度不同，这里用固定阈值近似区分波动特征，非交易所规则字面量。
    """
    is_cyb = instrument.startswith('300') or instrument.startswith('301') or instrument.startswith('688')
    if is_cyb:
        return {
            'limit_up': 12.0,  # 大涨阈值
            'limit_down': -12.0, # 大跌阈值
            'surge_trigger': 25.0, # 前期拉升阈值
            'flag_pole': 35.0, # 旗杆阈值
            'overheat': 60.0, # 短期过热阈值
            'is_cyb': True
        }
    else:
        return {
            'limit_up': 7.0,
            'limit_down': -7.0,
            'surge_trigger': 15.0,
            'flag_pole': 20.0,
            'overheat': 60.0,
            'is_cyb': False
        }

def calculate_window_features(df_window, instrument):
    """
    对单个时间窗口内的 K 线计算高低点、摆动方向、振幅、涨跌统计等特征。

    参数:
        df_window: 单票、按日期升序排列的一段 K 线（需含 open/high/low/close/volume/date）。
        instrument: 用于 ``get_stock_type_thresholds`` 判断板块阈值。

    返回:
        特征 ``dict``；若 ``df_window`` 为空则返回 ``None``。

    注意:
        方向由「先见低点还是先见高点」判定；首日涨跌幅用 ``pct_change`` 的 NaN 填 0，
        可能与真实昨收口径略有偏差。
    """
    if df_window.empty:
        return None
        
    high = df_window['high'].max()
    low = df_window['low'].min()
    
    idx_high = df_window['high'].idxmax()
    idx_low = df_window['low'].idxmin()
    
    date_h = df_window.loc[idx_high, 'date']
    date_l = df_window.loc[idx_low, 'date']
    
    start_date = df_window['date'].iloc[0]
    end_date = df_window['date'].iloc[-1]
    
    # 先见低点后见高点视为上升摆动，反之视为下降摆动（用于振幅与形态语义）
    direction = 'UP' if date_l < date_h else 'DOWN'
    
    if direction == 'UP':
        # Low -> High
        if low > 0:
            amplitude = (high - low) / low * 100
        else:
            amplitude = 0
    else:
        # High -> Low
        if high > 0:
            amplitude = (low - high) / high * 100
        else:
            amplitude = 0
            
    # 窗口内逐日涨跌幅：首行可能缺昨收，用 0 填充避免统计涨停天数时被 NaN 干扰
    df_w = df_window.copy()
    df_w['pct_change'] = df_w['close'].pct_change() * 100
    df_w['pct_change'] = df_w['pct_change'].fillna(0)
    
    thresh = get_stock_type_thresholds(instrument)
    
    limit_up_count = len(df_w[df_w['pct_change'] >= thresh['limit_up']])
    limit_down_count = len(df_w[df_w['pct_change'] <= thresh['limit_down']])
    
    # Positive/Negative days
    up_days = len(df_w[df_w['close'] > df_w['open']])
    down_days = len(df_w[df_w['close'] < df_w['open']])
    
    # 窗口首尾收盘真实涨跌：反映整段净涨跌，与「摆动振幅」含义不同
    close_start = df_w['close'].iloc[0]
    close_end = df_w['close'].iloc[-1]
    if close_start > 0:
        real_change = (close_end - close_start) / close_start * 100
    else:
        real_change = 0
    
    return {
        'start_date': start_date,
        'end_date': end_date,
        'high': high,
        'low': low,
        'date_h': date_h,
        'date_l': date_l,
        'direction': direction,
        'amplitude': amplitude,
        'real_change': real_change,
        'limit_up_count': limit_up_count,
        'limit_down_count': limit_down_count,
        'up_days': up_days,
        'down_days': down_days,
        'close_end': df_w['close'].iloc[-1],
        'close_start': df_w['close'].iloc[0],
        'volume_avg': df_w['volume'].mean()
    }

def segment_windows(df, t_date):
    """
    从 T 日向前递归切约 4 段「动态窗口」（每段约 23 个交易日），每段调用 ``calculate_window_features``。

    参数:
        df: 单票 K 线，需含 ``date`` 列（可与 ``t_date`` 比较的日期格式）。
        t_date: 当前锚定截止日（通常取样本区间最后交易日），窗口从该日向前生长。

    返回:
        ``list[dict]``，最多 4 个元素：``windows[0]`` 为最近一段（W1），``windows[3]`` 为最早（W4）。
        若数据不足以切段或陷入死循环保护，可能少于 4 个。

    注意:
        下一段的截止日取当前窗内「高点日、低点日」中较早者，以衔接上一波摆动；
        若与当前截止日相同则回退到窗起点日前一日，避免无限循环。
    """
    windows = []
    current_end_date = t_date
    
    # Ensure df is sorted by date
    df = df.sort_values('date').reset_index(drop=True)
    
    for i in range(4):
        # 在当前截止日及之前找最后一根 K 线索引，作为本窗口右端
        end_indices = df[df['date'] <= current_end_date].index
        if len(end_indices) == 0:
            break
        end_idx = end_indices[-1]
        
        # 每窗约一个月（23 根）：与形态规则里「中期摆动」尺度一致
        start_idx = max(0, end_idx - 22)
        
        if start_idx >= end_idx:
            break
            
        df_slice = df.iloc[start_idx : end_idx + 1]
        
        # Calculate features
        feat = calculate_window_features(df_slice, df['instrument'].iloc[0])
        if not feat:
            break
            
        windows.append(feat)
        
        # Next end date = min(date_h, date_l)
        next_end = min(feat['date_h'], feat['date_l'])
        
        # 防止 next_end 与当前截止日相同导致死循环：强制把截止日上移到本窗左端
        if next_end == current_end_date:
            if start_idx > 0:
                current_end_date = df.iloc[start_idx]['date']
                # 单日窗仍无法推进则终止
                if current_end_date == next_end:
                    break
            else:
                break
        else:
            current_end_date = next_end
            
    return windows

def identify_pattern(windows, instrument):
    """
    在已算好的四窗口特征上识别 N / W / H / V 四类形态之一（互斥优先级：N → H → V → W）。

    参数:
        windows: ``segment_windows`` 的返回值，须含 4 段；``windows[0]`` 为最近窗 W1。
        instrument: 证券代码，用于板块阈值。

    返回:
        三元组 ``(pattern_type, description, pattern_roles)``：
        - 命中时 ``pattern_type`` 为 ``N`` / ``W`` / ``H`` / ``V`` 之一；
        - 未命中时 ``pattern_type`` 与 ``description``、``pattern_roles`` 可能为 ``None``；
        - ``pattern_roles`` 为各窗口角色说明的字典（键为 ``W1``～``W4``）。

    注意:
        规则内含大量经验阈值（如回撤比例、收复颈线），调参会显著改变选股集合。
    """
    if len(windows) < 4:
        return None, "数据不足：需要完整 4 个动态窗口", None
        
    w1, w2, w3, w4 = windows[0], windows[1], windows[2], windows[3]
    thresh = get_stock_type_thresholds(instrument)
    
    # --- N 型：前段拉升 → 洗盘 → W1 再起（需收复前高附近且实体强度达标）---
    w4_surge = w4['direction'] == 'UP' and w4['amplitude'] > thresh['surge_trigger']
    w3_surge = w3['direction'] == 'UP' and w3['amplitude'] > thresh['surge_trigger']
    
    if (w4_surge or w3_surge):
        # W1 需有真实涨幅且收盘接近当日高点，避免长上影假突破
        if w1['direction'] == 'UP' and w1['close_end'] > w2['low']:
            prev_high = max(w4['high'], w3['high'])
            # 收复前高区约 85% 以上，才认为第二波有力
            if w1['high'] >= prev_high * 0.85:
                if w1['real_change'] > 5.0 and w1['close_end'] > w1['high'] * 0.8:
                    roles = {
                        'W4': 'Surge (Start)' if w4_surge else 'Context',
                        'W3': 'Surge (Start)' if w3_surge else 'Context',
                        'W2': 'Wash (Pullback)',
                        'W1': 'Surge (Second Wave)'
                    }
                    return "N", "N型: 强势整理后再起", roles

    # --- H 型：旗杆大拉升后高位平台，低点显著高于旗杆底 ---
    w4_flag = w4['direction'] == 'UP' and w4['amplitude'] > thresh['flag_pole']
    w3_flag = w3['direction'] == 'UP' and w3['amplitude'] > thresh['flag_pole']
    
    if (w4_flag or w3_flag):
        flag_low = w4['low'] if w4_flag else w3['low']  # 旗杆起点低价
        flag_high = w4['high'] if w4_flag else w3['high']
        
        # 平台低点明显高于旗杆底，表示涨幅未被完全回吐
        if w2['low'] > flag_low * 1.1 and w1['low'] > flag_low * 1.1:
            # 收盘价仍维持在旗杆高位的比例之上，视为强势整理
            if w1['close_end'] > flag_high * 0.8: 
                # W1 若深跌则更像破位而非横盘整理，直接否决 H 型
                if w1['direction'] == 'DOWN' and w1['amplitude'] < -20.0:
                    return None, None, None
                
                roles = {
                     'W4': 'Flag Pole' if w4_flag else 'Context',
                     'W3': 'Flag Pole' if w3_flag else 'Context',
                     'W2': 'Platform',
                     'W1': 'Breakout/Platform'
                }
                return "H", "H型: 高位横盘强者恒强", roles

    # --- V 型：W2 深跌后 W1 强反转（用振幅/收复比例/真实涨幅过滤 L 形阴跌）---
    if w2['direction'] == 'DOWN' and w2['amplitude'] < -15.0:
        if w1['direction'] == 'UP':
            if w1['amplitude'] > 20.0 and w1['amplitude'] > abs(w2['amplitude']) * 0.65 and w1['real_change'] > 10.0:
                roles = {
                    'W2': 'Crash',
                    'W1': 'Reversal',
                    'W3': 'Context',
                    'W4': 'Context'
                }
                return "V", "V底: 超跌反转", roles
            # 更陡的 V：略放宽收复比例，仍要求真实涨幅
            if w1['amplitude'] > 25.0 and w1['amplitude'] > abs(w2['amplitude']) * 0.6 and w1['real_change'] > 10.0:
                roles = {
                    'W2': 'Crash',
                    'W1': 'Reversal',
                    'W3': 'Context',
                    'W4': 'Context'
                }
                return "V", "V底: 强力反弹", roles

    # --- W 型：双底，两低点接近且 W1 向上确认（颈线收复）---
    # 情形一：W3 与 W1 低点相差在 10% 内，视为左右底接近
    if abs(w3['low'] - w1['low']) / w3['low'] < 0.1:
        neckline_high = w2['high']
        drop_height = neckline_high - w1['low']
        recovery_target = w1['low'] + drop_height * 0.5
        
        if w1['direction'] == 'UP' and w1['amplitude'] > 5.0 and w1['high'] > recovery_target and w1['real_change'] > 3.0:
            roles = {
                'W3': 'Left Bottom',
                'W2': 'Mid-Rebound',
                'W1': 'Right Bottom/Breakout',
                'W4': 'Context'
            }
            return "W", "W底: 双底结构", roles
            
    # 情形二：W2 与 W1 为双底，颈线用 W2 高点近似
    if abs(w2['low'] - w1['low']) / w2['low'] < 0.1:
        neckline_high = w2['high'] 
        drop_height = neckline_high - w1['low']
        recovery_target = w1['low'] + drop_height * 0.5
        
        if w1['direction'] == 'UP' and w1['amplitude'] > 5.0 and w1['high'] > recovery_target and w1['real_change'] > 3.0:
            roles = {
                'W2': 'Left Bottom',
                'W1': 'Right Bottom/Breakout',
                'W3': 'Context',
                'W4': 'Context'
            }
            return "W", "W底: 双底结构", roles

    return None, None, None

def check_risk(windows, instrument):
    """
    对已通过形态识别的股票做简单风控：过热拉升、下跌中继弱反弹等标为高风险。

    参数:
        windows: 与 ``identify_pattern`` 相同；为空则视为高风险。
        instrument: 证券代码，用于过热阈值等。

    返回:
        ``True`` 表示建议剔除（高风险），``False`` 表示通过。

    注意:
        与合规风控不同，此处仅为策略层过滤，不排除个股黑天鹅。
    """
    if not windows:
        return True
        
    w1 = windows[0]
    thresh = get_stock_type_thresholds(instrument)
    
    # 1. 短期过热：连续涨停或单窗振幅过大，降低追高风险
    if w1['direction'] == 'UP':
        # 单窗振幅超过 100% 直接否决，避免极端炒作段
        if w1['amplitude'] > 100.0:
            return True
            
        if w1['amplitude'] > thresh['overheat']:
            # Check for continuous acceleration (limit ups)
            if w1['limit_up_count'] >= 4: 
                return True
            
    # 2. 重心下移：W1 高点远低于 W4 且 W1 摆动弱，偏下跌中继里的弱反弹
    if len(windows) >= 4:
        w4 = windows[3]
        if w1['high'] < w4['high'] * 0.7: # Still far below previous high
            # And W1 is weak
            if w1['amplitude'] < 10.0:
                return True
    
    return False

def score_stock_micro(df, window_start_date):
    """
    在 ``window_start_date``（含）之后的片段上，用 A→B→C 结构做微观打分。

    参数:
        df: 单票完整区间 K 线（需含 instrument、date、OHLCV；若有 stock_name 会写入明细）。
        window_start_date: 与 W1 起点对齐的日期，只对该日及之后子序列搜 A/B/C。

    返回:
        ``(best_score, best_detail)``。未找到有效结构时 ``best_score`` 为 0，``best_detail`` 为 ``{}``；
        命中时 ``best_detail`` 含拉升日、回踩日、反弹日及各子分项得分。

    注意:
        A 日要求当日高点相对昨收涨幅达板块「涨停附近」阈值且放量；会结合前 5 日累计涨幅做过热惩罚。
        嵌套循环复杂度较高，仅在全市场扫描时由上层控制数据跨度。
    """
    df = df.reset_index(drop=True)
    if len(df) < 10:
        return 0, {}
        
    df['ma25'] = df['close'].rolling(window=25).mean()
    df['vol_ma5'] = df['volume'].rolling(window=5).mean()
    
    df_window = df[df['date'] >= window_start_date].reset_index(drop=True)
    if len(df_window) < 5:
        return 0, {}
        
    instrument = df_window['instrument'].iloc[0]
    thresh = get_stock_type_thresholds(instrument)
    # 与旧版一致：双创约 12%、主板约 7% 作为「强势拉升日」判别线
    surge_threshold = thresh['limit_up']
    
    best_score = 0
    best_detail = {}
    
    surge_candidates = []
    for i in range(len(df_window)):
        row = df_window.iloc[i]
        if i > 0:
            pre_close = df_window.iloc[i-1]['close']
            high_pct = (row['high'] / pre_close - 1) * 100
        else:
            orig_idx = df[df['date'] == row['date']].index[0]
            if orig_idx > 0:
                pre_close = df.iloc[orig_idx-1]['close']
                high_pct = (row['high'] / pre_close - 1) * 100
            else:
                continue

        # 放量确认：当日量至少为近 5 日均量 1.5 倍，减少无量假突破
        if high_pct >= surge_threshold and row['volume'] >= row['vol_ma5'] * 1.5:
            is_continuous_surge = False
            if i > 0:
                prev_row = df_window.iloc[i-1]
                orig_prev_idx = df[df['date'] == prev_row['date']].index[0]
                if orig_prev_idx > 0:
                    prev_pre_close = df.iloc[orig_prev_idx-1]['close']
                    prev_high_pct = (prev_row['high'] / prev_pre_close - 1) * 100
                    if prev_high_pct >= surge_threshold:
                        is_continuous_surge = True
            
            surge_candidates.append({
                'window_idx': i,
                'date': row['date'],
                'close': row['close'],
                'is_continuous': is_continuous_surge
            })
            
    if not surge_candidates:
        return 0, {}

    for surge in surge_candidates:
        idx_A = surge['window_idx']
        
        # A 点前 5 日累计涨幅过大则跳过，避免已透支的接力
        orig_idx_A = df[df['date'] == surge['date']].index[0]
        idx_A_prev = max(0, orig_idx_A - 1)
        start_idx = max(0, idx_A_prev - 5)
        
        if idx_A_prev > start_idx:
            p_close = df.iloc[idx_A_prev]['close']
            s_close = df.iloc[start_idx]['close']
            cum_return = (p_close / s_close - 1) * 100 if s_close > 0 else 0
        else:
            cum_return = 0
            
        if cum_return > thresh['overheat']: # Use overheat threshold
            continue 
            
        for idx_B in range(idx_A + 1, len(df_window)):
            row_B = df_window.iloc[idx_B]
            is_valid_pullback = False
            pullback_score = 0
            
            consecutive_yin = 0
            for j in range(idx_B, idx_A, -1):
                if df_window.iloc[j]['close'] < df_window.iloc[j]['open']:
                    consecutive_yin += 1
                else:
                    break
            if consecutive_yin >= 1:
                is_valid_pullback = True
                pullback_score += consecutive_yin * 2
                
            pre_B_close = df_window.iloc[idx_B-1]['close']
            pct_B = (row_B['close'] / pre_B_close - 1) * 100
            if pct_B <= -5.0:
                is_valid_pullback = True
                pullback_score += 5
                
            if row_B['close'] < row_B['ma25']:
                is_valid_pullback = True
                pullback_score += 5
                
            # 连续涨停后急跌洗盘（Golden Pit）：仍收在拉升日开盘价之上则加分
            if surge['is_continuous']:
                has_sharp_drop = False
                # Check drop from A to B
                for k in range(idx_A + 1, idx_B + 1):
                    prev_c = df_window.iloc[k-1]['close']
                    curr_c = df_window.iloc[k]['close']
                    if prev_c > 0:
                        day_pct = (curr_c - prev_c) / prev_c * 100
                        if day_pct < -7.0:
                            has_sharp_drop = True
                            break
                
                if has_sharp_drop:
                    surge_open = df_window.iloc[idx_A]['open']
                    if row_B['close'] > surge_open:
                        pullback_score += 15

            if not is_valid_pullback:
                continue
                
            for idx_C in range(idx_B + 1, min(idx_B + 4, len(df_window))):
                row_C = df_window.iloc[idx_C]
                pre_C_close = df_window.iloc[idx_C-1]['close']
                pct_C = (row_C['close'] / pre_C_close - 1) * 100
                
                is_valid_rebound = False
                rebound_score = 0
                
                limit_up_pct = 19.5 if thresh['is_cyb'] else 9.5
                if pct_C >= limit_up_pct:
                    is_valid_rebound = True
                    rebound_score += 20
                    
                if row_C['open'] > pre_C_close * 1.02 and row_C['low'] > pre_C_close:
                    is_valid_rebound = True
                    rebound_score += 15
                    
                if pct_C >= 5.0:
                    idx_C_minus_3 = max(idx_A, idx_C - 3)
                    price_C_minus_3 = df_window.iloc[idx_C_minus_3]['close']
                    if price_C_minus_3 > pre_C_close:
                        drop_amount = price_C_minus_3 - pre_C_close
                        recover_amount = row_C['close'] - pre_C_close
                        if recover_amount >= drop_amount * 0.5:
                            is_valid_rebound = True
                            rebound_score += 10
                            
                if not is_valid_rebound:
                    continue
                    
                max_rebound_idx = min(idx_C + 3, len(df_window) - 1)
                rebound_peak_price = df_window.iloc[idx_C:max_rebound_idx+1]['high'].max()
                
                surge_close = surge['close']
                if rebound_peak_price >= surge_close:
                    rebound_score += 20
                elif rebound_peak_price >= surge_close * 0.95:
                    rebound_score += 10
                elif rebound_peak_price < surge_close * 0.8:
                    rebound_score -= 20
                
                if rebound_score < 0:
                    continue
                    
                score_surge = 10 + (5 if surge['is_continuous'] else 0)
                score_risk = 20 * (1 - max(0, cum_return) / thresh['overheat'])
                
                total_score = score_surge + pullback_score + rebound_score + score_risk
                
                if total_score > best_score:
                    best_score = total_score
                    best_detail = {
                        'instrument': instrument,
                        'name': df_window['stock_name'].iloc[0] if 'stock_name' in df_window.columns else "",
                        'total_score': round(total_score, 2),
                        'surge_date': surge['date'],
                        'bottom_date': row_B['date'],
                        'rebound_date': row_C['date'],
                        'cum_return_5d': round(cum_return, 2),
                        'surge_score': score_surge,
                        'pullback_score': pullback_score,
                        'rebound_score': rebound_score,
                        'risk_score': round(score_risk, 2)
                    }

    return best_score, best_detail


# =============================================================================
# 月线 / 周线 / 趋势 规则层（在形态、风控、微观分通过后执行）
# =============================================================================

def aggregate_monthly_klines(df_daily):
    """
    将单票日线聚合为月线（最后一根月线允许"未走完"，其口径以日线最后交易日为准）。

    参数:
        df_daily: 单票日线 DataFrame，需含 ``date``（``YYYY-MM-DD`` 字符串或可解析为日期）、
            ``open`` / ``high`` / ``low`` / ``close`` / ``volume``，按日期任意顺序均可。

    返回:
        ``pandas.DataFrame``，列为 ``month_key``（``YYYY-MM``）、``open`` / ``high`` / ``low`` /
        ``close`` / ``volume``、``last_trade_date``（该月最后一个**实际**交易日，``YYYY-MM-DD``）。
        按时间升序排序。

    注意:
        - 当月若未走完（如 T=2026-04-17），则该月聚合自然只覆盖 4 月已发生的交易日；
          ``last_trade_date`` 即为 T 日，确保后续 MA120 取值与"当月截止"一致。
        - 不做空值/异常 K 检查，依赖上游 ``get_kline_data`` 已过滤 ST/退。
    """
    if df_daily is None or df_daily.empty:
        return pd.DataFrame(columns=['month_key', 'open', 'high', 'low', 'close', 'volume', 'last_trade_date'])

    df = df_daily.copy()
    df['_dt'] = pd.to_datetime(df['date'])
    df = df.sort_values('_dt').reset_index(drop=True)
    df['month_key'] = df['_dt'].dt.strftime('%Y-%m')

    rows = []
    for mk, g in df.groupby('month_key', sort=True):
        rows.append({
            'month_key': mk,
            # 月开 = 月内首个交易日 open；月收 = 月内最后交易日 close
            'open': float(g['open'].iloc[0]),
            'high': float(g['high'].max()),
            'low': float(g['low'].min()),
            'close': float(g['close'].iloc[-1]),
            'volume': float(g['volume'].sum()),
            'last_trade_date': g['_dt'].iloc[-1].strftime('%Y-%m-%d'),
        })
    return pd.DataFrame(rows)


def attach_ma60m_to_monthly(df_monthly):
    """
    在月线表上滚动算「过去 60 个月收盘均值」（即月线 60 月均线，俗称"5 年线"）。

    参数:
        df_monthly: ``aggregate_monthly_klines`` 的输出。

    返回:
        新增 ``ma60_m`` 列的 ``df_monthly`` 副本；月线不足 60 根的部分该列为 ``NaN``，
        上层规则会按"保守不通过/不加分"处理。

    注意:
        - 这里用的是 **月线** 上的 60 期均值，不是"日线 60 日均线"，与行情软件月线视图下的
          长期均线（如 5 年线）语义对齐；
        - 之前版本曾用"日线 MA120 在月末取值"近似"半年线"，但与用户在软件月线视图下看到的
          MA120（实为月线 120 月线 / 10 年线）语义不一致；库内日线历史不足 10 年，故采用 5 年线；
        - 5 年线足够代表"长期均价/长期成本"，对中国 A 股近 10 年内风格切换更稳健，避免远古
          泡沫（如 2015 顶部）把 10 年线拉得"虚高"。
    """
    if df_monthly is None or df_monthly.empty:
        out = df_monthly.copy() if df_monthly is not None else pd.DataFrame()
        if not out.empty:
            out['ma60_m'] = np.nan
        return out

    out = df_monthly.copy()
    out['ma60_m'] = out['close'].rolling(window=60, min_periods=60).mean()
    return out


# =============================================================================
# 月表（kline_qfq_monthly）数据访问层
# 把"月线 + ma60_m"从主流程的临时聚合改为查派生表，并把 T 当月作为 partial 行补上
# =============================================================================

def fetch_monthly_history_batch(instruments, t_month_key):
    """从派生月表批量加载多票"已完整月"数据（剔除 T 月及之后）。

    参数:
        instruments: 候选票列表（可能上千）。
        t_month_key: T 日所在月，格式 ``'YYYY-MM'``；月表中 ``month_key < t_month_key``
            才会返回，避免历史回测时把"按系统当前月剔除"的派生数据混入 T 月。

    返回:
        ``dict[instrument] -> DataFrame[month_key, last_trade_date, open, high, low,
        close, volume, ma60_m]``，按时间升序；某只票月表无数据时不存在该 key。

    注意:
        - 一次性 SQL 取 5000+ 票的全部历史月（55w 行级别）约 2-5 秒，远低于按票循环；
        - 月表里 ``ma60_m`` 已预存（前 59 月为 NULL），无需在 Python 端再算；
        - 如果月表为空（首次使用未回填），返回空 dict，上层应触发降级或终止。
    """
    if not instruments:
        return {}
    placeholders = ','.join([f"'{i}'" for i in instruments])
    q = f"""
        SELECT instrument, month_key, last_trade_date, open, high, low, close, volume, ma60_m
        FROM kline_qfq_monthly
        WHERE instrument IN ({placeholders})
          AND month_key < '{t_month_key}'
        ORDER BY instrument, month_key ASC
    """
    df = pd.read_sql(q, engine)
    if df.empty:
        return {}
    # last_trade_date → 字符串便于后续与日线 date 拼接
    df['last_trade_date'] = pd.to_datetime(df['last_trade_date']).dt.strftime('%Y-%m-%d')
    out = {}
    for inst, g in df.groupby('instrument', sort=False):
        out[inst] = g.drop(columns=['instrument']).reset_index(drop=True)
    return out


def attach_partial_t_month(df_monthly_history, df_daily_t_month):
    """把"T 当月（未走完）"的 partial 月线行拼到月表历史尾部，并算其 ``ma60_m``。

    参数:
        df_monthly_history: 该票月表的历史段（``fetch_monthly_history_batch`` 的单票元素），
            字段 ``month_key, last_trade_date, open, high, low, close, volume, ma60_m``，
            按时间升序，**不含** T 月。
        df_daily_t_month: 该票 T 月日线（含 ``date / open / high / low / close / volume``）；
            可为空（如新股当月还没成交记录），此时直接返回 history。

    返回:
        新的 DataFrame：history + 1 行 partial（如果有 T 月日线），列与 history 一致。
        partial 行的 ``ma60_m`` = (前 59 月 close 之和 + partial close) / 60；不足 59 月为 NaN。
    """
    if df_daily_t_month is None or df_daily_t_month.empty:
        return df_monthly_history.copy() if df_monthly_history is not None else pd.DataFrame()

    df_d = df_daily_t_month.copy()
    df_d['_dt'] = pd.to_datetime(df_d['date'])
    df_d = df_d.sort_values('_dt').reset_index(drop=True)

    partial = {
        'month_key': df_d['_dt'].iloc[-1].strftime('%Y-%m'),
        'last_trade_date': df_d['_dt'].iloc[-1].strftime('%Y-%m-%d'),
        'open': float(df_d['open'].iloc[0]),
        'high': float(df_d['high'].max()),
        'low': float(df_d['low'].min()),
        'close': float(df_d['close'].iloc[-1]),
        'volume': float(df_d['volume'].sum()),
        'ma60_m': np.nan,
    }
    # ma60_m for partial: 取 history 最近 59 月 close + partial.close 平均
    if df_monthly_history is not None and len(df_monthly_history) >= 59:
        last59 = df_monthly_history.iloc[-59:]['close'].astype(float).tolist()
        partial['ma60_m'] = (sum(last59) + partial['close']) / 60.0

    if df_monthly_history is None or df_monthly_history.empty:
        return pd.DataFrame([partial])
    return pd.concat([df_monthly_history, pd.DataFrame([partial])], ignore_index=True)


def check_daily_weekly_trend(df_daily):
    """
    日线 + 周线趋势硬过滤：要求日线收盘站上 MA20，周线收盘站上 MA10 周且 MA10 周斜率向上。

    参数:
        df_daily: 单票日线 DataFrame（同上）。

    返回:
        ``True`` 表示通过；``False`` 表示趋势不健康，应被剔除。

    注意:
        - 周线由日线按 ISO 周聚合（``%G-%V``），周收盘 = 当周最后一个交易日 close；
        - 数据不足（日线 < 60 或周线 < 12）时一律视为不通过，避免新股/停牌票误入；
        - 这是"硬过滤"层；如需改为软扣分，需在调用处改为加分逻辑而非直接 ``continue``。
    """
    if df_daily is None or df_daily.empty:
        return False

    df = df_daily.copy()
    df['_dt'] = pd.to_datetime(df['date'])
    df = df.sort_values('_dt').reset_index(drop=True)
    if len(df) < 60:
        return False

    # --- 日线：收盘 >= MA20，确认短期趋势未破位 ---
    df['ma20'] = df['close'].rolling(20, min_periods=20).mean()
    last = df.iloc[-1]
    if pd.isna(last['ma20']) or last['close'] < last['ma20']:
        return False

    # --- 周线聚合：以 ISO 周为 key（跨年友好），取每周最后交易日的 close ---
    df['week_key'] = df['_dt'].dt.strftime('%G-%V')
    weekly = (
        df.groupby('week_key', sort=False)
          .agg(close=('close', 'last'), date=('_dt', 'last'))
          .reset_index()
          .sort_values('date')
          .reset_index(drop=True)
    )
    if len(weekly) < 12:
        return False

    weekly['ma10w'] = weekly['close'].rolling(10, min_periods=10).mean()
    last_w = weekly.iloc[-1]
    if pd.isna(last_w['ma10w']) or last_w['close'] < last_w['ma10w']:
        return False

    # 斜率：最近 8 周 MA10w 端点差 > 0；不要求陡峭，只要不下行
    recent_ma10 = weekly['ma10w'].dropna().tail(8)
    if len(recent_ma10) < 2 or recent_ma10.iloc[-1] <= recent_ma10.iloc[0]:
        return False

    return True


def evaluate_monthly_rules(df_monthly, instrument):
    """
    月线规则综合评估：执行 4 条硬过滤 + 多项加分项，返回是否通过、明细 dict、加分总和。

    参数:
        df_monthly: 已含 ``ma60_m`` 列的月线 DataFrame（按时间升序），列为
            ``month_key / last_trade_date / open / high / low / close / volume / ma60_m``。
            通常由 ``fetch_monthly_history_batch`` 取月表 + ``attach_partial_t_month``
            拼接 T 当月 partial 得到；不再在本函数内部聚合日线，主流程提速依赖于此。
        instrument: 证券代码，用于按板块取月度涨幅上限（主板 35% / 双创 45%）。

    返回:
        三元组 ``(passed: bool, info: dict, bonus: float)``：
        - ``passed=False`` 表示被任一硬过滤拒绝，``info['reject_reason']`` 标明触发条件；
        - ``info`` 携带可解释字段，便于 ``--debug_stock`` 与人工对照 K 线复核；
        - ``bonus`` 为本层加分项之和。

    长期均线口径:
        统一采用 **月线 60 月均线（5 年线，``ma60_m``）**。
        历史背景：用户最初按行情软件月线视图标注的"MA120"实为"120 月线 / 10 年线"，
        而库内日线历史不足 10 年，无法稳定计算 10 年线，故折中用 5 年线。

    硬过滤（任一不通过即剔除）:
        1.c 3 年峰值（方案 Y, A∨B）::
            A) 当前月最高 ≥ 过去 36 月最高 × 0.95；或
            B) 本月"首次站上 ma60_m"且 当前月收 vs 过去 36 月最低 修复 ≥ 30%。
        2.a 月线低位徘徊：最近 6 月内月收 ≥ ma60_m 的次数 = 0。
        2.b 突破后久未再突破：最近 12 月中存在最早一次月收 ≥ ma60_m，但其后 ≥ 3 月月收 < ma60_m
            且最近 1 月月收仍 < ma60_m。
        2.c 近 3 月未站回前期高点：``prior_peak`` = "T 向前数第 4 个月起再回看 24 个月"区间月最高最大值；
            若最近 3 月最高 < ``prior_peak`` × 0.80 则剔除。

    加分项（每项独立判定，互不阻塞，部分互斥取大）:
        1.a 月线连涨（硬/软取大）::
             硬版：最近连续 ≥ 3 根月环比 > 0，且单月最大涨幅 ≤ 板块上限 → +15；
             软版：最近 6 月里 ≥ 4 月月环比 > 0 → +10；同时命中只取 +15。
        1.b 有效突破 ma60_m（梯度加分）::
             "有效突破"= 该月 月收 ≥ ma60_m 且 月最高 ≥ 3 年峰值 × 阈值
             （强：×1.0；弱：×0.8）。在最近 12 月内找 **最近** 一次有效突破，按"距今几月+强弱"给分：
                       当月  前1月  前2~3月  前4~6月  前7~12月
                 强突破:  20    15      12       8        5
                 弱突破:  12    10       8       5        3
        1.d 底部量能放大：最近 4 月月成交量均值 ≥ 之前 12 月均值 × 1.3 → +10。
        1.e 长期稳健站上：最近 6 月里 ≥ 5 月月收 ≥ ma60_m → +10。
        1.f 6 月主升浪：6 月累计涨幅 ∈ [30%, 120%] → +10（避免过热）。
        1.g 突破前蓄势：在 1.b 命中的"有效突破月"前，紧邻 2 月连涨 +5 / 3 月连涨 +8（取大）。
        附加：连续 2 月月收 ≥ ma60_m 且 这 2 月累计涨幅 ≤ 40% → +10。

    注意:
        - 数据不足（月线 < 6 / < 12 / < 16 / < 28 / < 60 等）时按"该规则保守不通过 / 不加分"处理；
        - 所有"上涨"以"月环比收盘 > 0"为准（与月 K 阴阳无关），与用户约定一致；
        - 当月可能未走完，按 T 日截止聚合，已在 ``aggregate_monthly_klines`` 处理。
    """
    info = {
        'reject_reason': None,
        # 硬过滤诊断
        'peak_3y_pass_path': None,
        'peak_3y_value': None,
        'peak_3y_current': None,
        'low_3y_value': None,
        'recovery_from_3y_low': None,
        'recent6m_above_ma60m_count': None,
        'first_break_ma60m_month': None,
        'months_below_after_break': None,
        'prior_peak_value': None,
        'prior_peak_recovery_ratio': None,
        # 1.a 月线连涨（硬/软）
        'monthly_consec_up_count': 0,
        'monthly_consec_up_max_pct': 0.0,
        'recent6m_up_count': 0,
        # 1.b 有效突破
        'eff_break_when': '无',          # 当月 / 前1月 / 前2-3月 / 前4-6月 / 前7-12月 / 无
        'eff_break_strength': '无',      # 强 / 弱 / 无
        'eff_break_month_key': None,
        # 1.d 量能放大
        'vol_expand_ratio': None,
        # 1.e/1.f
        'recent6m_above_ma60m_count_for_bonus': None,
        'six_month_return': None,
        # 1.g 突破前蓄势
        'pre_break_consec_up': 0,
        # 附加
        'two_months_above_ma60m_pass': False,
        'two_months_cum_ret': None,
        # 各加分细项
        'bonus_1a': 0,
        'bonus_1b': 0,
        'bonus_1d': 0,
        'bonus_1e': 0,
        'bonus_1f': 0,
        'bonus_1g': 0,
        'bonus_2m': 0,
    }

    # --- 月线表（已带 ma60_m，由调用方从派生月表 + T 当月 partial 拼接得到）---
    if df_monthly is None or len(df_monthly) < 6:
        info['reject_reason'] = '月线数据不足(<6)'
        return False, info, 0.0
    df_m = df_monthly

    # 当 ma60_m 在最近月仍是 NaN（典型为上市不足 5 年的新股），所有依赖 ma60_m 的规则
    # 都按"数据不足，规则跳过"处理：不强行拒绝，也不给加分；这样能保留新股走通其他规则的机会。
    ma60m_available = bool(pd.notna(df_m.iloc[-1]['ma60_m']))
    info['ma60m_available'] = ma60m_available

    cur = df_m.iloc[-1]
    info['peak_3y_current'] = round(float(cur['high']), 2)

    thresh = get_stock_type_thresholds(instrument)
    # 单月涨幅上限：主板 35%，双创 45%（沿用 ``get_stock_type_thresholds`` 的板块差异思想）
    max_monthly_gain = 0.45 if thresh['is_cyb'] else 0.35

    # =========================================================================
    # 硬过滤 1.c：3 年峰值（方案 Y，A 或 B 任一通过）
    # =========================================================================
    # 取"当前月之前"的最近 36 月作为参考；不足则用现有所有历史
    prev_window = df_m.iloc[-37:-1] if len(df_m) >= 37 else df_m.iloc[:-1]
    pass_a = False
    pass_b = False

    if len(prev_window) >= 1:
        peak_3y = float(prev_window['high'].max())
        low_3y = float(prev_window['low'].min())
        info['peak_3y_value'] = round(peak_3y, 2)
        info['low_3y_value'] = round(low_3y, 2)
        # A 路径：当前月最高接近或超过 3 年最高（保留 5% 缓冲，避免毫厘误杀）
        if peak_3y > 0 and float(cur['high']) >= peak_3y * 0.95:
            pass_a = True
    else:
        peak_3y = None
        low_3y = None

    # B 路径：本月"首次站上 ma60_m"（上一根月收 < ma60_m，当前月收 ≥ ma60_m）+ 自 3 年低点修复 ≥ 30%
    is_first_break_this_month = False
    if ma60m_available and pd.notna(cur['ma60_m']) and cur['close'] >= cur['ma60_m']:
        prev = df_m.iloc[-2]
        if pd.notna(prev['ma60_m']) and prev['close'] < prev['ma60_m']:
            is_first_break_this_month = True
    if is_first_break_this_month and low_3y is not None and low_3y > 0:
        recovery = (float(cur['close']) - low_3y) / low_3y
        info['recovery_from_3y_low'] = round(recovery, 3)
        if recovery >= 0.30:
            pass_b = True

    if not (pass_a or pass_b):
        info['peak_3y_pass_path'] = '未通过'
        info['reject_reason'] = '1.c 3年峰值未通过(A∧B 均不满足)'
        return False, info, 0.0
    info['peak_3y_pass_path'] = 'A:历史峰值' if pass_a else 'B:突破+修复'

    # =========================================================================
    # 硬过滤 2.a：月线低位徘徊（最近 6 月没有任何一根月收 ≥ ma60_m）
    #   ma60_m 不可用（新股不足 5 年）时跳过此项硬过滤，避免一刀切误杀。
    # =========================================================================
    last6 = df_m.iloc[-6:]
    above6 = int(((last6['close'] >= last6['ma60_m']) & last6['ma60_m'].notna()).sum())
    info['recent6m_above_ma60m_count'] = above6
    info['recent6m_above_ma60m_count_for_bonus'] = above6
    if ma60m_available and above6 == 0:
        info['reject_reason'] = '2.a 近6月月收均未站上 ma60_m'
        return False, info, 0.0

    # =========================================================================
    # 硬过滤 2.b：曾突破 ma60_m 但久未再突破
    #   - 在最近 12 月中找最早一次月收 ≥ ma60_m 的月 m*；
    #   - 若 m* 之后已有 ≥ 3 根月收 < ma60_m 且最近 1 根月收仍 < ma60_m，则剔除。
    #   - ma60_m 不可用时跳过此项。
    # =========================================================================
    if ma60m_available and len(df_m) >= 12:
        last12 = df_m.iloc[-12:].reset_index(drop=True)
        first_break_local_idx = None
        for i in range(len(last12)):
            r = last12.iloc[i]
            if pd.notna(r['ma60_m']) and r['close'] >= r['ma60_m']:
                first_break_local_idx = i
                break
        if first_break_local_idx is not None:
            after_break = last12.iloc[first_break_local_idx + 1:]
            below_count = int(((after_break['close'] < after_break['ma60_m']) & after_break['ma60_m'].notna()).sum())
            info['first_break_ma60m_month'] = last12.iloc[first_break_local_idx]['month_key']
            info['months_below_after_break'] = below_count
            last_row = last12.iloc[-1]
            if (
                below_count >= 3
                and pd.notna(last_row['ma60_m'])
                and last_row['close'] < last_row['ma60_m']
            ):
                info['reject_reason'] = '2.b 突破ma60_m后久未再突破(≥3月在线下且当月仍在线下)'
                return False, info, 0.0

    # =========================================================================
    # 硬过滤 2.c：近 3 月最高 < prior_peak × 0.80（雷科防务情形）
    #   prior_peak = 自当前往前第 4 个月起，再向更早回看 24 个月，区间内月最高的最大值
    # =========================================================================
    if len(df_m) >= 4:
        # 切片：iloc[-(4+24):-3] = 取索引 -28..-4 共 24 根（最早一根可能因长度不足而少于 24 根）
        prior_window = df_m.iloc[-(4 + 24):-3] if len(df_m) >= 28 else df_m.iloc[:-3]
        if len(prior_window) > 0:
            prior_peak = float(prior_window['high'].max())
            recent3_high = float(df_m.iloc[-3:]['high'].max())
            info['prior_peak_value'] = round(prior_peak, 2)
            ratio = (recent3_high / prior_peak) if prior_peak > 0 else 0.0
            info['prior_peak_recovery_ratio'] = round(ratio, 3)
            # 阈值 0.80：偏宽松，便于人工二次复核（用户要求适度放宽）
            if prior_peak > 0 and recent3_high < prior_peak * 0.80:
                info['reject_reason'] = '2.c 近3月最高未站回前期高点的80%'
                return False, info, 0.0

    bonus = 0.0

    # =========================================================================
    # 加分 1.a：月线连涨（硬版 +15 / 软版 +10，取大）
    #   硬版：最近连续 ≥ 3 月月环比 > 0 且 单月涨幅 ≤ 板块上限
    #   软版：最近 6 月里 ≥ 4 月月环比 > 0
    # =========================================================================
    consec_up = 0
    consec_max_pct = 0.0
    for i in range(len(df_m) - 1, 0, -1):
        c_now = float(df_m.iloc[i]['close'])
        c_prev = float(df_m.iloc[i - 1]['close'])
        if c_prev > 0 and c_now > c_prev:
            consec_up += 1
            consec_max_pct = max(consec_max_pct, (c_now - c_prev) / c_prev)
        else:
            break
    info['monthly_consec_up_count'] = consec_up
    info['monthly_consec_up_max_pct'] = round(consec_max_pct, 3)

    bonus_1a_hard = 15 if (consec_up >= 3 and consec_max_pct <= max_monthly_gain) else 0
    # 软版：滚动看最近 6 个月，月环比 > 0 的根数
    recent6m_up_count = 0
    if len(df_m) >= 7:
        for j in range(len(df_m) - 6, len(df_m)):
            c_now = float(df_m.iloc[j]['close'])
            c_prev = float(df_m.iloc[j - 1]['close'])
            if c_prev > 0 and c_now > c_prev:
                recent6m_up_count += 1
    info['recent6m_up_count'] = recent6m_up_count
    bonus_1a_soft = 10 if recent6m_up_count >= 4 else 0
    bonus_1a = max(bonus_1a_hard, bonus_1a_soft)
    info['bonus_1a'] = bonus_1a
    bonus += bonus_1a

    # =========================================================================
    # 加分 1.b：有效突破 ma60_m 的梯度加分
    #   有效突破月 m：close[m] ≥ ma60_m[m]  AND  high[m] ≥ peak_3y(对 m 之前 36 月) × 阈值
    #     强：阈值 1.0；弱：阈值 0.8
    #   策略：在最近 12 个月里，从"最近"往回找第一根「至少弱突破」的月份；
    #         强弱以该月实际命中的最高阈值为准；按"距今几月 + 强弱"查表给分。
    # =========================================================================
    n = len(df_m)
    eff_when_label = '无'
    eff_strength = '无'
    eff_when_offset = None       # 0=当月, 1=前1月, ...
    eff_break_idx = None         # 在 df_m 里的整数索引

    # 距今偏移 -> 标签
    def _offset_to_label(off):
        if off == 0:
            return '当月'
        if off == 1:
            return '前1月'
        if 2 <= off <= 3:
            return '前2-3月'
        if 4 <= off <= 6:
            return '前4-6月'
        if 7 <= off <= 12:
            return '前7-12月'
        return '无'

    # 强突破得分表
    score_strong = {'当月': 20, '前1月': 15, '前2-3月': 12, '前4-6月': 8, '前7-12月': 5}
    score_weak   = {'当月': 12, '前1月': 10, '前2-3月': 8,  '前4-6月': 5, '前7-12月': 3}

    max_lookback = min(12, n)
    for off in range(0, max_lookback):
        idx = n - 1 - off
        if idx < 1:
            break
        row = df_m.iloc[idx]
        if pd.isna(row['ma60_m']):
            continue
        if float(row['close']) < float(row['ma60_m']):
            continue
        # 计算该月之前 36 月的 3 年峰值
        win = df_m.iloc[max(0, idx - 36):idx]
        if len(win) < 1:
            continue
        peak_for_m = float(win['high'].max())
        if peak_for_m <= 0:
            continue
        high_m = float(row['high'])
        if high_m >= peak_for_m * 1.0:
            eff_strength = '强'
        elif high_m >= peak_for_m * 0.8:
            eff_strength = '弱'
        else:
            continue
        eff_when_offset = off
        eff_when_label = _offset_to_label(off)
        eff_break_idx = idx
        break

    info['eff_break_when'] = eff_when_label
    info['eff_break_strength'] = eff_strength
    if eff_break_idx is not None:
        info['eff_break_month_key'] = df_m.iloc[eff_break_idx]['month_key']
    if eff_when_label != '无' and eff_strength != '无':
        table = score_strong if eff_strength == '强' else score_weak
        bonus_1b = table.get(eff_when_label, 0)
    else:
        bonus_1b = 0
    info['bonus_1b'] = bonus_1b
    bonus += bonus_1b

    # =========================================================================
    # 加分 1.g：在 1.b 命中的"有效突破月"前，紧邻 2/3 月连涨（互斥取大）
    #   2 月连涨 +5；3 月连涨 +8
    # =========================================================================
    pre_consec = 0
    if eff_break_idx is not None and eff_break_idx >= 1:
        for k in range(eff_break_idx - 1, 0, -1):
            c_now = float(df_m.iloc[k]['close'])
            c_prev = float(df_m.iloc[k - 1]['close'])
            if c_prev > 0 and c_now > c_prev:
                pre_consec += 1
            else:
                break
    info['pre_break_consec_up'] = pre_consec
    if pre_consec >= 3:
        bonus_1g = 8
    elif pre_consec == 2:
        bonus_1g = 5
    else:
        bonus_1g = 0
    info['bonus_1g'] = bonus_1g
    bonus += bonus_1g

    # =========================================================================
    # 加分 1.d：底部起涨阶段量能放大
    #   最近 4 月月成交量均值 ≥ 之前 12 月均值 × 1.3
    # =========================================================================
    if len(df_m) >= 16:
        recent_n = 4
        recent_vol = float(df_m.iloc[-recent_n:]['volume'].mean())
        prev12_vol = float(df_m.iloc[-(recent_n + 12):-recent_n]['volume'].mean())
        if prev12_vol > 0:
            ratio_v = recent_vol / prev12_vol
            info['vol_expand_ratio'] = round(ratio_v, 2)
            if ratio_v >= 1.3:
                info['bonus_1d'] = 10
                bonus += 10

    # =========================================================================
    # 加分 1.e：长期稳健站上 ma60_m
    #   最近 6 月里 ≥ 5 月 月收 ≥ ma60_m
    # =========================================================================
    if above6 >= 5:
        info['bonus_1e'] = 10
        bonus += 10

    # =========================================================================
    # 加分 1.f：6 月主升浪（30% ≤ 6月累计涨幅 ≤ 120%）
    #   过滤"已经太热"的票，留出后续上涨空间
    # =========================================================================
    if len(df_m) >= 7:
        c_now6 = float(df_m.iloc[-1]['close'])
        c_base6 = float(df_m.iloc[-7]['close'])
        if c_base6 > 0:
            ret6 = (c_now6 - c_base6) / c_base6
            info['six_month_return'] = round(ret6, 3)
            if 0.30 <= ret6 <= 1.20:
                info['bonus_1f'] = 10
                bonus += 10

    # =========================================================================
    # 加分 附加：连续 2 月月收 ≥ ma60_m，且 2 月累计涨幅 ≤ 40%
    # =========================================================================
    if len(df_m) >= 3:
        m1 = df_m.iloc[-1]
        m2 = df_m.iloc[-2]
        m_before2 = df_m.iloc[-3]
        if (
            pd.notna(m1['ma60_m']) and pd.notna(m2['ma60_m'])
            and m1['close'] >= m1['ma60_m']
            and m2['close'] >= m2['ma60_m']
            and float(m_before2['close']) > 0
        ):
            cum_ret = (float(m1['close']) - float(m_before2['close'])) / float(m_before2['close'])
            info['two_months_cum_ret'] = round(cum_ret, 3)
            if cum_ret <= 0.40:
                info['two_months_above_ma60m_pass'] = True
                info['bonus_2m'] = 10
                bonus += 10

    return True, info, bonus


def run_quant_picker(t_date=None, top_k=20, debug_stock=None):
    """
    全市场（或单票调试）执行选股流水线，并落盘 JSON 结果。

    参数:
        t_date: 选股锚定日 ``YYYY-MM-DD``；``None`` 时用当前系统日期向库取不晚于该日的交易日历。
        top_k: 输出得分最高的前 K 只股票（默认 20）。
        debug_stock: 若指定证券代码（如 ``600519.SH``），只拉取该票并打印各阶段诊断日志。

    返回:
        无显式返回值；结果写入 ``results/top_{top_k}_stocks_{end_date}.json``，异常路径通过日志提示。

    注意:
        ``end_date`` 为所取交易日中的最后一天，未必等于 ``t_date``（当 ``t_date`` 非交易日时
        会落在最近的前一交易日）；``debug_stock`` 的 SQL 为字符串拼接，仅用于本地可信调试。
        日线主取数缩回 ~850 交易日；月线层走 ``kline_qfq_monthly`` 派生表 + T 当月 partial。
    """
    logging.info(f"开始执行量化选股 (T日: {t_date or '最新'})")

    # 主取数：~850 交易日（约 3.5 年）覆盖动态四窗口/形态识别/微观/趋势所需跨度。
    # 月线层数据已迁到派生月表（kline_qfq_monthly），不依赖此窗口长度。
    dates = get_trading_days(t_date, count=850)
    if len(dates) < 200:
        logging.error("交易日数据不足（需要 >= 200）。")
        return

    start_date = dates[0]
    end_date = dates[-1]

    logging.info(f"提取数据总区间: {start_date} 到 {end_date} (共 {len(dates)} 个交易日)")
    
    if debug_stock:
        logging.info(f"--- DEBUG 模式: 仅分析股票 {debug_stock} ---")
        query = f"""
            SELECT k.*, s.name as stock_name
            FROM kline_qfq k
            JOIN stock_list s ON k.instrument = s.instrument
            WHERE k.date >= '{start_date}' AND k.date <= '{end_date}'
            AND k.instrument = '{debug_stock}'
            ORDER BY k.date ASC
        """
        df_all = pd.read_sql(query, engine)
    else:
        df_all = get_kline_data(start_date, end_date)
        
    if df_all.empty:
        logging.error("未获取到K线数据。")
        return
        
    df_all['date'] = pd.to_datetime(df_all['date']).dt.strftime('%Y-%m-%d')

    # === 月线层数据预取（一次性 SQL，避免循环内拉数据）===
    # T 月 key（'YYYY-MM'）：用于从月表只取"严格在 T 月之前"的已完整月，
    # T 月本身按 partial（截至 T 日的日线现场聚合）拼到尾部。
    t_month_key = end_date[:7]
    candidate_instruments = sorted(df_all['instrument'].unique().tolist())
    t_month_start = t_month_key + '-01'

    logging.info(f"加载月表（{len(candidate_instruments)} 票，T 月之前的已完整月）...")
    t0_m = datetime.now()
    monthly_history_map = fetch_monthly_history_batch(candidate_instruments, t_month_key)
    logging.info(
        f"月表加载完成：{len(monthly_history_map)}/{len(candidate_instruments)} 票有数据；"
        f"耗时 {(datetime.now() - t0_m).total_seconds():.2f}s"
    )

    # T 月日线：从主取数 df_all 里筛选（已经在内存）；按 instrument 分组留作 partial 输入。
    df_t_month = df_all[df_all['date'] >= t_month_start]
    t_month_daily_map = {inst: g for inst, g in df_t_month.groupby('instrument')}

    results = []
    grouped = df_all.groupby('instrument')
    total = len(grouped)
    count = 0
    
    for inst, group in grouped:
        count += 1
        if not debug_stock and count % 500 == 0:
            logging.info(f"处理进度: {count}/{total}")
            
        group_df = group.copy()
        
        # 1. 宏观：动态四窗口 + 形态识别
        windows = segment_windows(group_df, end_date)
        
        if debug_stock:
            logging.info(f"[{inst}] 窗口分割结果:")
            for i, w in enumerate(windows):
                logging.info(f"  W{i+1}: {w['start_date']} -> {w['end_date']} | {w['direction']} | 幅度: {w['amplitude']:.2f}% | High: {w['high']} Low: {w['low']} | 涨停数: {w['limit_up_count']}")
        
        pattern_type, pattern_desc, pattern_roles = identify_pattern(windows, inst)
        
        if debug_stock:
            logging.info(f"[{inst}] 形态识别结果: {pattern_desc if pattern_desc else '未识别到形态'}")
            if pattern_roles:
                logging.info(f"  角色: {pattern_roles}")
        
        if not pattern_type:
            if debug_stock: logging.info(f"[{inst}] 失败: 未匹配到 N/W/H/V 形态")
            continue
            
        # 2. 风控过滤
        is_risky = check_risk(windows, inst)
        if debug_stock:
            logging.info(f"[{inst}] 风控检查: {'高风险' if is_risky else '通过'}")
             
        if is_risky:
            if debug_stock: logging.info(f"[{inst}] 失败: 触发风控")
            continue
            
        # 3. 微观：A→B→C 评分，>0 才入候选
        if windows:
            w1_start = windows[0]['start_date']
        else:
            w1_start = dates[-23] if len(dates) > 23 else dates[0]
            
        score, detail = score_stock_micro(group_df, w1_start)
        
        if debug_stock:
            logging.info(f"[{inst}] 微观评分: {score}")
            if score > 0:
                logging.info(f"  详情: {detail}")
        
        if score <= 0:
            if debug_stock:
                logging.info(f"[{inst}] 失败: 微观评分 = 0")
            continue

        # 4. 日线/周线趋势硬过滤：要求短期趋势未破位、周线 MA10w 仍在抬头
        trend_ok = check_daily_weekly_trend(group_df)
        if debug_stock:
            logging.info(f"[{inst}] 日/周线趋势: {'通过' if trend_ok else '不通过'}")
        if not trend_ok:
            if debug_stock:
                logging.info(f"[{inst}] 失败: 日/周线趋势硬过滤未通过")
            continue

        # 5. 月线层规则：硬过滤(1.c/2.a/2.b/2.c) + 加分(1.a/1.b/1.d/1.e/1.f/1.g/连续2月+≤40%)
        #    长期均线统一使用月线 60 月均线（5 年线，ma60_m），见 evaluate_monthly_rules 文档
        # 月线数据 = 月表历史（已含 ma60_m）+ T 月 partial（现场聚合 + 现场算 ma60_m）
        df_m_history = monthly_history_map.get(inst)
        df_d_t_month = t_month_daily_map.get(inst)
        df_m_full = attach_partial_t_month(df_m_history, df_d_t_month)
        monthly_pass, monthly_info, monthly_bonus = evaluate_monthly_rules(df_m_full, inst)
        if debug_stock:
            logging.info(f"[{inst}] 月线规则: {'通过' if monthly_pass else '不通过'} | bonus={monthly_bonus}")
            logging.info(f"  月线明细: {monthly_info}")
        if not monthly_pass:
            if debug_stock:
                logging.info(f"[{inst}] 失败: 月线硬过滤 -> {monthly_info.get('reject_reason')}")
            continue

        # === 通过所有过滤，组装结果记录 ===
        detail['pattern'] = pattern_desc
        detail['windows_info'] = f"W1:{windows[0]['direction']}({windows[0]['amplitude']:.1f}%)"

        # 写入动态窗口明细，便于人工对照 K 线复核
        detail['windows_detail'] = []
        for i, w in enumerate(windows):
            w_label = f"W{i+1}"
            role = pattern_roles.get(w_label, 'Context') if pattern_roles else 'Context'
            detail['windows_detail'].append({
                'window': w_label,
                'role': role,
                'start_date': w['start_date'],
                'end_date': w['end_date'],
                'direction': w['direction'],
                'amplitude': round(w['amplitude'], 2)
            })

        # 月线层可解释字段全部写入：方便 --debug_stock 与人工查验
        detail['monthly_bonus'] = monthly_bonus
        detail['monthly'] = monthly_info
        # final_score = 微观分(原 total_score) + 月线 bonus；用于最终排序
        detail['final_score'] = round(detail['total_score'] + monthly_bonus, 2)

        results.append(detail)

    # 按 final_score 降序：让月线趋势加分项把"理想形态"（如 002082 万邦德）顶到前面
    results.sort(key=lambda x: x['final_score'], reverse=True)
    top_results = results[:top_k]

    logging.info(f"--- 选股完成，TOP-{len(top_results)} ---")
    for i, res in enumerate(top_results, 1):
        # 控制台/日志只保留一行摘要；月线逐项诊断在 JSON 的 ``monthly`` 字段里
        logging.info(
            f"{i:02d}. [{res['pattern']}] {res['instrument']} {res['name']} | "
            f"final={res['final_score']} (micro={res['total_score']} + month={res['monthly_bonus']}) | "
            f"{res['windows_info']}"
        )
              
    if not os.path.exists('results'):
        os.makedirs('results')
        
    out_file = f"results/top_{top_k}_stocks_{end_date}.json"
    pd.DataFrame(top_results).to_json(out_file, orient='records', force_ascii=False, indent=2)
    logging.info(f"结果已保存至 {out_file}")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="OpenClaw 量化选股：动态窗口形态(N/W/H/V) + A→B→C 微观分 + 月线/周线趋势加权。",
        epilog="示例: python scripts/quant_picker.py --t_date 2025-10-31 --top_k 30\n"
        "调试单票: python scripts/quant_picker.py --debug_stock 002082.SZ --t_date 2025-10-31",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        '--t_date',
        type=str,
        default=None,
        help='锚定交易日 YYYY-MM-DD（库中取不晚于该日的最近交易日区间）',
    )
    parser.add_argument('--top_k', type=int, default=20, help='输出得分最高的前 K 只，默认 20')
    parser.add_argument(
        '--debug_stock',
        type=str,
        default=None,
        help='仅分析该 instrument（如 600519.SH），并输出窗口/形态/评分详细日志',
    )
    args = parser.parse_args()

    run_quant_picker(t_date=args.t_date, top_k=args.top_k, debug_stock=args.debug_stock)
