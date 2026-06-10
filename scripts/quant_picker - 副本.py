"""
量化选股（Phase 2）：基于本地数据库的多层筛选流水线。

功能概述：
    1. 从 ``trading_calendar`` 取 T 日及以前约 150 个交易日，确定 K 线拉取区间；
    2. 从 ``kline_qfq`` 联表 ``stock_list`` 读取前复权日线，排除名称含 ST/退 的品种；
    3. 对每只股票按「动态四窗口」切分（约每月一段，从 T 向前递归），计算每窗高低点、
       方向、振幅、涨跌停计数等特征；
    4. ``identify_pattern`` 识别 N / W / H / V 四类技术形态（创业板/科创板与主板阈值不同）；
    5. ``check_risk`` 过滤短期过热、弱势反弹等高风险情形；
    6. ``score_stock_micro`` 在最新窗口起点之后做 A（拉升）→ B（回踩）→ C（再起）微观打分；
    7. 按总分降序取 Top-K，写入 ``results/top_{K}_stocks_{T}.json``。

数据口径：
    - 日期为 ``YYYY-MM-DD`` 字符串与交易日对齐；K 线为前复权（表 ``kline_qfq``）。
    - 形态与风控中的百分比阈值为经验规则，修改会影响召回率与误报率。

运行方式（建议在仓库根目录执行，以便 ``logs/``、``results/`` 相对路径一致）::

    python scripts/quant_picker.py
    python scripts/quant_picker.py --t_date 2025-01-15 --top_k 30
    python scripts/quant_picker.py --debug_stock 600519.SH

注意：
    - 依赖 ``scripts.db_session`` 中的数据库连接；无数据或交易日不足时会记录错误并提前返回。
    - ``--debug_stock`` 仅分析单票并打出窗口/形态/评分日志，便于对照 K 线排查规则。
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

def get_trading_days(t_date=None, count=150):
    """
    查询 T 日（含）以前的连续交易日列表，从早到晚排序。

    参数:
        t_date: 锚定交易日，格式 ``YYYY-MM-%d``；默认当前机器日期（未必是交易日）。
        count: 向前取多少个交易日，默认 150，用于覆盖四窗口回溯所需跨度。

    返回:
        ``list[str]``，元素为 ``YYYY-MM-%d``，按时间升序。

    注意:
        若库中交易日记录不足，返回列表可能短于 ``count``，上层需判断长度。
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
        ``end_date`` 为所取 150 个交易日中的最后一天，未必等于 ``t_date``（当 ``t_date`` 非交易日时
        会落在最近的前一交易日）；``debug_stock`` 的 SQL 为字符串拼接，仅用于本地可信调试。
    """
    logging.info(f"开始执行量化选股 (T日: {t_date or '最新'})")

    dates = get_trading_days(t_date, count=150)
    if len(dates) < 50:
        logging.error("数据不足。")
        return
        
    start_date = dates[0]
    end_date = dates[-1]
    
    logging.info(f"提取数据总区间: {start_date} 到 {end_date} (用于动态窗口分析)")
    
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
        
        if score > 0:
            detail['pattern'] = pattern_desc
            detail['windows_info'] = f"W1:{windows[0]['direction']}({windows[0]['amplitude']:.1f}%)"
            
            # Add detailed window info
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
            
            results.append(detail)
            
    results.sort(key=lambda x: x['total_score'], reverse=True)
    top_results = results[:top_k]
    
    logging.info(f"--- 选股完成，TOP-{len(top_results)} ---")
    for i, res in enumerate(top_results, 1):
        logging.info(f"{i:02d}. [{res['pattern']}] {res['instrument']} {res['name']} | 总分: {res['total_score']} "
              f"| {res['windows_info']}")
              
    if not os.path.exists('results'):
        os.makedirs('results')
        
    out_file = f"results/top_{top_k}_stocks_{end_date}.json"
    pd.DataFrame(top_results).to_json(out_file, orient='records', force_ascii=False, indent=2)
    logging.info(f"结果已保存至 {out_file}")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="OpenClaw 量化选股：基于动态窗口形态（N/W/H/V）与 A→B→C 微观结构打分。",
        epilog="示例: python scripts/quant_picker.py --t_date 2025-03-01 --top_k 30\n"
        "调试单票: python scripts/quant_picker.py --debug_stock 600519.SH",
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
