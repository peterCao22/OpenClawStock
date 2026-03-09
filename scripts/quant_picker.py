import logging
import os
import sys
import argparse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.db_session import engine

# Configure logging
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
    获取T日及之前的交易日历
    为了支持4个动态窗口的回溯，我们需要更长的历史数据，这里默认取150天
    """
    if not t_date:
        t_date = datetime.now().strftime('%Y-%m-%d')
        
    query = f"SELECT trade_date FROM trading_calendar WHERE is_trading_day = true AND trade_date <= '{t_date}' "
    query += f"ORDER BY trade_date DESC LIMIT {count}"
    
    df = pd.read_sql(query, engine)
    dates = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d').tolist()
    dates.reverse() # 从早到晚
    return dates

def get_kline_data(start_date, end_date):
    """获取指定区间的K线数据，并过滤掉ST、停牌股"""
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
    """根据股票代码返回涨跌幅阈值"""
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
    """计算单个窗口的特征"""
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
    
    # Determine direction and amplitude
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
            
    # Calculate daily changes for stats
    # Assuming df_window is a slice of a larger df, we might miss pre_close for the first element
    # But we can approximate or use what we have.
    # Let's calculate pct_change on the fly for this window
    df_w = df_window.copy()
    df_w['pct_change'] = df_w['close'].pct_change() * 100
    df_w['pct_change'] = df_w['pct_change'].fillna(0)
    
    thresh = get_stock_type_thresholds(instrument)
    
    limit_up_count = len(df_w[df_w['pct_change'] >= thresh['limit_up']])
    limit_down_count = len(df_w[df_w['pct_change'] <= thresh['limit_down']])
    
    # Positive/Negative days
    up_days = len(df_w[df_w['close'] > df_w['open']])
    down_days = len(df_w[df_w['close'] < df_w['open']])
    
    # Calculate real change (interval percentage change)
    # (Close_End - Close_Start) / Close_Start * 100
    # Note: Using Close_Start (close of the first day of the window)
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
    动态分割4个窗口
    Returns: List of window features [W1, W2, W3, W4] (W1 is latest)
    """
    windows = []
    current_end_date = t_date
    
    # Ensure df is sorted by date
    df = df.sort_values('date').reset_index(drop=True)
    
    for i in range(4):
        # Find end index
        end_indices = df[df['date'] <= current_end_date].index
        if len(end_indices) == 0:
            break
        end_idx = end_indices[-1]
        
        # Start index = end_idx - 22 (approx 1 month)
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
        
        # Prevent infinite loop if next_end == current_end_date
        if next_end == current_end_date:
             # Force move back to start of current window to avoid getting stuck
             if start_idx > 0:
                 # Move to the day before start of current window? 
                 # Or just use start_date of current window.
                 # Let's use the date at start_idx
                 current_end_date = df.iloc[start_idx]['date']
                 # If that is also same (1 day window), break
                 if current_end_date == next_end:
                     break
             else:
                 break
        else:
            current_end_date = next_end
            
    return windows

def identify_pattern(windows, instrument):
    """
    识别 N/W/H/V 形态
    windows: [W1, W2, W3, W4] (W1 is latest)
    Returns: (PatternType, Description, PatternRoles)
    """
    if len(windows) < 4:
        return None, "Insufficient Data (Need 4 Windows)", None
        
    w1, w2, w3, w4 = windows[0], windows[1], windows[2], windows[3]
    thresh = get_stock_type_thresholds(instrument)
    
    # --- 1. N Pattern (N型) ---
    # Logic: Surge (W4/W3) -> Wash (W2) -> Surge (W1)
    # W4 or W3 must be a strong UP wave
    w4_surge = w4['direction'] == 'UP' and w4['amplitude'] > thresh['surge_trigger']
    w3_surge = w3['direction'] == 'UP' and w3['amplitude'] > thresh['surge_trigger']
    
    if (w4_surge or w3_surge):
        # W2 should be a wash (Down or weak Up, or consolidation)
        # Check if W1 is starting a new surge
        # [Optimization] W1 must have real gains (> 5%) and close near high (no long upper shadow)
        if w1['direction'] == 'UP' and w1['close_end'] > w2['low']:
             # High position check: W1 High should be close to previous high
             prev_high = max(w4['high'], w3['high'])
             if w1['high'] >= prev_high * 0.85: # 85% recovery
                 if w1['real_change'] > 5.0 and w1['close_end'] > w1['high'] * 0.8:
                     roles = {
                         'W4': 'Surge (Start)' if w4_surge else 'Context',
                         'W3': 'Surge (Start)' if w3_surge else 'Context',
                         'W2': 'Wash (Pullback)',
                         'W1': 'Surge (Second Wave)'
                     }
                     return "N", "N型: 强势整理后再起", roles

    # --- 2. H Pattern (H型) ---
    # Logic: Huge Surge (W4/W3) -> High Platform (W2, W1)
    # Flag pole
    w4_flag = w4['direction'] == 'UP' and w4['amplitude'] > thresh['flag_pole']
    w3_flag = w3['direction'] == 'UP' and w3['amplitude'] > thresh['flag_pole']
    
    if (w4_flag or w3_flag):
        flag_low = w4['low'] if w4_flag else w3['low'] # Start of pole
        flag_high = w4['high'] if w4_flag else w3['high']
        
        # Platform check: W2 and W1 lows should be significantly above flag_low
        # Meaning they held the gains
        if w2['low'] > flag_low * 1.1 and w1['low'] > flag_low * 1.1:
            # And W1 is near the top
            if w1['close_end'] > flag_high * 0.8: 
                # [Optimization] H-Pattern Failure Check:
                # If W1 drops too much (e.g. > 20%), it might be a breakdown, not a consolidation
                if w1['direction'] == 'DOWN' and w1['amplitude'] < -20.0:
                    return None, None, None
                
                roles = {
                     'W4': 'Flag Pole' if w4_flag else 'Context',
                     'W3': 'Flag Pole' if w3_flag else 'Context',
                     'W2': 'Platform',
                     'W1': 'Breakout/Platform'
                }
                return "H", "H型: 高位横盘强者恒强", roles

    # --- 3. V Pattern (V底) ---
    # Logic: Crash (W2) -> V Reversal (W1)
    if w2['direction'] == 'DOWN' and w2['amplitude'] < -15.0: # Deep drop
        # W1 Reversal
        if w1['direction'] == 'UP':
            # [Optimization] V-Pattern Tightening:
            # 1. Amplitude > 20% (Absolute strength)
            # 2. Recovery > 65% of drop (Relative strength) - Filter out "L-shape"
            # 3. Real Change > 10% (Must actually gain > 10%)
            if w1['amplitude'] > 20.0 and w1['amplitude'] > abs(w2['amplitude']) * 0.65 and w1['real_change'] > 10.0:
                roles = {
                    'W2': 'Crash',
                    'W1': 'Reversal',
                    'W3': 'Context',
                    'W4': 'Context'
                }
                return "V", "V底: 超跌反转", roles
            # Or if W1 is very sharp (short time, high amp) - Keep this for super sharp V
            if w1['amplitude'] > 25.0 and w1['amplitude'] > abs(w2['amplitude']) * 0.6 and w1['real_change'] > 10.0:
                roles = {
                    'W2': 'Crash',
                    'W1': 'Reversal',
                    'W3': 'Context',
                    'W4': 'Context'
                }
                return "V", "V底: 强力反弹", roles

    # --- 4. W Pattern (W底) ---
    # Logic: Double Bottom. 
    # Look for two lows in W3/W2/W1 that are close.
    # Let's gather all lows from W3, W2, W1
    lows = [w3['low'], w2['low'], w1['low']]
    min_l = min(lows)
    
    # Check if we have two distinct lows close to min_l
    # This is a bit heuristic.
    # Alternative: W3 Down, W2 Up/Down, W1 Up.
    # Check if W1 Low is close to W3 Low (or W2 Low)
    
    # Case 1: W3 Low and W1 Low
    if abs(w3['low'] - w1['low']) / w3['low'] < 0.1: # Within 10%
        # [Optimization] W-Pattern Confirmation:
        # W1 must be UP and have enough strength (> 5%) to confirm the right bottom support
        # AND W1 High must recover at least 50% of the drop from W2 High (Neckline logic)
        # AND W1 Real Change > 3.0% (Positive gain)
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
            
    # Case 2: W2 Low and W1 Low
    if abs(w2['low'] - w1['low']) / w2['low'] < 0.1:
        # Same logic for narrow W-bottom
        # Here W2 is left bottom, so W1 is right bottom.
        # But where is the neckline? It would be the high between W2 start and W1 start.
        # Since we don't have that granular data easily here, we can use W2 High as a proxy for local high?
        # Or just stick to absolute amplitude for narrow bottom as it's faster.
        # Let's use W2 High as proxy for previous resistance.
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
    风控检查
    Returns: True if Risky, False if Safe
    """
    if not windows:
        return True
        
    w1 = windows[0]
    thresh = get_stock_type_thresholds(instrument)
    
    # 1. Short-term Overheat (短期过热)
    if w1['direction'] == 'UP':
        # [Optimization] Absolute Cap: If W1 surge > 100%, reject immediately (too risky)
        if w1['amplitude'] > 100.0:
            return True
            
        if w1['amplitude'] > thresh['overheat']:
            # Check for continuous acceleration (limit ups)
            if w1['limit_up_count'] >= 4: 
                return True # Risky
            
    # 2. Downtrend (重心下移)
    # Compare W4 High vs W1 High
    if len(windows) >= 4:
        w4 = windows[3]
        if w1['high'] < w4['high'] * 0.7: # Still far below previous high
            # And W1 is weak
            if w1['amplitude'] < 10.0:
                return True # Just a dead cat bounce in a downtrend
    
    return False

def score_stock_micro(df, window_start_date):
    """
    原有的 A->B->C 评分逻辑 (Micro Check)
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
    surge_threshold = thresh['limit_up'] # Use limit up or surge trigger? Original was 12/7.
    # Original logic used 12.0 for CYB and 7.0 for Main. 
    # My new get_stock_type_thresholds returns limit_up as 12/7. So it matches.
    
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
        
        # Risk check (5-day cum return)
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
    logging.info(f"开始执行量化选股 (T日: {t_date or '最新'})")
    
    # 扩大数据获取范围至 150 天
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
        
        # 1. Macro Filter: Dynamic Windows & Pattern Recognition
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
            
        # 2. Risk Check
        is_risky = check_risk(windows, inst)
        if debug_stock:
             logging.info(f"[{inst}] 风控检查: {'高风险' if is_risky else '通过'}")
             
        if is_risky:
            if debug_stock: logging.info(f"[{inst}] 失败: 触发风控")
            continue
            
        # 3. Micro Filter: Score Stock (A->B->C logic)
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
            
            # 只有在非debug模式或者debug模式下才添加结果
            # 其实debug模式下也添加结果没问题，最后打印出来
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
    parser = argparse.ArgumentParser(description='OpenClaw Quant Picker')
    parser.add_argument('--t_date', type=str, default=None, help='指定T日，格式 YYYY-MM-DD')
    parser.add_argument('--top_k', type=int, default=20, help='输出前K名')
    parser.add_argument('--debug_stock', type=str, default=None, help='调试特定股票代码 (e.g. 600759.SH)')
    args = parser.parse_args()
    
    run_quant_picker(t_date=args.t_date, top_k=args.top_k, debug_stock=args.debug_stock)
