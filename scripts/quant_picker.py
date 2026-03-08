import os
import sys
import argparse
import pandas as pd
import numpy as np
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.db_session import engine

def get_trading_days(t_date=None, count=50):
    """获取T日及之前的交易日历"""
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

def score_stock(df, window_start_date):
    """
    对单个股票的K线数据进行打分，严格按照 A、B、C 及涨幅天花板逻辑
    df: 包含多日数据的 DataFrame，已按时间升序
    window_start_date: 观察窗口的起始日期 (通常是 T-66 或 T-22 等)
    """
    df = df.reset_index(drop=True)
    if len(df) < 10:
        return 0, {}
        
    # 计算 MA25
    df['ma25'] = df['close'].rolling(window=25).mean()
    # 计算 MA(V, 5) 用于判断放量
    df['vol_ma5'] = df['volume'].rolling(window=5).mean()
    
    # 截取观察窗口数据
    df_window = df[df['date'] >= window_start_date].reset_index(drop=True)
    if len(df_window) < 5:
        return 0, {}
        
    instrument = df_window['instrument'].iloc[0]
    stock_name = df_window['stock_name'].iloc[0] if 'stock_name' in df_window.columns else ""
    is_cyb = instrument.startswith('300') or instrument.startswith('301') or instrument.startswith('688')
    
    surge_threshold = 12.0 if is_cyb else 7.0
    risk_threshold = 60.0 if is_cyb else 35.0
    
    best_score = 0
    best_detail = {}
    
    # 为了避免嵌套过深，我们先找出所有符合 "节点A (放量大涨)" 的候选日
    # 条件：单日最高涨幅 >= 阈值，且当日成交量 >= 前5日均量 * 1.5
    surge_candidates = []
    for i in range(len(df_window)):
        row = df_window.iloc[i]
        # 注意：这里用户写的是“最高涨幅”，如果是指收盘涨幅，用 change_ratio；如果是盘中触及，可能需要用 (high / pre_close - 1)*100
        # 稳妥起见，这里按最高涨幅计算（假设有 pre_close 字段，如果没有则用前一日 close）
        if i > 0:
            pre_close = df_window.iloc[i-1]['close']
            high_pct = (row['high'] / pre_close - 1) * 100
            close_pct = (row['close'] / pre_close - 1) * 100
        else:
            # 如果是窗口第一天，我们去原 df 找前一天
            orig_idx = df[df['date'] == row['date']].index[0]
            if orig_idx > 0:
                pre_close = df.iloc[orig_idx-1]['close']
                high_pct = (row['high'] / pre_close - 1) * 100
                close_pct = (row['close'] / pre_close - 1) * 100
            else:
                continue

        # 判断涨幅和放量
        if high_pct >= surge_threshold and row['volume'] >= row['vol_ma5'] * 1.5:
            # 优先条件：连板或连续2天大涨加分
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

    # 遍历每个攻击浪起点 (节点 A)
    for surge in surge_candidates:
        idx_A = surge['window_idx']
        
        # 风险控制：「涨幅天花板」过滤器
        # 计算基准：从 Date_A 的前一天（起涨前）开始计算往前倒推5个交易日
        orig_idx_A = df[df['date'] == surge['date']].index[0]
        idx_A_prev = max(0, orig_idx_A - 1)
        start_idx = max(0, idx_A_prev - 5)
        
        if idx_A_prev > start_idx:
            p_close = df.iloc[idx_A_prev]['close']
            s_close = df.iloc[start_idx]['close']
            if s_close > 0:
                cum_return = (p_close / s_close - 1) * 100
            else:
                cum_return = 0
        else:
            cum_return = 0
            
        if cum_return > risk_threshold:
            continue # 超过天花板，过滤掉
            
        # 寻找节点 B：「深度回调」（必须发生在 Date_A 之后）
        for idx_B in range(idx_A + 1, len(df_window)):
            row_B = df_window.iloc[idx_B]
            
            # 判断是否有效洗盘（满足任意一条）
            is_valid_pullback = False
            pullback_score = 0
            
            # 条件1：连续 1-5 根阴线 (这里简单判断为当前是阴线，且可能前面也是阴线)
            consecutive_yin = 0
            for j in range(idx_B, idx_A, -1):
                if df_window.iloc[j]['close'] < df_window.iloc[j]['open']: # 或者 close < pre_close
                    consecutive_yin += 1
                else:
                    break
            if consecutive_yin >= 1:
                is_valid_pullback = True
                pullback_score += consecutive_yin * 2
                
            # 条件2：大阴线或跌停 (单日跌幅 >= 5%)
            pre_B_close = df_window.iloc[idx_B-1]['close']
            pct_B = (row_B['close'] / pre_B_close - 1) * 100
            if pct_B <= -5.0:
                is_valid_pullback = True
                pullback_score += 5
                
            # 条件3：破位均线
            if row_B['close'] < row_B['ma25']:
                is_valid_pullback = True
                pullback_score += 5
                
            if not is_valid_pullback:
                continue
                
            # 寻找节点 C：「快速回抽」
            # 必须在 Date_B 之后的 3 个交易日内发生
            for idx_C in range(idx_B + 1, min(idx_B + 4, len(df_window))):
                row_C = df_window.iloc[idx_C]
                pre_C_close = df_window.iloc[idx_C-1]['close']
                pct_C = (row_C['close'] / pre_C_close - 1) * 100
                
                is_valid_rebound = False
                rebound_score = 0
                
                # 强度条件1：涨停确认
                # 简单判断涨跌停，创业板20%，主板10%
                limit_up_pct = 19.5 if is_cyb else 9.5
                if pct_C >= limit_up_pct:
                    is_valid_rebound = True
                    rebound_score += 20
                    
                # 强度条件2：跳空缺口
                if row_C['open'] > pre_C_close * 1.02 and row_C['low'] > pre_C_close:
                    is_valid_rebound = True
                    rebound_score += 15
                    
                # 强度条件3：大阳反攻
                if pct_C >= 5.0:
                    # 计算前3日跌幅
                    idx_C_minus_3 = max(idx_A, idx_C - 3)
                    price_C_minus_3 = df_window.iloc[idx_C_minus_3]['close']
                    if price_C_minus_3 > pre_C_close: # 确实有跌
                        drop_amount = price_C_minus_3 - pre_C_close
                        recover_amount = row_C['close'] - pre_C_close
                        if recover_amount >= drop_amount * 0.5:
                            is_valid_rebound = True
                            rebound_score += 10
                            
                if not is_valid_rebound:
                    continue
                    
                # 强度条件4：回抽幅度检验 (非常重要)
                # 要求回抽不仅要快，还要有力，我们比较回抽后（Date_C及其后）的最高点是否能摸到或者突破前高（Date_A的收盘价）
                # 为了不局限于Date_C当天，我们可以看Date_C及其后3天内的最高价
                max_rebound_idx = min(idx_C + 3, len(df_window) - 1)
                rebound_peak_price = df_window.iloc[idx_C:max_rebound_idx+1]['high'].max()
                
                # 比较 rebound_peak_price 和 surge_close
                surge_close = surge['close']
                if rebound_peak_price >= surge_close:
                    rebound_score += 20 # 突破前高，极其强势
                elif rebound_peak_price >= surge_close * 0.95:
                    rebound_score += 10 # 几乎摸到前高
                elif rebound_peak_price < surge_close * 0.8:
                    rebound_score -= 20 # 弱反弹，减分甚至淘汰
                
                if rebound_score < 0:
                    continue # 弱反抽直接淘汰
                    
                # 综合评分计算
                score_surge = 10 + (5 if surge['is_continuous'] else 0)
                score_risk = 20 * (1 - max(0, cum_return) / risk_threshold)
                
                total_score = score_surge + pullback_score + rebound_score + score_risk
                
                if total_score > best_score:
                    best_score = total_score
                    best_detail = {
                        'instrument': instrument,
                        'name': stock_name,
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

def run_quant_picker(t_date=None, top_k=20):
    print(f"开始执行量化选股 (T日: {t_date or '最新'})")
    
    # 获取过去3个窗口（约66个交易日）作为数据区间
    # 窗口1: T-22 到 T
    # 窗口2: T-44 到 T-22
    # 窗口3: T-66 到 T-44
    windows_count = 3
    days_per_window = 22
    total_days_needed = windows_count * days_per_window + 50 # 多取50天为了计算均线和风控起点
    
    dates = get_trading_days(t_date, count=total_days_needed)
    if len(dates) < 50:
        print("数据不足，无法计算均线或执行滑动窗口。")
        return
        
    start_date = dates[0]
    end_date = dates[-1]
    
    print(f"提取数据总区间: {start_date} 到 {end_date} (支持 {windows_count} 个滑动窗口的深度搜索)")
    
    df_all = get_kline_data(start_date, end_date)
    if df_all.empty:
        print("未获取到K线数据。")
        return
        
    # 预处理：将日期转为字符串对比
    df_all['date'] = pd.to_datetime(df_all['date']).dt.strftime('%Y-%m-%d')
    
    results = []
    grouped = df_all.groupby('instrument')
    total = len(grouped)
    count = 0
    
    for inst, group in grouped:
        count += 1
        if count % 500 == 0:
            print(f"处理进度: {count}/{total}")
            
        group_df = group.copy()
        best_stock_score = 0
        best_stock_detail = {}
        
        # 按照滑动窗口从近到远搜索
        # 如果最近的窗口找到了符合特征的形态，直接采用，否则往历史窗口追溯
        for w in range(windows_count):
            # dates 是按时间从早到晚排序的，dates[-1] 是T日
            # w=0: start_idx = -23 (对应T-22, 假设今天包含在内)
            # w=1: start_idx = -45 (对应T-44)
            # w=2: start_idx = -67 (对应T-66)
            start_offset = -(w + 1) * days_per_window - 1
            if abs(start_offset) > len(dates):
                break
                
            window_start_date = dates[start_offset]
            
            score, detail = score_stock(group_df, window_start_date)
            
            if score > 0:
                # 给越近的窗口加权，鼓励近期发生的异动
                # 比如窗口1不减分，窗口2减10分，窗口3减20分
                time_decay_penalty = w * 10 
                score = max(1, score - time_decay_penalty)
                detail['total_score'] = score
                detail['found_in_window'] = w + 1
                
                if score > best_stock_score:
                    best_stock_score = score
                    best_stock_detail = detail
                
                # 如果在近期窗口找到了高分（比如>50分），就不再往历史挖了，避免选出太陈旧的形态
                if best_stock_score > 50:
                    break
                    
        if best_stock_score > 0:
            results.append(best_stock_detail)
            
    # 排序并取 Top-K
    results.sort(key=lambda x: x['total_score'], reverse=True)
    top_results = results[:top_k]
    
    print(f"\n--- 选股完成，TOP-{len(top_results)} ---")
    for i, res in enumerate(top_results, 1):
        window_str = f"窗口{res['found_in_window']}"
        print(f"{i:02d}. [{window_str}] {res['instrument']} {res['name']} | 总分: {res['total_score']} "
              f"(大涨:{res['surge_date']}, 见底:{res['bottom_date']}, 回抽:{res['rebound_date']}) "
              f"| 5日风控: {res['cum_return_5d']}%")
              
    # 保存结果到 JSON
    if not os.path.exists('results'):
        os.makedirs('results')
        
    out_file = f"results/top_{top_k}_stocks_{end_date}.json"
    pd.DataFrame(top_results).to_json(out_file, orient='records', force_ascii=False, indent=2)
    print(f"\n结果已保存至 {out_file}")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='OpenClaw Quant Picker')
    parser.add_argument('--t_date', type=str, default=None, help='指定T日，格式 YYYY-MM-DD')
    parser.add_argument('--top_k', type=int, default=20, help='输出前K名')
    args = parser.parse_args()
    
    run_quant_picker(t_date=args.t_date, top_k=args.top_k)
