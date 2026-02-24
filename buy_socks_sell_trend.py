"""
趋势买入策略
状态机: 无趋势 → 趋势确认 → 回调等待 → 介入 → 持有 → 趋势延续/失效 → 退出 → 无趋势

卖出策略：基于日线结构性低点
- Step 1: 获取本周日线数据
- Step 2: 日线高点不再创新高 → 开始找日线低点
- Step 3: 日线收盘价跌破这个低点 → 趋势失效，清仓
"""

import pandas as pd
from typing import Dict, List, Optional, Tuple
from ana_stocks import (
    get_weekly_data,
    get_daily_data,
    STOCK_CODE_EXPORT as STOCK_CODE,
    BACKTEST_YEARS_EXPORT as BACKTEST_YEARS,
    INITIAL_CAPITAL_EXPORT as INITIAL_CAPITAL,
    get_year_range
)

ATR_MULTIPLIER = 2.5

def detect_trend(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['ma20'] = df['close'].rolling(20).mean()
    df['slope'] = df['ma20'] - df['ma20'].shift(5)
    df['ma_up'] = df['slope'] > 0
    df['trend_up'] = df['ma_up'].rolling(5).sum() == 5
    df['trend_up'] = df['trend_up'].fillna(False).astype(bool)
    return df


def run_backtest(stock_code: str = STOCK_CODE):
    start_year, end_year = get_year_range(BACKTEST_YEARS)
    weekly_data = get_weekly_data(stock_code, days=365 * BACKTEST_YEARS)

    if weekly_data is None or len(weekly_data) < 25:
        print(f"数据不足，需要至少25周数据，当前只有{len(weekly_data) if weekly_data else 0}周")
        return None, {}

    weekly_data = detect_trend(weekly_data)
    weekly_data['date'] = pd.to_datetime(weekly_data['date'])

    daily_data = get_daily_data(stock_code, days=365 * BACKTEST_YEARS)
    if daily_data is None or len(daily_data) < 60:
        print(f"日线数据不足，需要至少60天数据，当前只有{len(daily_data) if daily_data else 0}天")
        daily_data = pd.DataFrame()
    else:
        daily_data['date'] = pd.to_datetime(daily_data['date'])
        daily_data = daily_data.sort_values('date').reset_index(drop=True)
        prev_close = daily_data['收盘'].shift(1)
        tr1 = daily_data['最高'] - daily_data['最低']
        tr2 = (daily_data['最高'] - prev_close).abs()
        tr3 = (daily_data['最低'] - prev_close).abs()
        daily_data['TR'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        daily_data['ATR_14'] = daily_data['TR'].rolling(14).mean()

    cash = INITIAL_CAPITAL
    shares = 0
    trend_shares = 0  # 趋势仓（30%）
    trend_stage_high = None  # 趋势仓独立前高（不会被清除，持续更新）
    buy_count = 0
    sell_count = 0
    trade_history = []
    completed_trades = []
    
    # 仓位分配标志位
    main_position_ratio = 0.7  # 主力仓默认70%
    trend_position_ratio = 0.3  # 趋势仓默认30%

    high_10d = None
    prev_trend_up = None
    avg_atr_20w = None  # 过去20周平均ATR

    consecutive_down_weeks = 0  # 连续下跌周数
    prev_week_down = False  # 记住上一周是否下跌（跨年不重置）
    in_retracement = False  # 是否进入回撤评估状态
    retracement_start_price = None  # 回撤开始前的收盘价
    retracement_start_atr = None  # 回撤开始时的ATR
    retracement_lowest_close = None  # 回撤期间的最低收盘价
    front_high = None  # 前高
    front_high_broken_week = None  # 突破前高的那周索引
    last_retracement_low = None  # L1: 上一轮回撤最低点
    stop_loss_sold_half = 0  # 止损卖出次数（0/1/2，对应30%/30%/40%）
    current_retracement_low = None  # L2: 当前回撤最低点
    has_valid_retracement = False  # 是否已有有效回撤(L1)
    retracement_invalidation_count = 0  # 回撤失效计数：连续不创新高或跌破MA20的周数
    weeks_since_valid_retracement = 0  # 有效回撤后经过的周数（用于冷却）
    weekly_high_price = None  # 用于追踪每周最高价是否创新高

    yearly_summary = {}
    last_year = None
    prev_year_end_asset = INITIAL_CAPITAL

    print(f"\n{'='*80}")
    print(f"股票代码: {stock_code}")
    print(f"回测区间: {start_year} - {end_year} ({BACKTEST_YEARS}年)")
    print(f"起始资金: {INITIAL_CAPITAL:.2f} 元")
    print(f"{'='*80}\n")

    header = f"{'周序号':<5} {'日期':<12} {'收盘价':>10} {'MA20':>10} {'ATR14':>8} {'10日高点':>10} {'趋势':>6} {'持有股数':>10} {'剩余资金':>12} {'总资产':>12} {'状态':<55} {'操作':<20}"
    print(header)
    print("-" * 195)

    for week_idx in range(len(weekly_data)):
        current_week_idx = week_idx + 1
        row = weekly_data.iloc[week_idx]
        current_price = row['close']
        current_date = row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date'])[:10]
        current_year = int(str(current_date)[:4])
        ma20 = row['ma20']
        trend_up = row['trend_up']

        if pd.isna(ma20):
            continue

        if week_idx > 0:
            last_week = weekly_data.iloc[week_idx - 1]
            last_weekly_close = last_week['close']
            last_weekly_high = last_week['high']
        else:
            last_weekly_close = current_price
            last_weekly_high = current_price

        trend_str = "↑" if trend_up else "↓"
        state_info = ""
        operation = ""
        stop_loss_triggered = False  # 本周是否触发止损
        recent_daily_close = current_price
        recent_daily_high = None
        current_atr = None
        atr_value = None

        # 结构判定：有效回撤 + 突破前高 + 低点抬升
        current_weekly_high = row['high']
        current_weekly_low = row['low']

        # 回撤检测：是否进入回撤评估状态
        # 条件1: 连续 ≥2 周收盘价回落
        # 条件2: 任一周跌幅 ≥1 × ATR (需要ATR可用时)
        atr_multiplier = 1.0
        weekly_drop_pct = (last_weekly_close - current_price) / last_weekly_close if last_weekly_close > 0 else 0
        
        # 计算当周ATR（简化：使用周线振幅的一半作为ATR估计）
        weekly_atr_estimate = (current_weekly_high - current_weekly_low) / 2
        
        in_retracement = False
        if shares == 0:
            condition1 = consecutive_down_weeks >= 2  # 连续下跌≥2周
            condition2 = weekly_atr_estimate > 0 and (last_weekly_close - current_price) >= atr_multiplier * weekly_atr_estimate  # 跌幅≥1×ATR
            
            if condition1 or condition2:
                in_retracement = True
                # 如果是刚进入回撤，记录起始信息
                if retracement_start_price is None:
                    retracement_start_price = last_weekly_close
                    retracement_start_atr = weekly_atr_estimate
        
        # 记录回撤过程中的最低点(L2)
        if shares == 0 and in_retracement:
            if current_retracement_low is None or current_weekly_low < current_retracement_low:
                current_retracement_low = current_weekly_low
            if retracement_lowest_close is None or current_price < retracement_lowest_close:
                retracement_lowest_close = current_price
        
        # 连续下跌周统计（跨年连续）
        if shares == 0 and current_price < last_weekly_close:
            consecutive_down_weeks += 1
            prev_week_down = True
        elif shares == 0 and current_price >= last_weekly_close:
            # 回撤结束，检查是否满足有效回撤条件
            valid_retracement = False
            retracement_reason = ""
            
            if prev_week_down and consecutive_down_weeks >= 2:
                # 首次有效回撤只需要满足：回撤幅度 ≥0.8×ATR
                # 后续有效回撤需要满足全部4个条件
                is_first_retracement = not has_valid_retracement
                
                # 条件1: 回撤发生在突破前高之后 (front_high_broken_week已设置)
                # 首次有效回撤不需要此条件
                cond1_pass = front_high_broken_week is not None or is_first_retracement
                
                # 条件2: 回撤最低收盘价未破 MA20
                # 首次有效回撤不需要此条件
                cond2_pass = True if is_first_retracement else (retracement_lowest_close is not None and retracement_lowest_close > ma20)
                
                # 条件3: 回撤幅度 ≥ max(1 × ATR, 5% × 回撤高点)
                cond3_pass = False
                if retracement_start_atr is not None and retracement_start_atr > 0 and current_retracement_low is not None and retracement_start_price is not None:
                    retracement_drop = retracement_start_price - current_retracement_low
                    min_drop = max(1 * retracement_start_atr, 0.05 * retracement_start_price)
                    if retracement_drop >= min_drop:
                        cond3_pass = True
                
                # 条件4: 回撤后出现向上确认 (MA20 slope > 0)
                # 首次有效回撤不需要此条件
                ma20_slope = row['slope'] if 'slope' in row else 0
                cond4_pass = True if is_first_retracement else (ma20_slope > 0)
                
                if cond1_pass and cond2_pass and cond3_pass and cond4_pass:
                    valid_retracement = True
                    retracement_reason = "有效回撤"
                else:
                    reasons = []
                    if not cond1_pass: reasons.append("未突破前高")
                    if not cond2_pass: reasons.append("跌破MA20")
                    if not cond3_pass: reasons.append("幅度不足")
                    if not cond4_pass: reasons.append("未确认向上")
                    retracement_reason = "无效:" + "+".join(reasons)
                
                if valid_retracement:
                    front_high = last_weekly_high  # 记录前高
                    last_retracement_low = current_retracement_low  # L1 = 上轮回撤最低点
                    has_valid_retracement = True  # 标记已有有效回撤
                    weeks_since_valid_retracement = 0  # 重置冷却计数
                    state_info = f"[前高@{front_high:.2f} L1@{last_retracement_low:.2f}]"
                else:
                    # 显示无效原因
                    state_info = f"[{retracement_reason}]"
                
                # 重置回撤记录
                current_retracement_low = None
                retracement_start_price = None
                retracement_start_atr = None
                retracement_lowest_close = None
            
            # 重置连续下跌周计数（在回撤结束后重置）
            consecutive_down_weeks = 0
            prev_week_down = False

        # 有效回撤失效检测：已有有效回撤且已设置前高，且冷却期过后
        if has_valid_retracement and front_high is not None and shares == 0:
            weeks_since_valid_retracement += 1
            
            # 冷却期内不检测失效
            if weeks_since_valid_retracement <= 1:
                # 更新每周最高价追踪
                if weekly_high_price is None or current_weekly_high > weekly_high_price:
                    weekly_high_price = current_weekly_high
            else:
                # 条件1: 连续2周收盘不创新高
                if weekly_high_price is not None and current_weekly_high <= weekly_high_price:
                    retracement_invalidation_count += 1
                else:
                    weekly_high_price = current_weekly_high
                    retracement_invalidation_count = 0
                
                # 条件2: 跌破MA20
                if current_price < ma20:
                    retracement_invalidation_count += 1
                
                # 如果满足失效条件，重置有效回撤状态
                if retracement_invalidation_count >= 2:
                    has_valid_retracement = False
                    front_high = None
                    front_high_broken_week = None
                    last_retracement_low = None
                    state_info = "[回撤失效]"
                    retracement_invalidation_count = 0
        
        # 更新每周最高价追踪
        if shares == 0:
            if weekly_high_price is None or current_weekly_high > weekly_high_price:
                weekly_high_price = current_weekly_high

        if daily_data is not None and len(daily_data) > 0:
            current_year = row['year']
            current_week = row['week']
            
            week_daily_data = daily_data[
                (daily_data['year'] == current_year) &
                (daily_data['week'] == current_week)
            ]

            if len(week_daily_data) > 0:
                recent_daily_high = week_daily_data['最高'].max()
                recent_daily_low = week_daily_data['最低'].min()
                recent_daily_close = week_daily_data['收盘'].iloc[-1]
                current_atr = week_daily_data['ATR_14'].iloc[-1]
                
                # 趋势仓买入检查（在更新前高之前）
                trend_buy_triggered = False
                if trend_shares == 0 and trend_stage_high is not None and current_price > trend_stage_high:
                    ma20_slope = row['slope'] if 'slope' in row else 0
                    if ma20_slope > 0:
                        trend_buy_triggered = True
                
                # 更新趋势仓独立前高（持续更新，不会被清除）
                if trend_stage_high is None or current_price > trend_stage_high:
                    trend_stage_high = current_price
                
                if shares > 0 or trend_shares > 0:
                    current_date_ts = row['date']
                    lookback_start = current_date_ts - pd.Timedelta(days=14)
                    daily_10d = daily_data[
                        (daily_data['date'] >= lookback_start) &
                        (daily_data['date'] <= current_date_ts)
                    ].tail(10)
                    
                    if len(daily_10d) > 0:
                        high_10d = daily_10d['最高'].max()
                        atr_value = daily_10d['ATR_14'].iloc[-1]
                    else:
                        high_10d = recent_daily_high
                        atr_value = current_atr
                    
                    if high_10d is not None and atr_value is not None and not pd.isna(atr_value):
                        sell_price = high_10d - ATR_MULTIPLIER * atr_value
                    else:
                        sell_price = None
                    
                    if high_10d is not None:
                        daily_below_sell = week_daily_data[week_daily_data['收盘'] < sell_price]
                        if len(daily_below_sell) > 0:
                            actual_sell_price = daily_below_sell['收盘'].iloc[0]
                            actual_sell_date = daily_below_sell['date'].iloc[0]
                            state_info = f"[{actual_sell_date.strftime('%m-%d')} 跌破止损]"

        if current_week_idx >= 25:
            # 计算过去20周平均ATR（趋势仓用）
            if week_idx >= 20:
                start_idx = max(0, week_idx - 20)
                recent_20w = weekly_data.iloc[start_idx:week_idx]
                if 'atr_weekly' in recent_20w.columns:
                    avg_atr_20w = recent_20w['atr_weekly'].mean()
            
            # 趋势仓买入执行（使用前面检查好的trend_buy_triggered）
            # 注意：如果本周已触发止损，则不再买入
            if trend_shares == 0 and trend_buy_triggered and not stop_loss_triggered:
                # 如果主力仓已买入，趋势仓使用全部剩余资金；否则使用默认比例
                if shares > 0:
                    trend_position_ratio = 1.0  # 主力仓已买，趋势仓全仓
                    position_desc = "100%"
                else:
                    trend_position_ratio = 0.3  # 默认30%
                    position_desc = "30%"
                
                trend_shares = int((cash * trend_position_ratio) / current_price)
                if trend_shares > 0:
                    cost = trend_shares * current_price
                    cash = cash - cost
                    buy_count += 1
                    operation = f"趋势仓买入{position_desc}(突破前高+MA20上行)"
                    trade_history.append({
                        'week': current_week,
                        'date': current_date,
                        'price': current_price,
                        'shares': trend_shares,
                        'cost': cost,
                        'type': 'BUY_TREND'
                    })
                    high_10d = recent_daily_high  # 趋势仓买入后设置高点
                    state_info = f"[趋势仓{position_desc}]"
            
            # 主力仓买入条件（70%仓位）
            # 注意：如果本周已触发止损，则不再买入
            if shares == 0 and not stop_loss_triggered:
                if trend_up:
                    # 结构判定：必须先有有效回撤，然后突破前高，再满足L2>L1
                    can_buy = True
                    
                    # 条件0: 必须有有效回撤(前高)才能买
                    if not has_valid_retracement or front_high is None:
                        can_buy = False
                    
                    # 条件1: 突破前高
                    if can_buy and front_high is not None:
                        if current_weekly_high > front_high:
                            if front_high_broken_week is None:
                                front_high_broken_week = week_idx  # 只在首次突破时记录
                        elif current_weekly_high <= front_high:
                            can_buy = False
                    
                    # 条件2: 低点抬升(L2 > L1)，仅当已有有效回撤时检查
                    l2_l1_check_passed = True
                    if can_buy and has_valid_retracement:
                        if last_retracement_low is not None and current_retracement_low is not None:
                            if current_retracement_low <= last_retracement_low:
                                can_buy = False
                                l2_l1_check_passed = False
                    
                    # 条件3: 突破前高后的接下来2根周K中，不存在收盘价 < MA20
                    ma20_check_passed = True
                    if can_buy and front_high_broken_week is not None:
                        weeks_after_break = week_idx - front_high_broken_week
                        if 1 <= weeks_after_break <= 2:
                            if current_price < ma20:
                                can_buy = False
                                ma20_check_passed = False
                    
                    # 保存买入条件信息（在重置变量之前）
                    buy_condition_info = {
                        'front_high': front_high,
                        'has_valid_retracement': has_valid_retracement,
                        'last_retracement_low': last_retracement_low,
                        'current_retracement_low': current_retracement_low,
                        'front_high_broken_week': front_high_broken_week,
                        'l2_l1_passed': l2_l1_check_passed,
                        'ma20_passed': ma20_check_passed
                    }
                    
                    if can_buy:
                        # 如果趋势仓已买入，主力仓使用全部剩余资金；否则使用默认70%
                        if trend_shares > 0:
                            main_position_ratio = 1.0  # 趋势仓已买，主力仓全仓
                            position_desc = "100%"
                        else:
                            main_position_ratio = 0.7  # 默认70%
                            position_desc = "70%"
                        
                        new_shares = int((cash * main_position_ratio) / current_price)
                        if new_shares > 0:
                            buy_conditions = []
                            info = buy_condition_info
                            if info['has_valid_retracement']:
                                if info['front_high'] is not None:
                                    buy_conditions.append(f"突破前高{info['front_high']:.2f}")
                                if info['last_retracement_low'] is not None and info['current_retracement_low'] is not None:
                                    buy_conditions.append(f"L2>L1({info['current_retracement_low']:.2f}>{info['last_retracement_low']:.2f})")
                                if info['front_high_broken_week'] is not None:
                                    weeks_after = week_idx - info['front_high_broken_week']
                                    buy_conditions.append(f"突破{weeks_after}周")
                                if info['last_retracement_low'] is not None:
                                    buy_conditions.append(f"L1@{info['last_retracement_low']:.2f}")
                            else:
                                buy_conditions.append("无有效回撤")
                            if not info['l2_l1_passed']:
                                buy_conditions.append("L2≤L1")
                            if not info['ma20_passed']:
                                buy_conditions.append("收盘<MA20")
                            condition_str = '+'.join(buy_conditions) if buy_conditions else '首笔买入'
                            
                            cost = new_shares * current_price
                            cash = cash - cost
                            shares = new_shares
                            buy_count += 1
                            operation = f"买入({condition_str})"
                            high_10d = None
                            front_high = None  # 买入后重置前高
                            front_high_broken_week = None  # 买入后重置突破周索引
                            consecutive_down_weeks = 0
                            last_retracement_low = None
                            current_retracement_low = None
                            has_valid_retracement = False
                            stop_loss_sold_half = 0  # 买入后重置止损状态
                            trade_history.append({
                                'week': current_week,
                                'date': current_date,
                                'price': current_price,
                                'shares': new_shares,
                                'cost': cost,
                                'type': 'BUY'
                            })
            
            # 卖出逻辑：只要有持仓就检查止损
            if (shares > 0 or trend_shares > 0) and high_10d is not None and atr_value is not None and not pd.isna(atr_value):
                sell_price = high_10d - ATR_MULTIPLIER * atr_value
                daily_below_sell = week_daily_data[week_daily_data['收盘'] < sell_price]
                if len(daily_below_sell) > 0:
                    actual_sell_price = daily_below_sell['收盘'].iloc[0]
                    actual_sell_date = daily_below_sell['date'].iloc[0]
                    
                    total_shares = shares + trend_shares
                    revenue = total_shares * actual_sell_price
                    cash += revenue
                    profit = revenue - sum(t['cost'] for t in trade_history if t['type'] in ['BUY', 'BUY_TREND'])
                    sell_count += 1
                    operation = f"{actual_sell_date.strftime('%m-%d')}跌破止损{sell_price:.2f}(高点{high_10d:.2f}-{ATR_MULTIPLIER}*ATR{atr_value:.2f})@{actual_sell_price:.2f} 盈利{profit:.0f}"
                    state_info = f"[{actual_sell_date.strftime('%m-%d')} 跌破止损]"
                    stop_loss_triggered = True  # 标记本周已触发止损
                    
                    sell_trade = {
                        'week': current_week,
                        'date': actual_sell_date.strftime('%Y-%m-%d'),
                        'price': actual_sell_price,
                        'shares': total_shares,
                        'revenue': revenue,
                        'profit': profit,
                        'type': 'SELL'
                    }
                    completed_trades.append(sell_trade)
                    
                    for t in trade_history:
                        if t['type'] in ['BUY', 'BUY_TREND']:
                            t['sold'] = True
                    completed_trades.extend(trade_history)
                    trade_history = []
                    shares = 0
                    trend_shares = 0
                    high_10d = None
                    stop_loss_sold_half = 0
                    # 卖出后重置趋势仓前高（从当前价格重新开始）
                    trend_stage_high = current_price
                    # 卖出后重置仓位比例标志位
                    main_position_ratio = 0.7  # 主力仓默认70%
                    trend_position_ratio = 0.3  # 趋势仓默认30%

        prev_trend_up = trend_up

        total_asset = cash + (shares + trend_shares) * recent_daily_close

        atr_str = f"{current_atr:>8.2f}" if current_atr is not None and not pd.isna(current_atr) else " " * 8
        high_10d_str = f"{high_10d:>10.2f}" if high_10d is not None else " " * 10
        total_shares = shares + trend_shares
        print(f"{current_week:<5} {current_date:<12} {current_price:>10.2f} {ma20:>10.2f} {atr_str} {high_10d_str} {trend_str:>6} {total_shares:>10} {cash:>12.2f} {total_asset:>12.2f} {state_info:<20} {operation:<20}")

        if last_year is None:
            last_year = current_year
            yearly_summary[current_year] = {'start_asset': prev_year_end_asset, 'end_asset': None}

        if current_year != last_year:
            yearly_summary[last_year]['end_asset'] = total_asset
            prev_year_end_asset = total_asset
            yearly_summary[current_year] = {'start_asset': prev_year_end_asset, 'end_asset': None}
            last_year = current_year

        yearly_summary[current_year]['end_asset'] = total_asset

    # 最终清仓：处理主力仓和趋势仓
    total_final_shares = shares + trend_shares
    if total_final_shares > 0:
        last_close = weekly_data.iloc[-1]['close']
        revenue = total_final_shares * last_close
        profit = revenue - sum(t['cost'] for t in trade_history if t['type'] in ['BUY', 'BUY_TREND'])
        cash += revenue  # 累加卖出收入到现有现金
        sell_count += 1
        for t in trade_history:
            if t['type'] in ['BUY', 'BUY_TREND']:
                t['sold'] = True
        final_sell_trade = {
            'week': weekly_data.iloc[-1].name + 1,
            'date': str(weekly_data.iloc[-1]['date'])[:10],
            'price': last_close,
            'shares': total_final_shares,
            'revenue': revenue,
            'profit': profit,
            'type': 'SELL'
        }
        completed_trades.extend(trade_history)
        completed_trades.append(final_sell_trade)
        print(f"{'最后':<6} {str(weekly_data.iloc[-1]['date'])[:10]:<12} {last_close:>10.2f} {weekly_data.iloc[-1]['ma20']:<10.2f} {'清':>8} {0:>10} {cash:>12.2f} {cash:>12.2f} {'清仓 最终':<20} {'清仓 最终':<30}")

    print(f"\n{'='*80}")
    final_asset = cash
    print(f"\n{'='*80}")
    print(f"起始资金: {INITIAL_CAPITAL:.2f} 元")
    print(f"最终资产: {final_asset:.2f} 元")
    print(f"收益率: {(final_asset / INITIAL_CAPITAL - 1) * 100:.2f}%")
    print(f"{'='*80}\n")

    print(f"{'='*60}")
    print(f"买入统计: {buy_count}次")
    print(f"{'='*60}")
    for i, trade in enumerate([t for t in completed_trades if t['type'] == 'BUY'], 1):
        print(f"  第{i}次: {trade['date']} 买入 {trade['shares']}股 @ {trade['price']:.2f}")
    print(f"  总买入次数: {buy_count}")
    print(f"{'='*60}\n")

    print(f"{'='*60}")
    print(f"卖出统计: {sell_count}次")
    print(f"{'='*60}")
    for i, trade in enumerate([t for t in completed_trades if t['type'] == 'SELL'], 1):
        profit_str = f"盈利{trade.get('profit', 0):.0f}" if trade.get('profit', 0) >= 0 else f"亏损{abs(trade.get('profit', 0)):.0f}"
        print(f"  第{i}次: {trade['date']} 卖出 {trade['shares']}股 @ {trade['price']:.2f} ({profit_str})")
    print(f"  总卖出次数: {sell_count}")
    print(f"{'='*60}\n")

    print(f"{'='*60}")
    print("年度收益:")
    print(f"{'='*60}")
    sorted_years = sorted(yearly_summary.keys())
    yearly_returns = {}
    for year in sorted_years:
        if 'end_asset' in yearly_summary[year] and yearly_summary[year]['end_asset'] is not None:
            start = yearly_summary[year]['start_asset']
            end = yearly_summary[year]['end_asset']
            diff = end - start
            pct = (diff / start * 100) if start > 0 else 0
            yearly_returns[year] = pct
            print(f"{year}年: {start:.2f} → {end:.2f} ({diff:+.2f}元, {pct:+.2f}%)")
    print(f"{'='*60}\n")

    total_return = (final_asset / INITIAL_CAPITAL - 1) * 100
    return total_return, yearly_returns


if __name__ == "__main__":
    total_return, yearly_returns = run_backtest()
    print(f"\n汇总结果:")
    print(f"总收益率: {total_return:+.2f}%")
    print(f"年度收益率: {yearly_returns}")
