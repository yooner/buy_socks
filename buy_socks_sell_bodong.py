"""
波动率策略 - 基于波动率变化的交易策略
买入：条件A(波动率连续向0靠近)、条件B(波动率从负变正)、条件C(波动率>1连续天数)
卖出：波动率降低至前一天97%以下
"""

import pandas as pd
import numpy as np
from ana_stocks import (
    get_daily_data,
    STOCK_CODE_EXPORT as STOCK_CODE,
    BACKTEST_YEARS_EXPORT as BACKTEST_YEARS,
    get_year_range
)

# 卖出策略全局参数
SELL_RATIO_THRESHOLD = 0.97  # 波动率降至前一天97%以下全卖

# 连续低于0卖出开关
ENABLE_SELL_CONSECUTIVE_BELOW_ZERO = True  # 设置为False可关闭此卖出条件
SELL_CONSECUTIVE_BELOW_ZERO_DAYS = 7  # 持有后连续10天波动率<0卖出

# 条件C全局开关
ENABLE_CONDITION_C = True  # 设置为False可关闭条件C买入

# 买入条件全局参数
BUY_DECLINE_DAYS_REQUIRED = 3  # 波动率连续向0靠近所需天数（条件A）

# 条件B参数
BUY_CONDITION_B_DIFF = 0.3     # 波动率从负变正的最小差值

# 条件C参数
BUY_CONDITION_C_DAYS = 4       # 波动率>1的连续天数要求
BUY_CONDITION_C_VOL_THRESHOLD = 0.85  # 波动率阈值（用于计数）


def calculate_slope_atr(df, ma_period=20, atr_period=14, n=5):
    """
    计算 MA20 的 ATR 归一化斜率（波动率）。
    公式: (MA20[t] - MA20[t-n]) / ATR[t]
    """
    # 计算 MA20
    df['MA20'] = df['收盘'].rolling(window=ma_period, min_periods=ma_period).mean()

    # 计算 ATR
    df['prev_close'] = df['收盘'].shift(1)
    df['tr1'] = df['最高'] - df['最低']
    df['tr2'] = (df['最高'] - df['prev_close']).abs()
    df['tr3'] = (df['最低'] - df['prev_close']).abs()
    df['TR'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
    df['ATR'] = df['TR'].rolling(window=atr_period, min_periods=atr_period).mean()

    # 计算 MA20 在 n 天内的变动
    df['MA20_shift'] = df['MA20'].shift(n)
    df['MA20_change'] = df['MA20'] - df['MA20_shift']

    # 计算归一化斜率
    df['波动率'] = df['MA20_change'] / df['ATR'].replace(0, np.nan)

    # 删除中间辅助列
    df.drop(columns=['prev_close', 'tr1', 'tr2', 'tr3', 'TR', 'MA20_shift', 'MA20_change'], inplace=True)

    return df


def run_backtest(stock_code: str = STOCK_CODE):
    """回测主函数"""
    start_year, end_year = get_year_range(BACKTEST_YEARS)
    
    # 获取日线数据
    df = get_daily_data(stock_code, days=365 * BACKTEST_YEARS + 100)
    
    if df is None or len(df) < 60:
        print(f"数据不足，需要至少60天数据，当前只有{len(df) if df is not None else 0}天")
        return None
    
    # 按日期从远到近排序
    df = df.sort_values('date').reset_index(drop=True)
    
    # 只取最近一年的数据
    days_to_show = 365 * BACKTEST_YEARS
    df = df.tail(days_to_show).reset_index(drop=True)
    
    # 计算技术指标
    df['ma20'] = df['收盘'].rolling(window=20, min_periods=20).mean()
    
    # 计算ATR
    prev_close = df['收盘'].shift(1)
    tr1 = df['最高'] - df['最低']
    tr2 = (df['最高'] - prev_close).abs()
    tr3 = (df['最低'] - prev_close).abs()
    df['tr'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df['atr14'] = df['tr'].rolling(window=14, min_periods=14).mean()
    
    # 计算波动率
    df = calculate_slope_atr(df, ma_period=20, atr_period=14, n=5)
    
    # 初始化交易变量
    initial_capital = 100000
    cash = initial_capital
    position = 0
    buy_price = 0
    trades = []
    trade_count = 0
    
    # 买入条件计数器
    volatility_declining_days = 0  # 波动率连续向0靠近天数（数值变大）
    prev_volatility = None         # 前一天波动率
    volatility_above_one_days = 0  # 波动率在1以上的连续天数
    volatility_below_zero_days = 0 # 波动率<0的连续天数
    is_condition_c_trade = False   # 标记是否为条件C买入的交易
    
    # 卖出条件计数器（从买入日开始）
    hold_below_zero_days = 0       # 持有后连续波动率<0的天数
    
    # 打印表头
    print(f"\n{'='*130}")
    print(f"股票代码: {stock_code}")
    print(f"回测区间: {start_year} - {end_year} ({BACKTEST_YEARS}年)")
    print(f"起始资金: {initial_capital:,.2f}")
    print(f"买入条件A: (连续向0靠近{BUY_DECLINE_DAYS_REQUIRED}天且波动率<0) - 全仓买入")
    print(f"买入条件B: (波动率从负变正且相差>{BUY_CONDITION_B_DIFF}) - 全仓买入")
    print(f"买入条件C: (波动率>{BUY_CONDITION_C_VOL_THRESHOLD}连续第{BUY_CONDITION_C_DAYS}天) - 全仓买入")
    print(f"卖出条件: 波动率>0且降低时，降至前一天{SELL_RATIO_THRESHOLD*100:.0f}%以下则全卖；条件C买入需等>{BUY_CONDITION_C_VOL_THRESHOLD}天数归0才卖")
    print(f"{'='*130}\n")
    
    header = f"{'日':<5} {'日期':<12} {'收盘':>8} {'MA20':>8} {'ATR14':>8} {'波动率':>8} {'靠近天数':>6} {'<0天数':>6} {'>1天数':>6} {'操作':<12} {'持仓':>8} {'市值':>12}"
    print(header)
    print("-" * 130)
    
    # 遍历每一天进行回测
    for i in range(len(df)):
        row = df.iloc[i]
        day_num = i + 1
        date_str = row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date'])[:10]
        close_price = row['收盘']
        ma20 = row['ma20']
        volatility = row['波动率']
        
        action = ""
        
        # 确保数据有效
        if pd.notna(volatility):
            # 判断波动率变化（在更新prev_volatility之前判断）
            is_volatility_declining = False
            is_volatility_increasing_toward_zero = False  # 波动率向0靠近（数值变大）
            if prev_volatility is not None:
                if volatility > prev_volatility:
                    # 判断是否在向0靠近（当前和前一期都为负，且当前更大/更接近0）
                    if volatility < 0 and prev_volatility < 0:
                        is_volatility_increasing_toward_zero = True
                elif volatility < prev_volatility:
                    is_volatility_declining = True
            
            # 更新波动率在阈值以上的连续天数（必须在卖出判断之前更新）
            if volatility > BUY_CONDITION_C_VOL_THRESHOLD:
                volatility_above_one_days += 1
            else:
                volatility_above_one_days = 0
            
            # 更新波动率<0的连续天数
            if volatility < 0:
                volatility_below_zero_days += 1
            else:
                volatility_below_zero_days = 0
            
            # 条件B：波动率从负值变向正值，且两者相差>配置值
            # 必须在更新prev_volatility之前判断
            condition_b = False
            if prev_volatility is not None and prev_volatility < 0 and volatility > 0:
                volatility_diff = volatility - prev_volatility
                if volatility_diff > BUY_CONDITION_B_DIFF:
                    condition_b = True
            
            # 持有后更新连续波动率<0天数计数器
            if position > 0 and not is_condition_c_trade:
                if volatility < 0:
                    hold_below_zero_days += 1
                else:
                    hold_below_zero_days = 0
            
            # 卖出策略
            should_sell = False
            sell_reason = ""
            
            if is_condition_c_trade and position > 0:
                # 条件C买入的交易：只要volatility_above_one_days归0就卖出（不判断波动率是否降低）
                if volatility_above_one_days == 0:
                    should_sell = True
                    sell_reason = "C条件"
            elif position > 0 and not is_condition_c_trade:
                # 普通卖出条件1：波动率>0且降低，且降至前一天97%以下
                if volatility > 0 and is_volatility_declining:
                    volatility_ratio = volatility / prev_volatility if prev_volatility > 0 else 1.0
                    if volatility_ratio <= SELL_RATIO_THRESHOLD:
                        should_sell = True
                        sell_reason = "比率卖出"
                
                # 普通卖出条件2：持有后连续10天波动率<0（仅在开关打开时启用）
                if ENABLE_SELL_CONSECUTIVE_BELOW_ZERO and hold_below_zero_days >= SELL_CONSECUTIVE_BELOW_ZERO_DAYS:
                    should_sell = True
                    sell_reason = f"连续{SELL_CONSECUTIVE_BELOW_ZERO_DAYS}天<0"
            
            # 执行卖出（全卖）
            if should_sell and position > 0:
                sell_price = close_price
                sell_value = position * sell_price
                profit = (sell_price - buy_price) * position
                cash += sell_value
                action = f"卖出@{sell_price:.2f}({sell_reason})"
                trades.append({
                    'day': day_num,
                    'date': date_str,
                    'action': '卖出',
                    'price': sell_price,
                    'shares': position,
                    'profit': profit
                })
                position = 0
                buy_price = 0
                is_condition_c_trade = False  # 重置条件C标记
                # 卖出后重置计数器
                volatility_declining_days = 0
                hold_below_zero_days = 0  # 重置持有后波动率<0计数器
            
            # 更新前一天的波动率
            prev_volatility = volatility
            
            # 买入条件判断
            # 条件：波动率为负，连续向0靠近（数值变大）
            if is_volatility_increasing_toward_zero:
                volatility_declining_days += 1
            else:
                volatility_declining_days = 0
            
            # 条件A：连续向0靠近指定天数，且当天波动率<0（负值区间）
            condition_a = (volatility_declining_days >= BUY_DECLINE_DAYS_REQUIRED and 
                          volatility < 0)
            
            # 条件C：波动率>阈值连续指定天数（仅在开关打开时启用）
            condition_c = ENABLE_CONDITION_C and volatility_above_one_days >= BUY_CONDITION_C_DAYS
            
            # 买入逻辑（只有在没有持仓时才买入）
            if position == 0:
                # 条件C：全仓买入
                if condition_c:
                    buy_price = close_price
                    new_position = int(cash / buy_price)
                    if new_position > 0:
                        position = new_position
                        cost = position * buy_price
                        cash -= cost
                        trade_count += 1
                        is_condition_c_trade = True
                        action = f"买入C@{buy_price:.2f}"
                        trades.append({
                            'day': day_num,
                            'date': date_str,
                            'action': '买入',
                            'price': buy_price,
                            'shares': position,
                            'is_condition_c': True
                        })
                        volatility_declining_days = 0
                        # 初始化持有后波动率<0计数器
                        hold_below_zero_days = 1 if volatility < 0 else 0
                
                # 条件A或B：一次性全仓买入
                elif (condition_a or condition_b):
                    buy_price = close_price
                    new_position = int(cash / buy_price)
                    if new_position > 0:
                        position = new_position
                        cost = position * buy_price
                        cash -= cost
                        trade_count += 1
                        is_condition_c_trade = False
                        action = f"买入@{buy_price:.2f}"
                        trades.append({
                            'day': day_num,
                            'date': date_str,
                            'action': '买入',
                            'price': buy_price,
                            'shares': position,
                            'is_condition_c': False
                        })
                        volatility_declining_days = 0
                        # 初始化持有后波动率<0计数器
                        hold_below_zero_days = 1 if volatility < 0 else 0
        
        # 计算当前市值
        market_value = cash + position * close_price if position > 0 else cash
        position_str = f"{position}" if position > 0 else "0"
        
        # 数据显示
        ma20_str = f"{ma20:.2f}" if pd.notna(ma20) else "N/A"
        atr14_str = f"{row['atr14']:.2f}" if pd.notna(row['atr14']) else "N/A"
        volatility_str = f"{volatility:.2f}" if pd.notna(volatility) else "N/A"
        
        # 显示变小天数（只有在没有持仓时才显示，格式: 当前天数/目标天数）
        if position == 0:
            if volatility_declining_days > 0:
                display_declining_days_str = f"{volatility_declining_days}/{BUY_DECLINE_DAYS_REQUIRED}"
            else:
                display_declining_days_str = "0"
        else:
            display_declining_days_str = "-"
        
        print(f"{day_num:<5} {date_str:<12} {close_price:>8.2f} {ma20_str:>8} {atr14_str:>8} {volatility_str:>8} {display_declining_days_str:>6} {volatility_below_zero_days:>6} {volatility_above_one_days:>6} {action:<12} {position_str:>8} {market_value:>12,.2f}")
    
    # 计算最终收益
    final_value = cash + position * df.iloc[-1]['收盘'] if position > 0 else cash
    final_profit = final_value - initial_capital
    
    # 统计条件C交易数据
    condition_c_count = 0
    condition_c_loss_count = 0
    condition_c_total_profit = 0
    
    if trades:
        for idx, trade in enumerate(trades):
            if trade.get('is_condition_c') and trade['action'] == '买入':
                condition_c_count += 1
                # 找到对应的卖出交易
                for sell_trade in trades[idx+1:]:
                    if sell_trade['action'] == '卖出':
                        profit = sell_trade.get('profit', 0)
                        condition_c_total_profit += profit
                        if profit < 0:
                            condition_c_loss_count += 1
                        break
    
    print(f"\n{'='*130}")
    print(f"回测结果统计")
    print(f"{'='*130}")
    print(f"买卖次数: {trade_count}")
    print(f"起始资金: {initial_capital:,.2f}")
    print(f"最终资金: {final_value:,.2f}")
    print(f"总盈利: {final_profit:,.2f}")
    print(f"收益率: {(final_profit/initial_capital)*100:.2f}%")
    print(f"\n条件C交易统计:")
    print(f"  买入次数: {condition_c_count}")
    print(f"  亏损次数: {condition_c_loss_count}")
    print(f"  总盈利: {condition_c_total_profit:,.2f}")
    
    if trades:
        print(f"\n交易明细:")
        print(f"{'序号':<6} {'日期':<6} {'操作':<6} {'价格':>10} {'股数':>10} {'盈亏':>12} {'盈亏%':>8}")
        print("-" * 80)
        for idx, trade in enumerate(trades, 1):
            profit_str = f"{trade.get('profit', 0):,.2f}" if 'profit' in trade else "-"
            # 计算盈亏百分比
            if 'profit' in trade and trade['action'] == '卖出':
                # 找到对应的买入交易
                buy_trade = None
                for t in trades[:idx-1]:
                    if t['action'] == '买入':
                        buy_trade = t
                if buy_trade:
                    profit_pct = (trade.get('profit', 0) / (buy_trade['price'] * buy_trade['shares'])) * 100
                    profit_pct_str = f"{profit_pct:+.2f}%"
                else:
                    profit_pct_str = "-"
            else:
                profit_pct_str = "-"
            print(f"{idx:<6} {trade['date']:<12} {trade['action']:<6} {trade['price']:>10.2f} {trade['shares']:>10} {profit_str:>12} {profit_pct_str:>8}")
    
    print(f"{'='*130}")
    
    # 计算总收益率和年度收益率
    total_return = (final_profit / initial_capital) * 100 if initial_capital > 0 else 0
    
    yearly_returns = {}
    if trades:
        for trade in trades:
            if 'profit' in trade and 'date' in trade:
                year = int(trade['date'][:4])
                if year not in yearly_returns:
                    yearly_returns[year] = 0
                yearly_returns[year] += trade['profit']
    
    for year in yearly_returns:
        yearly_returns[year] = (yearly_returns[year] / initial_capital) * 100
    
    return total_return, yearly_returns, {'trades': trades, 'final_value': final_value}


if __name__ == "__main__":
    run_backtest()
