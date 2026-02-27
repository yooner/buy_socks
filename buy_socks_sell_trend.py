"""
趋势买入策略 - MA20上穿MA60持续3天买入，跌破MA20卖出
卖出后重新计算MA20超过MA60的持续天数
"""

import pandas as pd
import numpy as np
from ana_stocks import (
    get_daily_data,
    STOCK_CODE_EXPORT as STOCK_CODE,
    BACKTEST_YEARS_EXPORT as BACKTEST_YEARS,
    get_year_range
)


def calculate_slope_atr(df, ma_period=20, atr_period=14, n=5):
    """
    计算 MA20 的 ATR 归一化斜率（波动率）。
    公式: (MA20[t] - MA20[t-n]) / ATR[t]

    参数:
    df : DataFrame, 必须包含 '最高', '最低', '收盘' 列
    ma_period : int, MA20 的计算周期 (默认 20)
    atr_period : int, ATR 的计算周期 (默认 14)
    n : int, 计算斜率时的时间间隔 (默认 5)

    返回:
    DataFrame, 包含原始列以及新增的 'MA20', 'ATR', '波动率' 列
    """
    # 计算 MA20
    df['MA20'] = df['收盘'].rolling(window=ma_period, min_periods=ma_period).mean()

    # 计算 ATR (平均真实波幅)
    df['prev_close'] = df['收盘'].shift(1)
    df['tr1'] = df['最高'] - df['最低']
    df['tr2'] = (df['最高'] - df['prev_close']).abs()
    df['tr3'] = (df['最低'] - df['prev_close']).abs()
    df['TR'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
    df['ATR'] = df['TR'].rolling(window=atr_period, min_periods=atr_period).mean()

    # 计算 MA20 在 n 天内的变动
    df['MA20_shift'] = df['MA20'].shift(n)
    df['MA20_change'] = df['MA20'] - df['MA20_shift']

    # 计算归一化斜率 (避免除以 0)
    df['波动率'] = df['MA20_change'] / df['ATR'].replace(0, np.nan)

    # 删除中间辅助列，保留主要结果
    df.drop(columns=['prev_close', 'tr1', 'tr2', 'tr3', 'TR', 'MA20_shift', 'MA20_change'], inplace=True)

    return df


def run_backtest(stock_code: str = STOCK_CODE):
    """回测主函数"""
    start_year, end_year = get_year_range(BACKTEST_YEARS)
    
    # 获取日线数据，需要更多历史数据来计算MA60
    df = get_daily_data(stock_code, days=365 * BACKTEST_YEARS + 100)
    
    if df is None or len(df) < 60:
        print(f"数据不足，需要至少60天数据，当前只有{len(df) if df is not None else 0}天")
        return None
    
    # 按日期从远到近排序
    df = df.sort_values('date').reset_index(drop=True)
    
    # 只取最近一年的数据用于回测显示
    days_to_show = 365 * BACKTEST_YEARS
    df = df.tail(days_to_show).reset_index(drop=True)
    
    # 计算技术指标
    df['ma20'] = df['收盘'].rolling(window=20, min_periods=20).mean()
    df['ma60'] = df['收盘'].rolling(window=60, min_periods=60).mean()
    
    # 计算ATR
    prev_close = df['收盘'].shift(1)
    tr1 = df['最高'] - df['最低']
    tr2 = (df['最高'] - prev_close).abs()
    tr3 = (df['最低'] - prev_close).abs()
    df['tr'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df['atr14'] = df['tr'].rolling(window=14, min_periods=14).mean()
    
    # 计算波动率（MA20的ATR归一化斜率）
    df = calculate_slope_atr(df, ma_period=20, atr_period=14, n=5)
    
    # 初始化交易变量
    initial_capital = 100000  # 起始资金
    cash = initial_capital    # 可用现金
    position = 0              # 持仓股数
    buy_price = 0             # 买入价格
    trades = []               # 交易记录
    trade_count = 0           # 交易次数
    total_profit = 0          # 总盈利
    
    # 用于动态计算满足买入条件的持续天数
    # 条件：MA20 > MA60 且 收盘价 > MA20 且 ATR连续增长 且 波动率连续增长
    consecutive_valid_days = 0  # 持续满足条件的天数计数器
    atr_growing_days = 0        # ATR连续增长天数计数器
    volatility_growing_days = 0 # 波动率连续增长天数计数器
    prev_atr = None             # 前一天的ATR值
    prev_volatility = None      # 前一天的波动率值
    
    # 打印表头
    print(f"\n{'='*140}")
    print(f"股票代码: {stock_code}")
    print(f"回测区间: {start_year} - {end_year} ({BACKTEST_YEARS}年)")
    print(f"起始资金: {initial_capital:,.2f}")
    print(f"买入条件: 连续3天满足(MA20>MA60且收盘价>MA20且ATR递增且波动率>0.5且波动率递增)，第3天收盘价买入")
    print(f"卖出条件: 收盘价跌破MA20")
    print(f"{'='*140}\n")
    
    header = f"{'日':<5} {'日期':<6} {'开盘':>8} {'最高':>8} {'最低':>8} {'收盘':>8} {'MA20':>8} {'MA60':>8} {'ATR14':>8} {'波动率':>8} {'持续':>4} {'操作':<10} {'持仓':>8} {'市值':>12}"
    print(header)
    print("-" * 140)
    
    # 遍历每一天进行回测
    for i in range(len(df)):
        row = df.iloc[i]
        day_num = i + 1
        date_str = row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date'])[:10]
        close_price = row['收盘']
        ma20 = row['ma20']
        ma60 = row['ma60']
        
        action = ""  # 当天操作
        
        # 确保MA20和MA60有效
        atr14 = row['atr14']
        volatility = row['波动率']
        if pd.notna(ma20) and pd.notna(ma60) and pd.notna(atr14) and pd.notna(volatility):
            # 判断当天是否满足买入条件：MA20 > MA60 且 收盘价 > MA20
            is_valid = (ma20 > ma60) and (close_price > ma20)
            
            # 判断ATR是否增长
            is_atr_growing = False
            if prev_atr is not None and atr14 > prev_atr:
                is_atr_growing = True
            
            # 判断波动率是否增长
            is_volatility_growing = False
            if prev_volatility is not None and volatility > prev_volatility:
                is_volatility_growing = True
            
            # 更新持续天数（只有在没有持仓时才计数）
            if position == 0:
                if is_valid:
                    consecutive_valid_days += 1
                else:
                    consecutive_valid_days = 0
                
                # 更新ATR连续增长天数
                if is_atr_growing:
                    atr_growing_days += 1
                else:
                    atr_growing_days = 0
                
                # 更新波动率连续增长天数
                if is_volatility_growing:
                    volatility_growing_days += 1
                else:
                    volatility_growing_days = 0
            
            # 更新前一天的ATR值和波动率值
            prev_atr = atr14
            prev_volatility = volatility
            
            # 买入条件：连续3天满足条件(MA20>MA60且收盘价>MA20且ATR递增且波动率>0.5且波动率递增)，且当前没有持仓
            is_volatility_ok = True
            if position == 0 and consecutive_valid_days >= 3 and atr_growing_days >= 2 and volatility_growing_days >= 2 and is_volatility_ok:
                # 使用当天收盘价买入
                buy_price = close_price
                position = int(cash / buy_price)  # 买入股数（整数）
                if position > 0:
                    cost = position * buy_price
                    cash -= cost
                    trade_count += 1
                    action = f"买入@{buy_price:.2f}"
                    trades.append({
                        'day': day_num,
                        'date': date_str,
                        'action': '买入',
                        'price': buy_price,
                        'shares': position
                    })
                    # 买入后重置持续天数
                    consecutive_valid_days = 0
                    atr_growing_days = 0
                    volatility_growing_days = 0
            
            # 卖出条件：收盘价跌破MA20，且当前有持仓
            elif close_price < ma20 and position > 0:
                sell_price = close_price
                sell_value = position * sell_price
                profit = (sell_price - buy_price) * position
                total_profit += profit
                cash += sell_value
                action = f"卖出@{sell_price:.2f}"
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
                # 卖出后重置持续天数
                consecutive_valid_days = 0
                atr_growing_days = 0
                volatility_growing_days = 0
        
        # 计算当前市值
        market_value = cash + position * close_price if position > 0 else cash
        position_str = f"{position}" if position > 0 else "0"
        
        # MA20、MA60、ATR14和波动率显示
        ma20_str = f"{ma20:.2f}" if pd.notna(ma20) else "N/A"
        ma60_str = f"{ma60:.2f}" if pd.notna(ma60) else "N/A"
        atr14_str = f"{atr14:.2f}" if pd.notna(atr14) else "N/A"
        volatility = row['波动率']
        volatility_str = f"{volatility:.2f}" if pd.notna(volatility) else "N/A"
        
        # 显示持续天数（只有在没有持仓时才显示实际计数）
        display_days = consecutive_valid_days if position == 0 else 0
        
        print(f"{day_num:<5} {date_str:<12} {row['开盘']:>8.2f} {row['最高']:>8.2f} {row['最低']:>8.2f} "
              f"{close_price:>8.2f} {ma20_str:>8} {ma60_str:>8} {atr14_str:>8} {volatility_str:>8} {display_days:>4} {action:<10} {position_str:>8} {market_value:>12,.2f}")
    
    # 计算最终收益
    final_value = cash + position * df.iloc[-1]['收盘'] if position > 0 else cash
    final_profit = final_value - initial_capital
    
    print(f"\n{'='*140}")
    print(f"回测结果统计")
    print(f"{'='*140}")
    print(f"买卖次数: {trade_count}")
    print(f"起始资金: {initial_capital:,.2f}")
    print(f"最终资金: {final_value:,.2f}")
    print(f"总盈利: {final_profit:,.2f}")
    print(f"收益率: {(final_profit/initial_capital)*100:.2f}%")
    
    if trades:
        print(f"\n交易明细:")
        print(f"{'序号':<6} {'日期':<12} {'操作':<6} {'价格':>10} {'股数':>10} {'盈亏':>12}")
        print("-" * 70)
        for idx, trade in enumerate(trades, 1):
            profit_str = f"{trade.get('profit', 0):,.2f}" if 'profit' in trade else "-"
            print(f"{idx:<6} {trade['date']:<12} {trade['action']:<6} {trade['price']:>10.2f} {trade['shares']:>10} {profit_str:>12}")
    
    print(f"{'='*140}")
    
    # 计算总收益率和年度收益率（用于兼容run_all_socks.py）
    total_return = (final_profit / initial_capital) * 100 if initial_capital > 0 else 0
    
    # 计算年度收益率
    yearly_returns = {}
    if trades:
        for trade in trades:
            if 'profit' in trade and 'date' in trade:
                year = int(trade['date'][:4])
                if year not in yearly_returns:
                    yearly_returns[year] = 0
                yearly_returns[year] += trade['profit']
    
    # 将年度收益转换为收益率
    for year in yearly_returns:
        yearly_returns[year] = (yearly_returns[year] / initial_capital) * 100
    
    return total_return, yearly_returns, {'trades': trades, 'final_value': final_value}


if __name__ == "__main__":
    run_backtest()
