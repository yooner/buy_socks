"""
交易量分析策略

功能：
1. 从 ana_stocks.py 获取测试数据与回测年数
2. 列出每天的收盘价、交易量
3. 计算当天交易量相比前一天的百分比
4. 基于成交量变化进行买入卖出回测
"""

import pandas as pd
import numpy as np
from ana_stocks import (
    STOCK_CODE, get_daily_data, BACKTEST_START_DATE, BACKTEST_END_DATE,
    START_DATE, END_DATE, BACKTEST_YEARS
)

# 配置参数
INITIAL_CAPITAL = 100000  # 初始资金

# 买入配置
BUY_VOLUME_THRESHOLD = 150  # 成交量涨幅阈值（%），超过此值则买入

# 卖出配置
SELL_CONSECUTIVE_DAYS = 3  # 连续几天成交量正增长
SELL_VOLUME_THRESHOLD = 200  # 连续天数中至少有一天成交量涨幅超过此值（%）


def analyze_volume(stock_code: str = STOCK_CODE):
    """分析交易量数据"""
    
    # 获取日线数据
    print(f"正在获取股票 {stock_code} 的日线数据...")
    df = get_daily_data(stock_code)
    
    if df.empty:
        print(f"未获取到股票 {stock_code} 的数据")
        return
    
    # 按日期从前到后排序
    df = df.sort_values('date').reset_index(drop=True)
    
    # 计算交易量变化百分比（排序后再计算）
    df['成交量变化%'] = df['成交量'].pct_change() * 100
    
    # 格式化输出
    print(f"\n{'='*100}")
    print(f"股票代码: {stock_code}")
    print(f"数据范围: {df['date'].min().strftime('%Y-%m-%d')} 至 {df['date'].max().strftime('%Y-%m-%d')}")
    print(f"总交易日: {len(df)} 天")
    print(f"{'='*100}\n")
    
    # 打印表头
    print(f"{'日期':<12} {'收盘价':>10} {'成交量':>15} {'成交量变化%':>15}")
    print("-" * 60)
    
    # 打印每一天的数据
    for idx, row in df.iterrows():
        date_str = row['date'].strftime('%Y-%m-%d')
        close_price = row['收盘']
        volume = row['成交量']
        volume_change = row['成交量变化%']
        
        # 第一天的成交量变化为NaN
        if pd.isna(volume_change):
            volume_change_str = "-"
        else:
            volume_change_str = f"{volume_change:+.2f}%"
        
        print(f"{date_str:<12} {close_price:>10.2f} {volume:>15,.0f} {volume_change_str:>15}")
    
    # 统计信息
    print(f"\n{'='*100}")
    print("统计信息:")
    print(f"{'='*100}")
    
    print(f"\n收盘价统计:")
    print(f"  最高: {df['收盘'].max():.2f}")
    print(f"  最低: {df['收盘'].min():.2f}")
    print(f"  平均: {df['收盘'].mean():.2f}")
    print(f"  标准差: {df['收盘'].std():.2f}")
    
    print(f"\n成交量统计:")
    print(f"  最高: {df['成交量'].max():,.0f}")
    print(f"  最低: {df['成交量'].min():,.0f}")
    print(f"  平均: {df['成交量'].mean():,.0f}")
    print(f"  标准差: {df['成交量'].std():,.0f}")
    
    # 成交量变化统计（排除第一天）
    volume_changes = df['成交量变化%'].dropna()
    print(f"\n成交量变化统计:")
    print(f"  最大涨幅: {volume_changes.max():.2f}%")
    print(f"  最大跌幅: {volume_changes.min():.2f}%")
    print(f"  平均变化: {volume_changes.mean():.2f}%")
    print(f"  标准差: {volume_changes.std():.2f}%")
    
    # 找出成交量异常放大的日期
    print(f"\n成交量异常放大（涨幅 > 100%）:")
    abnormal_days = df[df['成交量变化%'] > 100]
    if not abnormal_days.empty:
        for _, row in abnormal_days.iterrows():
            date_str = row['date'].strftime('%Y-%m-%d')
            print(f"  {date_str}: 成交量 {row['成交量']:,.0f}, 涨幅 {row['成交量变化%']:.2f}%")
    else:
        print("  无")
    
    # 找出成交量异常缩小的日期
    print(f"\n成交量异常缩小（跌幅 < -50%）:")
    abnormal_days = df[df['成交量变化%'] < -50]
    if not abnormal_days.empty:
        for _, row in abnormal_days.iterrows():
            date_str = row['date'].strftime('%Y-%m-%d')
            print(f"  {date_str}: 成交量 {row['成交量']:,.0f}, 跌幅 {row['成交量变化%']:.2f}%")
    else:
        print("  无")
    
    return df


def backtest_volume_strategy(stock_code: str = STOCK_CODE):
    """基于成交量策略进行回测"""
    
    # 获取日线数据
    print(f"\n{'='*100}")
    print(f"开始回测股票 {stock_code} 的成交量策略")
    print(f"{'='*100}")
    
    df = get_daily_data(stock_code)
    
    if df.empty:
        print(f"未获取到股票 {stock_code} 的数据")
        return
    
    # 按日期从前到后排序
    df = df.sort_values('date').reset_index(drop=True)
    
    # 计算交易量变化百分比
    df['成交量变化%'] = df['成交量'].pct_change() * 100
    
    # 计算连续正增长天数
    df['连续正增长天数'] = 0
    consecutive_count = 0
    for i in range(len(df)):
        if i > 0 and df.loc[i, '成交量变化%'] > 0:
            consecutive_count += 1
        else:
            consecutive_count = 0
        df.loc[i, '连续正增长天数'] = consecutive_count
    
    # 回测变量
    cash = INITIAL_CAPITAL
    position = 0
    buy_price = 0
    trades = []
    
    print(f"\n买入规则: 成交量涨幅 > {BUY_VOLUME_THRESHOLD}%")
    print(f"卖出规则: 连续 {SELL_CONSECUTIVE_DAYS} 天成交量正增长，且至少有一天涨幅 > {SELL_VOLUME_THRESHOLD}%")
    print(f"初始资金: {cash:,.2f}\n")
    
    # 打印表头
    print(f"{'日期':<12} {'收盘价':>10} {'成交量':>15} {'成交量变化%':>15} {'操作':>10} {'持仓':>10}")
    print("-" * 80)
    
    # 遍历每一天
    for i in range(len(df)):
        row = df.iloc[i]
        date_str = row['date'].strftime('%Y-%m-%d')
        close_price = row['收盘']
        volume = row['成交量']
        volume_change = row['成交量变化%']
        consecutive_days = row['连续正增长天数']
        
        action = "-"
        
        # 买入逻辑：成交量涨幅超过阈值
        if position == 0 and not pd.isna(volume_change) and volume_change > BUY_VOLUME_THRESHOLD:
            shares = int(cash / close_price)
            if shares > 0:
                position = shares
                buy_price = close_price
                cash -= shares * close_price
                action = f"买入{shares}股"
                trades.append({
                    'date': date_str,
                    'action': '买入',
                    'price': close_price,
                    'shares': shares,
                    'volume_change': volume_change
                })
        
        # 卖出逻辑：连续3天成交量正增长，且至少有一天涨幅 > 200%
        elif position > 0 and consecutive_days >= SELL_CONSECUTIVE_DAYS:
            # 检查过去3天中是否有一天涨幅 > 200%
            if i >= SELL_CONSECUTIVE_DAYS:
                recent_changes = df.loc[i-SELL_CONSECUTIVE_DAYS+1:i+1, '成交量变化%'].values
                if any(change > SELL_VOLUME_THRESHOLD for change in recent_changes if not pd.isna(change)):
                    profit = (close_price - buy_price) * position
                    action = f"卖出{position}股"
                    trades.append({
                        'date': date_str,
                        'action': '卖出',
                        'price': close_price,
                        'shares': position,
                        'profit': profit,
                        'volume_change': volume_change
                    })
                    cash += position * close_price
                    position = 0
                    buy_price = 0
        
        # 打印当天数据
        volume_change_str = f"{volume_change:+.2f}%" if not pd.isna(volume_change) else "-"
        print(f"{date_str:<12} {close_price:>10.2f} {volume:>15,.0f} {volume_change_str:>15} {action:>10} {position:>10}")
    
    # 计算最终市值
    final_value = cash
    if position > 0:
        final_value += position * buy_price
    
    total_return = (final_value - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100
    
    # 统计交易
    buy_trades = [t for t in trades if t['action'] == '买入']
    sell_trades = [t for t in trades if t['action'] == '卖出']
    total_profit = sum(t.get('profit', 0) for t in sell_trades)
    win_trades = [t for t in sell_trades if t.get('profit', 0) > 0]
    lose_trades = [t for t in sell_trades if t.get('profit', 0) <= 0]
    
    print(f"\n{'='*100}")
    print("回测结果:")
    print(f"{'='*100}")
    print(f"\n资金统计:")
    print(f"  初始资金: {INITIAL_CAPITAL:,.2f}")
    print(f"  最终市值: {final_value:,.2f}")
    print(f"  总收益率: {total_return:+.2f}%")
    
    print(f"\n交易统计:")
    print(f"  买入次数: {len(buy_trades)}")
    print(f"  卖出次数: {len(sell_trades)}")
    print(f"  总盈利: {total_profit:,.2f}")
    print(f"  盈利交易: {len(win_trades)} 次")
    print(f"  亏损交易: {len(lose_trades)} 次")
    
    if len(sell_trades) > 0:
        win_rate = len(win_trades) / len(sell_trades) * 100
        avg_profit = total_profit / len(sell_trades)
        print(f"  胜率: {win_rate:.2f}%")
        print(f"  平均每笔: {avg_profit:,.2f}")
    
    print(f"\n交易明细:")
    print(f"{'='*100}")
    for trade in trades:
        if trade['action'] == '买入':
            print(f"  {trade['date']}: 买入 {trade['shares']} 股 @ {trade['price']:.2f} (成交量变化: {trade['volume_change']:+.2f}%)")
        else:
            print(f"  {trade['date']}: 卖出 {trade['shares']} 股 @ {trade['price']:.2f}, 盈亏: {trade['profit']:+.2f} (成交量变化: {trade['volume_change']:+.2f}%)")
    
    return df


def main():
    """主函数"""
    stock_code = STOCK_CODE
    
    print(f"交易量分析策略")
    print(f"回测年数: {BACKTEST_YEARS} 年")
    print(f"回测开始日期: {BACKTEST_START_DATE if BACKTEST_START_DATE else START_DATE}")
    print(f"回测结束日期: {BACKTEST_END_DATE if BACKTEST_END_DATE else END_DATE}")
    
    # 先进行成交量分析
    df = analyze_volume(stock_code)
    
    # 再进行策略回测
    backtest_volume_strategy(stock_code)
    
    return df


if __name__ == "__main__":
    main()
