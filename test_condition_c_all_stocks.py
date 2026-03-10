"""
测试函数：遍历cache下所有股票，统计条件C买入情况
直接复用 buy_socks_sell_bodong.py 的回测逻辑
"""

import pandas as pd
import numpy as np
import os
import json
from datetime import datetime

# 导入主策略文件的函数
from buy_socks_sell_bodong import run_backtest
from ana_stocks import get_daily_data


def get_all_stock_codes_from_cache(cache_dir="cache"):
    """从cache目录获取所有股票代码"""
    stock_codes = []
    if not os.path.exists(cache_dir):
        return stock_codes
    
    for filename in os.listdir(cache_dir):
        if filename.endswith("_daily.json"):
            # 提取股票代码，如 "000002_daily.json" -> "000002"
            stock_code = filename.replace("_daily.json", "")
            stock_codes.append(stock_code)
    
    return sorted(stock_codes)


def analyze_condition_c_trades(stock_code, trades_data):
    """
    从交易记录中分析条件C买入情况
    
    参数:
        stock_code: 股票代码
        trades_data: 交易记录列表
    
    返回:
        list: 条件C买入记录列表
    """
    condition_c_trades = []
    
    if not trades_data or 'trades' not in trades_data:
        return condition_c_trades
    
    trades = trades_data['trades']
    
    for idx, trade in enumerate(trades):
        # 检查是否是条件C买入
        if trade.get('is_condition_c') and trade['action'] == '买入':
            buy_price = trade['price']
            buy_date = trade['date']
            shares = trade['shares']
            
            # 查找对应的卖出记录
            sell_price = None
            sell_date = None
            profit = None
            profit_pct = None
            sell_reason = None
            
            for sell_trade in trades[idx+1:]:
                if sell_trade['action'] == '卖出':
                    sell_price = sell_trade['price']
                    sell_date = sell_trade['date']
                    profit = sell_trade.get('profit', 0)
                    # 计算盈亏百分比
                    if buy_price > 0 and shares > 0:
                        profit_pct = (profit / (buy_price * shares)) * 100
                    sell_reason = sell_trade.get('reason', 'C条件')
                    break
            
            # 判断触发类型（根据action字段）
            action_str = trade.get('action_detail', '')
            if '倍' in str(action_str):
                trigger_type = '倍'
            elif '幅' in str(action_str):
                trigger_type = '幅'
            else:
                trigger_type = '全仓'
            
            condition_c_trades.append({
                'stock_code': stock_code,
                'buy_date': buy_date,
                'buy_price': buy_price,
                'shares': shares,
                'total_cost': buy_price * shares,
                'trigger_type': trigger_type,
                'trigger_value': 0,  # 从action中解析
                'sell_date': sell_date,
                'sell_price': sell_price,
                'profit': profit,
                'profit_pct': profit_pct,
                'sell_reason': sell_reason
            })
    
    return condition_c_trades


def test_all_stocks_condition_c():
    """
    测试所有股票的条件C买入情况
    """
    print("=" * 100)
    print("开始测试所有股票的条件C买入情况")
    print("=" * 100)
    
    # 获取所有股票代码
    stock_codes = get_all_stock_codes_from_cache()
    print(f"\n找到 {len(stock_codes)} 只股票: {', '.join(stock_codes)}")
    print()
    
    all_condition_c_trades = []
    stock_statistics = {}
    
    for idx, stock_code in enumerate(stock_codes, 1):
        print(f"[{idx}/{len(stock_codes)}] 正在分析股票: {stock_code}")
        
        # 检查数据是否存在
        cache_path = os.path.join("cache", f"{stock_code}_daily.json")
        if not os.path.exists(cache_path):
            print(f"  跳过: 数据文件不存在")
            continue
        
        try:
            # 使用主策略文件的回测函数
            # 临时修改STOCK_CODE环境变量
            import buy_socks_sell_bodong as strategy
            original_stock_code = strategy.STOCK_CODE
            strategy.STOCK_CODE = stock_code
            
            # 运行回测
            total_return, yearly_returns, trades_data = run_backtest(stock_code)
            
            # 恢复原始股票代码
            strategy.STOCK_CODE = original_stock_code
            
            # 分析条件C买入情况
            trades = analyze_condition_c_trades(stock_code, trades_data)
            
            if trades:
                all_condition_c_trades.extend(trades)
                
                # 统计该股票的条件C买入情况
                total_trades = len(trades)
                completed_trades = [t for t in trades if t['profit'] is not None]
                profitable_trades = sum(1 for t in completed_trades if t['profit'] > 0)
                total_profit = sum(t['profit'] for t in completed_trades if t['profit'] is not None)
                
                stock_statistics[stock_code] = {
                    'total_trades': total_trades,
                    'completed_trades': len(completed_trades),
                    'profitable_trades': profitable_trades,
                    'total_profit': total_profit,
                    'trades': trades
                }
                
                print(f"  条件C买入次数: {total_trades}, 已完成: {len(completed_trades)}, 盈利次数: {profitable_trades}, 总盈利: {total_profit:,.2f}")
            else:
                print(f"  无条件C买入记录")
                
        except Exception as e:
            print(f"  分析失败: {e}")
            import traceback
            traceback.print_exc()
    
    # 输出详细报告
    print("\n" + "=" * 100)
    print("详细报告")
    print("=" * 100)
    
    for stock_code, stats in stock_statistics.items():
        print(f"\n【股票 {stock_code}】")
        print(f"  总买入次数: {stats['total_trades']}")
        print(f"  已完成交易: {stats['completed_trades']}")
        print(f"  盈利次数: {stats['profitable_trades']}")
        print(f"  亏损次数: {stats['completed_trades'] - stats['profitable_trades']}")
        print(f"  总盈利: {stats['total_profit']:,.2f}")
        print(f"  每次交易详情:")
        
        for i, trade in enumerate(stats['trades'], 1):
            profit_str = f"{trade['profit']:,.2f}" if trade['profit'] is not None else "未卖出"
            profit_pct_str = f"{trade['profit_pct']:+.2f}%" if trade['profit_pct'] is not None else ""
            sell_info = f"卖出日期: {trade['sell_date']}, 卖出价: {trade['sell_price']:.2f}" if trade['sell_date'] else "未卖出"
            
            print(f"    {i}. 买入日期: {trade['buy_date']}, 买入价: {trade['buy_price']:.2f}, "
                  f"股数: {trade['shares']}, 触发: {trade['trigger_type']}, "
                  f"盈亏: {profit_str} {profit_pct_str}, {sell_info}")
    
    # 总体统计
    print("\n" + "=" * 100)
    print("总体统计")
    print("=" * 100)
    
    total_stocks_with_trades = len(stock_statistics)
    total_trades = len(all_condition_c_trades)
    completed_trades = [t for t in all_condition_c_trades if t['profit'] is not None]
    profitable_trades = [t for t in completed_trades if t['profit'] > 0]
    total_profit = sum(t['profit'] for t in completed_trades if t['profit'] is not None)
    
    print(f"有交易记录的股票数: {total_stocks_with_trades}")
    print(f"总买入次数: {total_trades}")
    print(f"已完成交易次数: {len(completed_trades)}")
    print(f"盈利次数: {len(profitable_trades)}")
    print(f"亏损次数: {len(completed_trades) - len(profitable_trades)}")
    print(f"总盈利: {total_profit:,.2f}")
    print(f"平均每次盈利: {total_profit / len(completed_trades) if completed_trades else 0:,.2f}")
    print("=" * 100)
    
    return stock_statistics


if __name__ == "__main__":
    test_all_stocks_condition_c()
