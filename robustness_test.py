"""
简化版策略测试脚本

策略逻辑：
1. 买入条件：价ATR倍数 < -3 时开始标记，等到价ATR倍数 > -3 时买入
2. 卖出条件（满足任一即卖出）：
   - 盈利10%
   - 价ATR倍数 > 3
   - 亏损10%（止损）

输出：每次买卖记录
"""

import pandas as pd
import numpy as np
import os
from glob import glob
from datetime import datetime

# 导入数据获取函数
from ana_stocks import get_daily_data

# 配置参数
CACHE_DIR = "./cache"
INITIAL_CAPITAL = 100000  # 初始资金

# 策略参数
BUY_TRIGGER_ATR = -3.0      # 买入触发：价ATR从 < -3 变为 > -3
PROFIT_TAKE_PCT = 0.10      # 盈利10%卖出
STOP_LOSS_PCT = -0.10       # 亏损10%止损
SELL_TRIGGER_ATR = 3.0      # 价ATR > 3 卖出

# 指定股票组合测试（如果为空，则测试所有股票）
SPECIFIC_STOCKS = ['302132', '603496', '000543', '000002']


def get_all_stock_codes(cache_dir=CACHE_DIR):
    """从缓存目录获取所有股票代码"""
    cache_files = glob(os.path.join(cache_dir, "*_daily.json"))
    stock_codes = []
    for f in cache_files:
        basename = os.path.basename(f)
        code = basename.replace('_daily.json', '')
        stock_codes.append(code)
    return sorted(stock_codes)


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    """准备股票数据，计算技术指标"""
    df = df.copy()
    
    # 按日期从远到近排序
    df = df.sort_values('date').reset_index(drop=True)
    
    # 计算MA20
    df['ma20'] = df['收盘'].rolling(window=20, min_periods=20).mean()
    
    # 计算ATR
    prev_close = df['收盘'].shift(1)
    tr1 = df['最高'] - df['最低']
    tr2 = (df['最高'] - prev_close).abs()
    tr3 = (df['最低'] - prev_close).abs()
    df['tr'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df['atr14'] = df['tr'].rolling(window=14, min_periods=14).mean()
    
    # 计算价ATR倍数
    df['价ATR倍'] = ((df['收盘'] - df['ma20']) / df['atr14']).replace([np.inf, -np.inf], np.nan)
    
    return df


def run_simple_strategy(stock_code: str):
    """
    运行简化策略
    
    策略逻辑：
    1. 买入：价ATR < -3 时标记，等到价ATR > -3 时买入
    2. 卖出：盈利10% 或 价ATR > 3 或 亏损10%止损
    """
    # 获取数据
    df = get_daily_data(stock_code, days=365 * 5 + 100)  # 获取5年数据
    
    if df is None or len(df) < 60:
        print(f"股票 {stock_code}: 数据不足，跳过")
        return None
    
    # 准备数据
    df = prepare_data(df)
    
    # 初始化
    cash = INITIAL_CAPITAL
    position = 0
    buy_price = 0
    trades = []
    
    # 状态标记
    waiting_for_buy = False  # 是否等待买入（价ATR已 < -3）
    
    print(f"\n{'='*80}")
    print(f"股票代码: {stock_code}")
    print(f"{'='*80}")
    print(f"{'日期':<12} {'操作':<10} {'价格':>10} {'股数':>8} {'盈亏':>12} {'备注':<20}")
    print("-" * 80)
    
    for i in range(len(df)):
        row = df.iloc[i]
        date_str = row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date'])[:10]
        close_price = row['收盘']
        price_atr = row['价ATR倍'] if pd.notna(row['价ATR倍']) else 0
        
        # 买入逻辑：没有持仓时
        if position == 0:
            if not waiting_for_buy:
                # 等待价ATR < -3
                if price_atr < BUY_TRIGGER_ATR:
                    waiting_for_buy = True
                    print(f"{date_str:<12} {'标记':<10} {close_price:>10.2f} {'0':>8} {'-':>12} 价ATR={price_atr:.2f} < -3")
            else:
                # 已经标记，等待价ATR > -3 买入
                if price_atr > BUY_TRIGGER_ATR:
                    # 买入
                    position = int(cash / close_price / 100) * 100
                    if position >= 100:
                        buy_price = close_price
                        cost = position * buy_price
                        cash -= cost
                        waiting_for_buy = False
                        
                        trades.append({
                            'date': date_str,
                            'action': '买入',
                            'price': buy_price,
                            'shares': position
                        })
                        
                        print(f"{date_str:<12} {'买入':<10} {buy_price:>10.2f} {position:>8} {'-':>12} 价ATR={price_atr:.2f} > -3")
        
        # 卖出逻辑：有持仓时
        else:
            current_profit_pct = (close_price - buy_price) / buy_price
            
            # 检查卖出条件
            sell_reason = None
            
            # 条件1：盈利10%
            if current_profit_pct >= PROFIT_TAKE_PCT:
                sell_reason = f"盈利{current_profit_pct*100:.1f}%"
            
            # 条件2：价ATR > 3
            elif price_atr > SELL_TRIGGER_ATR:
                sell_reason = f"价ATR={price_atr:.2f} > 3"
            
            # 条件3：亏损10%止损
            elif current_profit_pct <= STOP_LOSS_PCT:
                sell_reason = f"止损{current_profit_pct*100:.1f}%"
            
            # 执行卖出
            if sell_reason:
                sell_price = close_price
                sell_value = position * sell_price
                profit = (sell_price - buy_price) * position
                cash += sell_value
                
                trades.append({
                    'date': date_str,
                    'action': '卖出',
                    'price': sell_price,
                    'shares': position,
                    'profit': profit
                })
                
                print(f"{date_str:<12} {'卖出':<10} {sell_price:>10.2f} {position:>8} {profit:>12,.2f} {sell_reason}")
                
                position = 0
                buy_price = 0
    
    # 计算最终收益
    final_value = cash + position * df.iloc[-1]['收盘'] if position > 0 else cash
    total_return_pct = (final_value - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100
    
    print("-" * 80)
    print(f"交易次数: {len([t for t in trades if t['action'] == '卖出'])}")
    print(f"最终市值: {final_value:,.2f}")
    print(f"总收益率: {total_return_pct:+.2f}%")
    print(f"{'='*80}\n")
    
    return {
        'stock_code': stock_code,
        'trades': trades,
        'final_value': final_value,
        'total_return_pct': total_return_pct
    }


def main():
    """主函数"""
    # 确定要测试的股票
    if SPECIFIC_STOCKS:
        test_stocks = SPECIFIC_STOCKS
        print(f"使用指定股票: {test_stocks}")
    else:
        all_stocks = get_all_stock_codes()
        if not all_stocks:
            print("没有找到股票数据")
            return
        test_stocks = all_stocks[:1]  # 只测试第一只
        print(f"测试股票: {test_stocks[0]} (共{len(all_stocks)}只，每次测1只)")
    
    # 运行测试
    results = []
    for stock_code in test_stocks:
        result = run_simple_strategy(stock_code)
        if result:
            results.append(result)
    
    # 打印汇总
    if len(results) > 1:
        print(f"\n{'='*80}")
        print("汇总结果")
        print(f"{'='*80}")
        for r in results:
            print(f"{r['stock_code']}: 收益率 {r['total_return_pct']:+.2f}%, 最终市值 {r['final_value']:,.2f}")
        print(f"{'='*80}\n")


if __name__ == "__main__":
    main()
