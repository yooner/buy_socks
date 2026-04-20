"""
突破新高买入策略
- 起始资金10万，分N仓（默认4仓）
- 突破新高（30/60/180/360日）买入一仓
- 涨幅10%买入下一仓
- 最后一仓下跌超过阈值卖出
"""

import pandas as pd
import numpy as np
import os
from ana_stocks import (
    get_daily_data,
    STOCK_CODE_EXPORT as STOCK_CODE,
    BACKTEST_YEARS_EXPORT as BACKTEST_YEARS,
    get_year_range
)

# ==================== 策略配置参数 ====================
INITIAL_CAPITAL = 100000  # 起始资金
POSITION_COUNT = 4  # 分仓数量
BREAKOUT_THRESHOLD = 0.01  # 突破新高阈值（2%）
RISE_THRESHOLD = 0.01  # 涨幅阈值，买入下一仓（10%）

# 每仓的止损比例（第1仓5%，第2仓7%，第3仓10%，第4仓13%）
POSITION_STOP_LOSS = [0.03, 0.04, 0.05, 0.06]

# 新高周期配置（可配置，默认60, 90, 180, 360）
BREAKOUT_PERIODS = [180, 360]


def get_output_file_path(base_name="out_put.txt"):
    """获取可用的输出文件路径，如果被占用则使用序号递增"""
    if not os.path.exists(base_name):
        return base_name
    
    try:
        with open(base_name, 'w', encoding='utf-8') as f:
            pass
        return base_name
    except (PermissionError, IOError):
        base, ext = os.path.splitext(base_name)
        counter = 1
        while True:
            new_path = f"{base}{counter}{ext}"
            if not os.path.exists(new_path):
                return new_path
            try:
                with open(new_path, 'w', encoding='utf-8') as f:
                    pass
                return new_path
            except (PermissionError, IOError):
                counter += 1
                if counter > 100:
                    raise Exception("无法找到可用的输出文件路径")


def run_backtest(stock_code: str = STOCK_CODE):
    """回测主函数"""
    
    # 获取日线数据
    df = get_daily_data(stock_code, days=365 * BACKTEST_YEARS + 400)
    
    if df is None or len(df) < 400:
        print(f"数据不足，需要至少400天数据，当前只有{len(df) if df is not None else 0}天")
        return None
    
    # 准备数据
    df = prepare_stock_data(df)
    
    # 执行回测逻辑
    return _run_backtest_core(stock_code, df)


def prepare_stock_data(df: pd.DataFrame) -> pd.DataFrame:
    """准备股票数据，计算N日高低价"""
    df = df.copy()
    
    # 按日期从远到近排序
    df = df.sort_values('date').reset_index(drop=True)
    
    # 只取最近一年的数据
    days_to_show = 365 * BACKTEST_YEARS
    df = df.tail(days_to_show).reset_index(drop=True)
    
    # 计算N日高/低价（使用可配置的周期）
    # 使用min_periods=period确保只有满N天才显示数据
    for period in BREAKOUT_PERIODS:
        df[f'{period}日高'] = df['收盘'].rolling(window=period, min_periods=period).max()
        df[f'{period}日低'] = df['收盘'].rolling(window=period, min_periods=period).min()
    
    return df


def _run_backtest_core(stock_code: str, df: pd.DataFrame):
    """回测核心函数"""
    
    # 获取年份范围
    start_year, end_year = get_year_range(BACKTEST_YEARS)
    
    # 初始化交易变量
    initial_capital = INITIAL_CAPITAL
    cash = initial_capital
    position_per_unit = initial_capital / POSITION_COUNT  # 每仓资金
    
    # 持仓状态
    positions = []  # 已买入的仓位列表，每个元素是买入价格
    total_shares = 0  # 总持股数
    
    # 新高锁定状态：买入第一仓后锁定，卖出后解锁
    locked_highs = {}  # 锁定的各周期新高值
    is_highs_locked = False  # 是否锁定新高
    
    # 持仓期间最高价（用于跟踪止损）
    position_high_price = 0  # 持仓期间观察到的最高价
    
    # 交易记录
    trades = []
    
    # 收集所有输出内容
    output_lines = []

    def log_print(*args, **kwargs):
        """同时打印到终端和收集到列表"""
        line = " ".join(str(arg) for arg in args)
        print(line, **kwargs)
        output_lines.append(line)

    # 打印表头
    log_print(f"\n{'='*120}")
    log_print(f"股票代码: {stock_code}")
    log_print(f"回测区间: {start_year} - {end_year} ({BACKTEST_YEARS}年)")
    log_print(f"起始资金: {initial_capital:,.2f}")
    log_print(f"分仓数量: {POSITION_COUNT}仓，每仓{position_per_unit:,.2f}元")
    log_print(f"买入条件: 突破{BREAKOUT_PERIODS}日新高(>{BREAKOUT_THRESHOLD*100:.0f}%)买入，涨幅>{RISE_THRESHOLD*100:.0f}%买入下一仓")
    log_print(f"卖出条件: 持仓最高价下跌超过对应仓位止损比例卖出")
    log_print(f"{'='*120}\n")

    # 动态生成表头
    period_headers = ' '.join([f"{str(p)+'日高':>10}" for p in BREAKOUT_PERIODS])
    header = f"{'日':<5} {'日期':<12} {'收盘':>10} {period_headers} {'操作':<25} {'持仓':>8} {'市值':>12}"
    log_print(header)
    log_print("-" * (70 + len(BREAKOUT_PERIODS) * 11))
    
    # 遍历每一天
    for i in range(len(df)):
        row = df.iloc[i]
        day_num = i + 1
        date_str = row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date'])[:10]
        close_price = row['收盘']
        
        action = ""  # 当日操作描述
        
        # 获取当日N日高价（用于判断突破）
        breakout_types = []
        for period in BREAKOUT_PERIODS:
            if is_highs_locked:
                # 使用锁定的新高值判断
                prev_high = locked_highs.get(period)
                if prev_high and close_price >= prev_high * (1 + BREAKOUT_THRESHOLD):
                    breakout_types.append(f"{period}日")
            else:
                # 使用当日的N日高判断（收盘价是否创了当日N日新高）
                current_high = row[f'{period}日高']
                if pd.notna(current_high) and close_price >= current_high * (1 - 0.0001):  # 允许微小误差
                    # 检查前一日是否也是新高（避免重复买入）
                    if i > 0:
                        prev_close = df.iloc[i-1]['收盘']
                        prev_high = df.iloc[i-1][f'{period}日高']
                        if pd.notna(prev_high) and prev_close < prev_high * (1 - 0.0001):
                            breakout_types.append(f"{period}日")
                    else:
                        breakout_types.append(f"{period}日")
        
        # ========== 更新持仓期间最高价 ==========
        if len(positions) > 0:
            position_high_price = max(position_high_price, close_price)
        
        # ========== 检查是否需要卖出 ==========
        if len(positions) > 0:
            drop_pct = (position_high_price - close_price) / position_high_price
            # 根据当前持仓数量确定止损比例（第1仓用5%，第2仓用7%，第3仓用10%，第4仓用13%）
            current_stop_loss = POSITION_STOP_LOSS[len(positions) - 1]
            
            if drop_pct >= current_stop_loss:
                # 卖出所有持仓
                sell_value = total_shares * close_price
                profit = sell_value - sum(p * (initial_capital / POSITION_COUNT / p) for p in positions)
                cash += sell_value
                
                action = f"卖出全部@{close_price:.2f}(从最高{position_high_price:.2f}跌{drop_pct*100:.1f}%,超{current_stop_loss*100:.0f}%)"
                trades.append({
                    'day': day_num,
                    'date': date_str,
                    'action': '卖出',
                    'price': close_price,
                    'shares': total_shares,
                    'profit': profit
                })
                
                positions = []
                total_shares = 0
                position_high_price = 0  # 重置持仓最高价
                
                # 解锁新高检测
                is_highs_locked = False
                locked_highs = {}
                
                # 卖出后清空breakout_types，避免当天再买入
                breakout_types = []
        
        # ========== 检查是否需要买入 ==========
        if len(positions) < POSITION_COUNT and cash >= position_per_unit:
            should_buy = False
            buy_reason = ""
            
            if len(positions) == 0:
                # 第一仓：检查是否突破任何一种新高
                if breakout_types:
                    should_buy = True
                    buy_reason = f"突破{'/'.join(breakout_types)}新高"
            else:
                # 后续仓位：检查是否比上一仓上涨超过阈值
                last_buy_price = positions[-1]
                rise_pct = (close_price - last_buy_price) / last_buy_price
                
                if rise_pct >= RISE_THRESHOLD:
                    should_buy = True
                    buy_reason = f"比上仓涨{rise_pct*100:.1f}%"
            
            if should_buy:
                # 计算可买入股数
                buy_amount = position_per_unit
                shares_to_buy = int(buy_amount / close_price / 100) * 100  # 取整到100股
                
                if shares_to_buy >= 100 and cash >= shares_to_buy * close_price:
                    cost = shares_to_buy * close_price
                    cash -= cost
                    positions.append(close_price)
                    total_shares += shares_to_buy
                    
                    action = f"买入第{len(positions)}仓@{close_price:.2f}({buy_reason})"
                    trades.append({
                        'day': day_num,
                        'date': date_str,
                        'action': '买入',
                        'price': close_price,
                        'shares': shares_to_buy,
                        'unit': len(positions)
                    })
                    
                    # 如果是第一仓，锁定当前所有周期的新高值，并初始化持仓最高价
                    if len(positions) == 1:
                        is_highs_locked = True
                        position_high_price = close_price  # 初始化持仓最高价为买入价
                        for period in BREAKOUT_PERIODS:
                            locked_highs[period] = row[f'{period}日高']
        
        # 计算当前市值
        market_value = cash + total_shares * close_price
        position_str = f"{len(positions)}/{POSITION_COUNT}" if len(positions) > 0 else "0/0"
        
        # 动态生成显示值
        def fmt_val(val):
            if pd.notna(val):
                return f"{val:>10.2f}"
            else:
                return f"{'N/A':>10}"
        period_values = ' '.join([fmt_val(row[f'{p}日高']) for p in BREAKOUT_PERIODS])
        
        log_print(f"{day_num:<5} {date_str:<12} {close_price:>10.2f} {period_values} {action:<25} {position_str:>8} {market_value:>12,.2f}")
    
    log_print(f"{'='*120}")
    
    # 计算最终收益
    final_value = cash + total_shares * df.iloc[-1]['收盘']
    final_profit = final_value - initial_capital
    total_return = (final_profit / initial_capital) * 100
    
    log_print(f"\n{'='*120}")
    log_print(f"最终资金: {final_value:,.2f}")
    log_print(f"总盈亏: {final_profit:,.2f} ({total_return:+.2f}%)")
    log_print(f"交易次数: {len(trades)}")
    log_print(f"{'='*120}")
    
    # 保存到文件
    output_file = get_output_file_path()
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(output_lines))
        print(f"\n[文件已保存至: {output_file}]")
    except Exception as e:
        print(f"\n[警告: 无法保存文件 - {e}]")
    
    return total_return, {}, {'trades': trades, 'final_value': final_value, 'holding_info': None}


if __name__ == "__main__":
    run_backtest()
