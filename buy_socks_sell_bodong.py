"""
突破新高买入策略 - 集成六状态市场分析
- 起始资金10万，分N仓（默认4仓）
- 突破新高（30/60/180/360日）买入一仓
- 涨幅10%买入下一仓
- 最后一仓下跌超过阈值卖出

六状态分析:
- 上升趋势 / 自然回升 / 次级回升
- 下降趋势 / 自然回撤 / 次级回撤
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
from market_state_analyzer import MarketStateAnalyzer, MarketState

# ==================== 策略配置参数 ====================
INITIAL_CAPITAL = 100000  # 起始资金
POSITION_COUNT = 3  # 分仓数量（默认3仓）
ADD_POSITION_THRESHOLD = 0.05  # 加仓阈值，每上涨3%买入下一仓

# 新高周期配置（可配置，默认60, 90, 180, 360）
BREAKOUT_PERIODS = [180, 360]

# 状态转换阈值配置（可配置）
SIX_POINTS_PCT = 0.20 # 6个点对应的百分比（默认20%）
THREE_POINTS_PCT = 0.10 # 3个点对应的百分比（默认10%）


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
    cash = initial_capital  # 可用现金
    position = 0  # 持仓数量
    position_cost = 0  # 持仓成本（平均成本）
    position_count = 0  # 已买入仓位数量
    last_buy_price = 0  # 上次买入价格（用于计算加仓）
    invested_capital = 0  # 已投入资金
    fixed_position_value = 0  # 固定的每仓金额（买入第一仓时确定）
    
    # 交易记录
    trades = []
    
    # 初始化市场状态分析器（传入可配置的阈值参数）
    state_analyzer = MarketStateAnalyzer(
        six_points_pct=SIX_POINTS_PCT,
        three_points_pct=THREE_POINTS_PCT
    )
    df_with_states = state_analyzer.analyze(df, price_col='收盘', date_col='date')
    
    # 定义上升类型和下降类型趋势
    UP_TRENDS = ['上升趋势', '自然回升', '次级回升']
    DOWN_TRENDS = ['下降趋势', '自然回撤', '次级回撤']
    
    # 收集所有输出内容
    output_lines = []

    def log_print(*args, **kwargs):
        """同时打印到终端和收集到列表"""
        line = " ".join(str(arg) for arg in args)
        print(line, **kwargs)
        output_lines.append(line)

    # 打印表头
    log_print(f"\n{'='*140}")
    log_print(f"股票代码: {stock_code}")
    log_print(f"回测区间: {start_year} - {end_year} ({BACKTEST_YEARS}年)")
    log_print(f"起始资金: {initial_capital:,.2f}")
    log_print(f"{'='*140}\n")

    # 动态生成表头
    header = f"{'日':<5} {'日期':<12} {'收盘':>10} {'市场状态':<12} {'段落':<6} {'关键点':>10} {'转换信息':<50}"
    log_print(header)
    log_print("-" * 105)
    
    # 遍历每一天
    prev_state = None
    for i in range(len(df_with_states)):
        row = df_with_states.iloc[i]
        day_num = i + 1
        date_str = row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date'])[:10]
        close_price = row['收盘']
        market_state = row['market_state']
        is_start = row['is_segment_start']
        is_end = row['is_segment_end']
        key_point = row['key_point']
        notes = row['state_notes']
        
        # 段落标记（同一天可能既是结束也是开始）
        segment_marker = ""
        if is_start and is_end:
            segment_marker = "[转换]"
        elif is_start:
            segment_marker = "[开始]"
        elif is_end:
            segment_marker = "[结束]"
        
        key_point_str = f"{key_point:.2f}" if pd.notna(key_point) else ""
        
        # 只在段落开始时显示转换信息（状态转换的当天）
        notes_str = notes if notes and is_start else ""
        
        # ===== 买卖逻辑 =====
        trade_action = ""
        
        # 计算当前总资金（现金 + 持仓市值）
        current_total_value = cash + position * close_price
        
        # 检查是否发生状态转换
        bought_on_transition = False
        if is_start and prev_state is not None:
            # 买入逻辑：只要转换到上升类型就买入（包括上升类型之间互转）
            if market_state in UP_TRENDS and prev_state != market_state:
                if position_count < POSITION_COUNT:
                    # 第一仓时固定每仓金额，后续沿用固定每仓金额
                    if position_count == 0:
                        fixed_position_value = current_total_value / POSITION_COUNT
                    position_value = fixed_position_value
                    if cash >= position_value * 0.99:
                        shares = position_value / close_price
                        prev_position = position
                        position += shares
                        if prev_position <= 0:
                            position_cost = close_price
                        else:
                            total_cost = position_cost * prev_position + close_price * shares
                            position_cost = total_cost / position
                        invested_capital += shares * close_price
                        cash -= shares * close_price
                        position_count += 1
                        last_buy_price = close_price
                        trade_action = f"[买入{position_count}/{POSITION_COUNT}]"
                        trades.append({
                            'date': date_str,
                            'action': '买入',
                            'position_num': position_count,
                            'price': close_price,
                            'shares': shares,
                            'value': shares * close_price
                        })
                        bought_on_transition = True

            # 卖出逻辑：从上升类型趋势变为下降类型趋势
            if prev_state in UP_TRENDS and market_state in DOWN_TRENDS:
                if position > 0:
                    sell_value = position * close_price
                    profit = sell_value - invested_capital
                    cash += sell_value
                    trade_action = "[卖出全部]"
                    trades.append({
                        'date': date_str,
                        'action': '卖出',
                        'price': close_price,
                        'shares': position,
                        'value': sell_value,
                        'profit': profit
                    })
                    position = 0
                    position_cost = 0
                    position_count = 0
                    last_buy_price = 0
                    invested_capital = 0
                    fixed_position_value = 0

        # 加仓逻辑：当前已有持仓（在上升类型趋势中），且价格上涨超过阈值，且还有可用现金
        # 如果当天已经发生“状态转换买入”，则跳过加仓，避免同一天重复买入
        if (not bought_on_transition) and position_count > 0 and position_count < POSITION_COUNT and market_state in UP_TRENDS:
            if close_price >= last_buy_price * (1 + ADD_POSITION_THRESHOLD):
                # 使用固定的每仓金额
                position_value = fixed_position_value
                # 检查是否有足够现金（允许1%的浮点误差）
                if cash >= position_value * 0.99:
                    shares = position_value / close_price
                    position += shares
                    # 更新平均成本
                    total_cost = position_cost * (position - shares) + close_price * shares
                    position_cost = total_cost / position
                    invested_capital += shares * close_price
                    cash -= shares * close_price
                    position_count += 1
                    last_buy_price = close_price
                    trade_action = f"[买入{position_count}/{POSITION_COUNT}]"
                    trades.append({
                        'date': date_str,
                        'action': '买入',
                        'position_num': position_count,
                        'price': close_price,
                        'shares': shares,
                        'value': shares * close_price
                    })
        
        # 更新前一天的状态
        if is_start:
            prev_state = market_state
        
        # 构建输出行
        output_str = f"{day_num:<5} {date_str:<12} {close_price:>10.2f} {market_state:<12} {segment_marker:<6} {key_point_str:>10} {notes_str:<50}"
        if trade_action:
            output_str += f" {trade_action}"
        log_print(output_str)
    
    log_print(f"{'='*140}")
    
    # 打印状态段落摘要
    log_print("\n【市场状态段落摘要】")
    segments_df = state_analyzer.get_segments_summary()
    for i, seg in segments_df.iterrows():
        duration = seg['持续天数']
        end_date = seg['结束日期'] if seg['结束日期'] else '现在'
        key_point_type = "最高点" if seg['状态'] in ['上升趋势', '自然回升', '次级回升'] else "最低点"
        end_price_str = f"{seg['结束价格']:.2f}" if pd.notna(seg['结束价格']) else '...'
        
        # 计算段落内的价格变化
        if pd.notna(seg['结束价格']):
            seg_change = seg['结束价格'] - seg['开始价格']
            seg_change_pct = (seg_change / seg['开始价格']) * 100
            change_str = f"{seg_change:+.2f}({seg_change_pct:+.2f}%)"
        else:
            change_str = "进行中"
        
        log_print(f"  [{i}] {seg['状态']}: {seg['开始日期']}~{end_date} | "
                  f"价格:{seg['开始价格']:.2f}→{end_price_str} {change_str} | "
                  f"{key_point_type}:{seg['关键点']:.2f} | {duration}天")
    
    # 计算最终收益（包括持仓市值）
    final_price = df_with_states.iloc[-1]['收盘']
    position_value = position * final_price
    final_value = cash + position_value
    final_profit = final_value - initial_capital
    total_return = (final_profit / initial_capital) * 100
    
    log_print(f"\n{'='*140}")
    log_print(f"最终资金: {final_value:,.2f} (现金: {cash:,.2f} + 持仓市值: {position_value:,.2f})")
    log_print(f"总盈亏: {final_profit:,.2f} ({total_return:+.2f}%)")
    log_print(f"交易次数: {len(trades)}")
    
    # 打印交易记录
    if trades:
        log_print(f"\n【交易记录】")
        for trade in trades:
            if trade['action'] == '买入':
                log_print(f"  买入: {trade['date']} | 仓位: {trade['position_num']}/{POSITION_COUNT} | 价格: {trade['price']:.2f} | 数量: {trade['shares']:.0f} | 金额: {trade['value']:,.2f}")
            else:
                log_print(f"  卖出: {trade['date']} | 价格: {trade['price']:.2f} | 数量: {trade['shares']:.0f} | 金额: {trade['value']:,.2f} | 盈亏: {trade['profit']:+.2f}")
    
    log_print(f"{'='*140}")
    
    # 保存到文件
    output_file = get_output_file_path()
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(output_lines))
        print(f"\n[文件已保存至: {output_file}]")
    except Exception as e:
        print(f"\n[警告: 无法保存文件 - {e}]")
    
    return total_return, {}, {'trades': trades, 'final_value': final_value, 'holding_info': None, 'state_segments': segments_df.to_dict('records')}


if __name__ == "__main__":
    run_backtest()
