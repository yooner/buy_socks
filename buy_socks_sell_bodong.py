"""
六状态市场分析策略 - 仅状态切换显示
- 只显示市场状态切换，不包含买卖逻辑
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
from market_state_analyzer import (
    MarketStateAnalyzer, 
    MarketState,
    get_six_points_by_price,
    get_three_points_by_price
)


def get_output_file_path(base_name="out_put.txt"):
    """获取可用的输出文件路径"""
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


def run_backtest(stock_code: str = STOCK_CODE, outbreak_sell_e_buyback_threshold: float = None, outbreak_position_buy_count: int = None):
    """回测主函数 - 仅显示状态切换"""
    
    # 获取日线数据
    df = get_daily_data(stock_code, days=365 * BACKTEST_YEARS + 400)
    
    if df is None or len(df) < 400:
        print(f"数据不足，需要至少400天数据，当前只有{len(df) if df is not None else 0}天")
        return None
    
    # 准备数据
    df = prepare_stock_data(df)
    
    # 执行回测逻辑 - 仅显示状态
    return _run_backtest_core(stock_code, df)


def prepare_stock_data(df: pd.DataFrame) -> pd.DataFrame:
    """准备股票数据"""
    df = df.copy()
    df = df.sort_values('date').reset_index(drop=True)
    days_to_show = 365 * BACKTEST_YEARS
    df = df.tail(days_to_show).reset_index(drop=True)
    return df


def _run_backtest_core(stock_code: str, df: pd.DataFrame):
    """回测核心函数 - 仅显示状态切换"""
    
    # 获取年份范围
    start_year, end_year = get_year_range(BACKTEST_YEARS)
    
    # 收集所有输出内容
    output_lines = []

    def log_print(*args, **kwargs):
        """同时打印到终端和收集到列表"""
        line = " ".join(str(arg) for arg in args)
        print(line, **kwargs)
        output_lines.append(line)

    # 初始化市场状态分析器
    state_analyzer = MarketStateAnalyzer(
        six_points_func=get_six_points_by_price,
        three_points_func=get_three_points_by_price
    )
    df_with_states = state_analyzer.analyze(df, price_col='收盘', date_col='date')
    
    # 打印表头
    log_print(f"\n{'='*140}")
    log_print(f"股票代码: {stock_code}")
    log_print(f"回测区间: {start_year} - {end_year} ({BACKTEST_YEARS}年)")
    log_print(f"{'='*140}\n")

    # 动态生成表头
    header = f"{'日':<5} {'日期':<12} {'收盘':>10} {'市场状态':<12} {'段落':<6} {'关键点':>10} {'参考点':>10} {'阶段结束标志':<35} {'转换信息':<30}"
    log_print(header)
    log_print("-" * 160)
    
    # 遍历每一天
    prev_state = None
    prev_downtrend_flag_triggered = False  # 前一天下降阶段结束标志是否已触发
    prev_uptrend_flag_triggered = False    # 前一天上升阶段结束标志是否已触发
    
    for i in range(len(df_with_states)):
        row = df_with_states.iloc[i]
        day_num = i + 1
        date_str = row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date'])[:10]
        close_price = row['收盘']
        market_state = row['market_state']
        is_start = row['is_segment_start']
        is_end = row['is_segment_end']
        key_point = row['key_point']
        ref_key_point = row.get('ref_key_point', None)
        notes = row['state_notes']
        
        # 段落标记
        segment_marker = ""
        if is_start and is_end:
            segment_marker = "[转换]"
        elif is_start:
            segment_marker = "[开始]"
        elif is_end:
            segment_marker = "[结束]"
        
        key_point_str = f"{key_point:.2f}" if pd.notna(key_point) else ""
        ref_key_point_str = f"{ref_key_point:.2f}" if pd.notna(ref_key_point) else ""
        
        # 只在段落开始时显示转换信息
        notes_str = notes if notes and is_start else ""
        
        # 阶段结束标志显示（只在触发当天显示）
        trend_end_flag_str = ""
        
        # 获取当前标志状态
        curr_downtrend_triggered = row.get('downtrend_end_flag_triggered', False)
        curr_uptrend_triggered = row.get('uptrend_end_flag_triggered', False)
        
        # 下降阶段结束标志 - 只在触发当天显示（从False变为True的那一天）
        if curr_downtrend_triggered and not prev_downtrend_flag_triggered:
            flag_low = row.get('downtrend_end_flag_low')
            if pd.notna(flag_low):
                rebound = close_price - flag_low
                trend_end_flag_str = f"下降阶段结束[{flag_low:.2f}→{close_price:.2f} +{rebound:.2f}]"
        # 上升阶段结束标志 - 只在触发当天显示（从False变为True的那一天）
        elif curr_uptrend_triggered and not prev_uptrend_flag_triggered:
            flag_high = row.get('uptrend_end_flag_high')
            if pd.notna(flag_high):
                pullback = flag_high - close_price
                trend_end_flag_str = f"上升阶段结束[{flag_high:.2f}→{close_price:.2f} -{pullback:.2f}]"
        
        # 更新前一天标志状态
        prev_downtrend_flag_triggered = curr_downtrend_triggered
        prev_uptrend_flag_triggered = curr_uptrend_triggered
        
        # 构建输出行
        output_str = f"{day_num:<5} {date_str:<12} {close_price:>10.2f} {market_state:<12} {segment_marker:<6} {key_point_str:>10} {ref_key_point_str:>10} {trend_end_flag_str:<35} {notes_str:<30}"
        log_print(output_str)
        
        # 更新前一天的状态
        if is_start:
            prev_state = market_state
    
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
            change_str = f"{seg_change:+.2f}元"
        else:
            change_str = "进行中"
        
        log_print(f"  [{i}] {seg['状态']}: {seg['开始日期']}~{end_date} | "
                  f"价格:{seg['开始价格']:.2f}→{end_price_str} {change_str} | "
                  f"{key_point_type}:{seg['关键点']:.2f} | {duration}天")
    
    log_print(f"{'='*140}")
    
    # 保存到文件
    output_file = get_output_file_path()
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(output_lines))
        print(f"\n[文件已保存至: {output_file}]")
    except Exception as e:
        print(f"\n[警告: 无法保存文件 - {e}]")
    
    return 0, {}, {'state_segments': segments_df.to_dict('records')}


if __name__ == "__main__":
    run_backtest()
