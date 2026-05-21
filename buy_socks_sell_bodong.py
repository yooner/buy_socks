"""
波动率策略 - 基于市场状态转换的交易策略
"""


import pandas as pd
import numpy as np
import os
import sys
import subprocess
import talib
from ana_stocks import (
    get_daily_data,
    STOCK_CODE_EXPORT as STOCK_CODE,
    BACKTEST_YEARS_EXPORT as BACKTEST_YEARS,
    get_year_range
)
from market_state_analyzer import (
    MarketStateAnalyzer,
    get_six_points_by_price,
    get_three_points_by_price
)


def get_output_file_path(base_name="out_put.txt"):
    """获取可用的输出文件路径，如果被占用则使用序号递增"""
    if not os.path.exists(base_name):
        return base_name
    
    # 尝试写入测试
    try:
        with open(base_name, 'w', encoding='utf-8') as f:
            pass
        return base_name
    except (PermissionError, IOError):
        # 文件被占用，寻找可用的序号
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
                if counter > 100:  # 防止无限循环
                    raise Exception("无法找到可用的输出文件路径")


# ATR周期配置
ATR_PERIOD = 14  # ATR计算周期，可配置为10、14、20等

# MACD卖出阈值
MACD_SELL_THRESHOLD = 0.03  # MACD从大变小的幅度阈值

# MACD卖出阈值 - 上升趋势
MACD_SELL_THRESHOLD_UPTREND = -0.15

# 自然回升MACD高值保护阈值 - 当MACD大于此值时，即使变低也不卖出
NATURAL_RALLY_MACD_PROTECT_THRESHOLD = 1.0  # 可配置，默认1.0

MAX_OBSERVATION_DAYS = 3  # 最大观察天数

def check_macd_sell_signal(df: pd.DataFrame, current_idx: int, threshold: float = 0.05) -> bool:
    """
    检查MACD卖出信号
    
    条件：
    - MACD柱从大变小（当前MACD < 前一天MACD）
    
    Args:
        df: DataFrame包含MACD数据
        current_idx: 当前索引
        threshold: MACD变小的幅度阈值（已弃用，保留参数兼容性）
        
    Returns:
        bool: 是否触发卖出信号
    """
    if current_idx < 1:
        return False
    
    current_macd = df.loc[current_idx, 'MACD']
    prev_macd = df.loc[current_idx - 1, 'MACD']
    
    if pd.isna(current_macd) or pd.isna(prev_macd):
        return False
    
    # MACD柱从大变小（当前 < 前一天），不限制幅度
    macd_decreasing = current_macd < prev_macd
    
    return macd_decreasing


def calculate_slope_atr(df, ma_period=20, atr_period=14, n=5):
    """计算波动率指标"""
    df = df.copy()
    
    # 计算MA
    ma_col = f'ma{ma_period}'
    df[ma_col] = df['收盘'].rolling(window=ma_period, min_periods=ma_period).mean()
    
    # 计算ATR
    prev_close = df['收盘'].shift(1)
    tr1 = df['最高'] - df['最低']
    tr2 = (df['最高'] - prev_close).abs()
    tr3 = (df['最低'] - prev_close).abs()
    df['tr'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df['atr'] = df['tr'].rolling(window=atr_period, min_periods=atr_period).mean()
    
    # 计算波动率：(收盘价 - MA) / ATR
    df['波动率'] = ((df['收盘'] - df[ma_col]) / df['atr']).replace([np.inf, -np.inf], np.nan)
    
    return df


def prepare_stock_data(df: pd.DataFrame) -> pd.DataFrame:
    """准备股票数据，计算所有技术指标"""
    df = df.copy()
    
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
    df['atr'] = df['tr'].rolling(window=ATR_PERIOD, min_periods=ATR_PERIOD).mean()
    
    # 计算波动率
    df = calculate_slope_atr(df, ma_period=20, atr_period=ATR_PERIOD, n=5)

    # 使用TA-Lib计算MACD指标 (默认参数: fastperiod=12, slowperiod=26, signalperiod=9)
    df['DIF'], df['DEA'], df['MACD'] = talib.MACD(
        df['收盘'].values,
        fastperiod=12,
        slowperiod=26,
        signalperiod=9
    )
    # MACD柱状图通常显示为2倍
    df['MACD'] = df['MACD'] * 2

    # 使用市场状态分析器获取原始市场状态
    analyzer = MarketStateAnalyzer(
        six_points_func=get_six_points_by_price,
        three_points_func=get_three_points_by_price
    )
    df = analyzer.analyze(df, price_col='收盘', date_col='date')
    
    # 提取状态转换信息和阶段结束标志
    state_transitions = []
    for i in range(len(df)):
        transition_info = ""
        if i > 0:
            prev_state = df.loc[i-1, 'market_state'] if 'market_state' in df.columns else ""
            curr_state = df.loc[i, 'market_state'] if 'market_state' in df.columns else ""
            is_segment_start = df.loc[i, 'is_segment_start'] if 'is_segment_start' in df.columns else False
            state_notes = df.loc[i, 'state_notes'] if 'state_notes' in df.columns else ""
            
            # 状态转换信息
            if is_segment_start and state_notes:
                transition_info = state_notes
            
            # 检查阶段结束标志
            # 上升阶段结束标志（DIF判断已内化到market_state_analyzer中）
            prev_uptrend_end = df.loc[i-1, 'uptrend_end_flag_triggered'] if 'uptrend_end_flag_triggered' in df.columns else False
            curr_uptrend_end = df.loc[i, 'uptrend_end_flag_triggered'] if 'uptrend_end_flag_triggered' in df.columns else False
            if not prev_uptrend_end and curr_uptrend_end:
                if transition_info:
                    transition_info += " | 上升阶段结束"
                else:
                    transition_info = "上升阶段结束"
            
            # 下降阶段结束标志
            prev_downtrend_end = df.loc[i-1, 'downtrend_end_flag_triggered'] if 'downtrend_end_flag_triggered' in df.columns else False
            curr_downtrend_end = df.loc[i, 'downtrend_end_flag_triggered'] if 'downtrend_end_flag_triggered' in df.columns else False
            if not prev_downtrend_end and curr_downtrend_end:
                if transition_info:
                    transition_info += " | 下降阶段结束"
                else:
                    transition_info = "下降阶段结束"
        else:
            transition_info = "初始状态"
        
        state_transitions.append(transition_info)
    
    df['state_transition'] = state_transitions
    
    return df


def run_backtest(stock_code: str = STOCK_CODE):
    """回测主函数"""
    
    # 获取日线数据
    df = get_daily_data(stock_code, days=365 * BACKTEST_YEARS + 100)
    
    if df is None or len(df) < 60:
        print(f"数据不足，需要至少60天数据，当前只有{len(df) if df is not None else 0}天")
        return None
    
    # 准备数据
    df = prepare_stock_data(df)
    
    # 执行回测
    return _run_backtest_with_trading(stock_code, df)


def _run_backtest_with_trading(stock_code: str, df: pd.DataFrame):
    """带交易逻辑的回测"""
    # 获取年份范围
    start_year, end_year = get_year_range(BACKTEST_YEARS)
    
    # 收集所有输出内容
    output_lines = []

    def log_print(*args, **kwargs):
        """同时打印到终端和收集到列表"""
        line = " ".join(str(arg) for arg in args)
        print(line, **kwargs)
        output_lines.append(line)

    # 打印表头
    log_print(f"\n{'='*175}")
    log_print(f"股票代码: {stock_code}")
    log_print(f"回测区间: {start_year} - {end_year} ({BACKTEST_YEARS}年)")
    log_print(f"{'='*165}\n")

    # 初始化交易状态
    position = 0  # 持仓数量
    cash = 100000  # 初始资金
    trades = []  # 交易记录
    
    # 仓位管理配置
    MAX_POSITIONS = 8  # 最大8个仓位
    PRICE_INCREASE_PCT = 0.05  # 每上涨5%加仓
    
    # 仓位状态
    position_count = 0  # 已买入的仓位数量
    first_buy_price = 0  # 第一仓买入价格
    last_buy_price = 0  # 上一仓买入价格
    fixed_position_value = 0  # 每仓固定金额（第一仓买入时确定）
    
    # MACD连续增长配置
    MACD_GROWTH_DAYS_REQUIRED = 3  # MACD需要连续3天增长
    
    # 记录下降趋势的最低点
    downtrend_lowest_price = 0  # 当前下降趋势的最低点
    in_downtrend = False  # 是否在下降趋势中
    after_downtrend = False  # 是否处于下降趋势之后（用于追踪MACD增长）
    
    # 标记是否已买入（避免同一下降趋势重复买入）
    downtrend_buy_triggered = False  # 当前下降趋势是否已触发买入
    
    # 标记今天是否刚买入/卖出
    just_bought_today = False
    just_sold_today = False
    
    # MACD连续增长计数
    macd_growth_count = 0
    
    # 记录前一天的持仓状态，用于判断是否是刚进入下降趋势
    prev_day_position = 0
    
    header = f"{'日':<5} {'日期':<10} {'收盘':>8} {'ATR'+str(ATR_PERIOD):>8} {'波动率':>8} {'DIF':>8} {'DEA':>8} {'MACD':>8} {'市场状态':>10} {'持仓':>6} {'操作':>20}"
    log_print(header)
    log_print("-" * 130)
    
    # 遍历每一天进行交易逻辑
    for i in range(len(df)):
        row = df.iloc[i]
        day_num = i + 1
        date_str = row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date'])[:10]
        
        # 重置当天卖出标志和买入标志
        just_sold_today = False
        just_bought_today = False
        
        close_price = row['收盘']
        volatility = row['波动率']
        atr = row['atr']
        dif = row['DIF']
        dea = row['DEA']
        macd = row['MACD']
        market_state = row['market_state'] if pd.notna(row['market_state']) else ""
        state_transition = row['state_transition'] if pd.notna(row['state_transition']) else ""
        
        action = ""  # 操作标记
        
        # ==========================================
        # 新策略：交易逻辑
        # ==========================================
        
        # 0. 清仓逻辑：如果刚变为下降趋势且持有仓位，立即清仓
        # 条件：有持仓、当天未买入、当前是下降趋势、前一天不是下降趋势
        prev_market_state = df.loc[i-1, 'market_state'] if i > 0 else ""
        is_downtrend_start = (market_state == '下降趋势' and prev_market_state != '下降趋势')
        
        if position > 0 and not just_bought_today and is_downtrend_start:
            sell_price = close_price
            cash += position * sell_price
            total_capital = cash  # 清仓后总资金等于现金
            profit = (sell_price - first_buy_price) * position if first_buy_price > 0 else 0
            profit_pct = (sell_price - first_buy_price) / first_buy_price * 100 if first_buy_price > 0 else 0
            print(f"[DEBUG-CLEAR] 日期:{date_str} 清仓后总资金:{total_capital:.0f} 本次盈亏:{profit:.0f}")
            action = f"清仓@{sell_price:.2f} 盈亏:{profit:+.0f}({profit_pct:+.2f}%)"
            trades.append({
                'day': day_num,
                'date': date_str,
                'action': '清仓(下降趋势)',
                'price': sell_price,
                'quantity': position,
                'profit': profit,
                'profit_pct': profit_pct
            })
            # 重置所有仓位状态
            position = 0
            position_count = 0
            first_buy_price = 0
            last_buy_price = 0
            fixed_position_value = 0  # 重置每仓固定金额
            just_sold_today = True
            after_downtrend = False  # 重置追踪状态
        
        # 1. 跟踪下降趋势最低点
        if market_state == '下降趋势':
            if not in_downtrend:
                # 刚进入下降趋势，重置状态
                in_downtrend = True
                after_downtrend = True  # 在下降趋势中也要追踪买入机会
                downtrend_lowest_price = close_price
                downtrend_buy_triggered = False
                macd_growth_count = 0
            else:
                # 更新最低点
                if close_price < downtrend_lowest_price:
                    downtrend_lowest_price = close_price
                    # 创新低后重置MACD计数和买入触发标志
                    macd_growth_count = 0
                    downtrend_buy_triggered = False
        else:
            # 非下降趋势
            if in_downtrend:
                # 刚离开下降趋势，继续追踪MACD增长
                in_downtrend = False
                after_downtrend = True
        
        # 2. 计算MACD连续增长天数（从下降趋势最低点之后开始追踪）
        # 在下降趋势中或离开下降趋势后，都可以计算MACD增长
        if after_downtrend and position == 0 and not downtrend_buy_triggered:
            if i > 0:
                prev_macd = df.loc[i-1, 'MACD']
                if pd.notna(macd) and pd.notna(prev_macd):
                    if macd > prev_macd:
                        macd_growth_count += 1
                    else:
                        macd_growth_count = 0
        
        # 3. 第一仓买入逻辑：从下降趋势最低点开始，MACD连续3天增长就买入
        # 在下降趋势中或离开下降趋势后，都可以买入
        if position == 0 and after_downtrend and not downtrend_buy_triggered:
            if macd_growth_count >= MACD_GROWTH_DAYS_REQUIRED:
                # 计算总资金（现金 + 持仓股票价值）
                total_capital = cash + position * close_price
                # 计算每仓固定金额（基于总资金分8仓，第一仓买入时确定）
                fixed_position_value = total_capital / MAX_POSITIONS
                buy_price = close_price
                buy_quantity = int(fixed_position_value / buy_price / 100) * 100
                print(f"[DEBUG-BUY1] 日期:{date_str} 总资金:{total_capital:.0f} 每仓金额:{fixed_position_value:.0f} 价格:{buy_price:.2f} 计算股数:{buy_quantity}")
                if buy_quantity > 0 and cash >= buy_quantity * buy_price:
                    position = buy_quantity
                    cash -= position * buy_price
                    first_buy_price = buy_price
                    last_buy_price = buy_price
                    position_count = 1
                    downtrend_buy_triggered = True
                    just_bought_today = True
                    action = f"买入第1仓@{buy_price:.2f} x{position}股"
                    trades.append({
                        'day': day_num,
                        'date': date_str,
                        'action': '买入第1仓',
                        'price': buy_price,
                        'quantity': position
                    })
        
        # 4. 加仓逻辑：每上涨5%加一仓
        if position > 0 and position_count < MAX_POSITIONS and not just_bought_today:
            target_price = last_buy_price * (1 + PRICE_INCREASE_PCT)
            if close_price >= target_price:
                # 使用固定的每仓金额（第一仓买入时确定）
                buy_price = close_price
                buy_quantity = int(fixed_position_value / buy_price / 100) * 100
                total_capital = cash + position * close_price
                print(f"[DEBUG-ADD] 日期:{date_str} 总资金:{total_capital:.0f} 每仓金额:{fixed_position_value:.0f} 价格:{buy_price:.2f} 计算股数:{buy_quantity} 当前持仓:{position}")
                if buy_quantity > 0 and cash >= buy_quantity * buy_price:
                    position += buy_quantity
                    cash -= buy_quantity * buy_price
                    position_count += 1
                    last_buy_price = buy_price
                    just_bought_today = True
                    action = f"加仓第{position_count}仓@{buy_price:.2f} x{buy_quantity}股"
                    trades.append({
                        'day': day_num,
                        'date': date_str,
                        'action': f'加仓第{position_count}仓',
                        'price': buy_price,
                        'quantity': buy_quantity
                    })
        
        # 5. 止损逻辑：买入后如果还在下降趋势中，MACD从大变小就卖出
        # 条件：有持仓，且当前市场状态是下降趋势
        if position > 0 and market_state == '下降趋势' and not just_bought_today:
            if i > 0:
                prev_macd = df.loc[i-1, 'MACD']
                if pd.notna(macd) and pd.notna(prev_macd):
                    if macd < prev_macd:  # MACD从大变小
                        sell_price = close_price
                        cash += position * sell_price
                        profit = (sell_price - first_buy_price) * position
                        profit_pct = (sell_price - first_buy_price) / first_buy_price * 100
                        action = f"止损卖出@{sell_price:.2f} 盈亏:{profit:+.0f}({profit_pct:+.2f}%)"
                        trades.append({
                            'day': day_num,
                            'date': date_str,
                            'action': '止损卖出(下降中MACD变小)',
                            'price': sell_price,
                            'quantity': position,
                            'profit': profit,
                            'profit_pct': profit_pct
                        })
                        # 重置所有仓位状态
                        position = 0
                        position_count = 0
                        first_buy_price = 0
                        last_buy_price = 0
                        fixed_position_value = 0  # 重置每仓固定金额
                        just_sold_today = True
                        downtrend_buy_triggered = False  # 允许再次买入
                        after_downtrend = False
        
        # 格式化输出
        line = f"{day_num:<5} {date_str:<10} {close_price:>8.2f}"
        
        # ATR
        if pd.notna(atr):
            line += f" {atr:>8.2f}"
        else:
            line += f" {'N/A':>8}"
        
        # 波动率
        if pd.notna(volatility):
            line += f" {volatility:>8.2f}"
        else:
            line += f" {'N/A':>8}"
        
        # DIF
        if pd.notna(dif):
            line += f" {dif:>8.2f}"
        else:
            line += f" {'N/A':>8}"
        
        # DEA
        if pd.notna(dea):
            line += f" {dea:>8.2f}"
        else:
            line += f" {'N/A':>8}"
        
        # MACD
        if pd.notna(macd):
            line += f" {macd:>8.2f}"
        else:
            line += f" {'N/A':>8}"
        
        # 市场状态和状态转换
        if state_transition:
            line += f" {market_state:>10}|{state_transition}"
        else:
            line += f" {market_state:>10}"
        
        # 持仓
        line += f" {position:>6}"
        
        # 操作
        line += f" {action:>30}"
        
        log_print(line)
    
    # 计算最终收益
    final_value = cash + position * df.iloc[-1]['收盘'] if position > 0 else cash
    total_return = (final_value - 100000) / 100000 * 100
    
    log_print(f"\n{'='*130}")
    log_print(f"【回测完成】")
    log_print(f"初始资金: 100,000")
    log_print(f"最终资产: {final_value:,.2f}")
    log_print(f"总收益率: {total_return:+.2f}%")
    log_print(f"交易次数: {len(trades)}")
    
    # 打印交易明细
    if trades:
        log_print(f"\n{'='*130}")
        log_print("【交易明细】")
        for trade in trades:
            if trade['action'] == '买入第1仓':
                log_print(f"  买入第1仓: {trade['date']} @ {trade['price']:.2f} x {trade['quantity']}股")
            elif trade['action'].startswith('加仓第'):
                log_print(f"  {trade['action']}: {trade['date']} @ {trade['price']:.2f} x {trade['quantity']}股")
            elif trade['action'] == '清仓(下降趋势)':
                log_print(f"  清仓(下降趋势): {trade['date']} @ {trade['price']:.2f} x {trade['quantity']}股 盈亏:{trade['profit']:+.0f}({trade['profit_pct']:+.2f}%)")
            elif trade['action'] == '止损卖出(下降中MACD变小)':
                log_print(f"  止损卖出(下降中MACD变小): {trade['date']} @ {trade['price']:.2f} x {trade['quantity']}股 盈亏:{trade['profit']:+.0f}({trade['profit_pct']:+.2f}%)")
            else:
                log_print(f"  {trade['action']}: {trade['date']} @ {trade['price']:.2f} x {trade['quantity']}股")
    
    log_print(f"{'='*130}")
    
    # 写入文件
    output_file = get_output_file_path()
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(output_lines))
        print(f"\n[文件已保存至: {output_file}]")
    except Exception as e:
        print(f"\n[警告: 无法保存文件 - {e}]")
    
    return final_value, trades


if __name__ == "__main__":
    # 检查是否有 -c 参数（生成图表）
    generate_chart = '-c' in sys.argv or '--chart' in sys.argv
    
    # 运行回测
    result = run_backtest()
    
    # 如果指定了 -c 参数，自动生成图表并打开
    if result and generate_chart:
        print("\n[正在生成图表...]")
        try:
            # 调用 generate_chart_with_states.py 生成带市场状态的图表
            script_dir = os.path.dirname(os.path.abspath(__file__))
            chart_script = os.path.join(script_dir, 'generate_chart_with_states.py')
            
            if os.path.exists(chart_script):
                subprocess.run([sys.executable, chart_script], check=True)
                
                # 打开生成的HTML文件
                html_file = os.path.join(script_dir, 'stock_chart_with_states.html')
                if os.path.exists(html_file):
                    print(f"[正在打开图表: {html_file}]")
                    if os.name == 'nt':  # Windows
                        os.startfile(html_file)
                    elif os.name == 'posix':  # macOS/Linux
                        subprocess.run(['open', html_file])
                else:
                    print(f"[警告: 未找到图表文件 {html_file}]")
            else:
                print(f"[警告: 未找到图表生成脚本 {chart_script}]")
        except Exception as e:
            print(f"[生成图表时出错: {e}]")
