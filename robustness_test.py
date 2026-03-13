"""
策略鲁棒性测试脚本

测试逻辑：
1. 遍历 cache 下的每只股票作为初始买入候选
2. 对每只股票作为起始点进行独立回测
3. 如果某只股票在回测开始时没有数据，则跳过，后续有数据了再买入
4. 比较不同起始点的回测结果，验证策略鲁棒性

输出：多种回测数据的对比分析
"""

import pandas as pd
import numpy as np
import os
import json
import random
from glob import glob
from datetime import datetime
from collections import defaultdict
from multiprocessing import Pool, cpu_count
from ana_stocks import (
    BACKTEST_START_DATE, BACKTEST_END_DATE, START_DATE, END_DATE
)

# 导入策略参数
from buy_socks_sell_bodong import (
    calculate_slope_atr, SELL_RATIO_THRESHOLD, BUY_DECLINE_DAYS_REQUIRED
)

# 导入成交量策略参数
try:
    from analyze_volume import (
        BUY_VOLUME_THRESHOLD, SELL_CONSECUTIVE_DAYS, SELL_VOLUME_THRESHOLD
    )
    HAS_VOLUME_STRATEGY = True
except ImportError:
    HAS_VOLUME_STRATEGY = False
    BUY_VOLUME_THRESHOLD = 150
    SELL_CONSECUTIVE_DAYS = 3
    SELL_VOLUME_THRESHOLD = 200

# 配置参数
CACHE_DIR = "./cache"
INITIAL_CAPITAL = 100000  # 初始资金

# 确定回测日期范围
if BACKTEST_START_DATE is None and BACKTEST_END_DATE is None:
    ACTUAL_START_DATE = START_DATE
    ACTUAL_END_DATE = END_DATE
else:
    ACTUAL_START_DATE = BACKTEST_START_DATE
    ACTUAL_END_DATE = BACKTEST_END_DATE

# 配置：是否使用多进程
# Windows 上多进程可能不稳定，如果遇到问题可以设置为 False
USE_MULTIPROCESSING = True

# 配置：最大进程数
# 设置为 None 则自动使用 CPU 核心数
# 建议设置为 CPU 核心数的 50%-100%
MAX_PROCESSES = 8  # 例如: 16, 24, 32 或 None(自动)

# 配置：是否使用随机股票组合测试
# 如果为 True，每次测试会从所有股票中随机选择 RANDOM_STOCK_COUNT 个股票
# 如果为 False，使用所有股票进行测试
ENABLE_RANDOM_STOCK_SELECTION = True

# 配置：随机选择的股票数量
# 例如设置为 10，则每次测试使用 10 只随机股票
RANDOM_STOCK_COUNT = 40

# 配置：测试轮数
# 设置为 None 则测试次数等于股票数量
# 设置为具体数字则只进行指定轮数的测试
TEST_ROUNDS = None  # 例如: 8, 16, 32 或 None(等于股票数量)

# 配置：指定股票组合测试
# 如果设置，将只使用这些股票进行测试，忽略随机选择
# 格式: ['601016', '600063', '000543', ...]
# SPECIFIC_STOCKS = ['302132', '603496', '000543']
SPECIFIC_STOCKS = None
# 配置：是否输出详细交易记录
# 开启后会打印每一笔买卖的详细信息
ENABLE_DETAILED_TRADES = True  # 设置为 True 输出详细交易记录

# 配置：随机种子
# 设置为 None 则每次运行结果不同
# 设置为具体数字则每次运行结果一致（用于复现结果）
RANDOM_SEED = None  # 例如: 42, 123, 456 或 None(随机)

# 配置：是否允许多股票同时持仓
# 开启后，当多只股票同时满足买入条件时，会平均分配资金买入多只股票
# 关闭后，同一时间只买入一只股票（按随机顺序）
ENABLE_MULTI_STOCK_HOLDING = True  # 设置为 True 启用多股票同时持仓

# 配置：最小买入股数
# 设置买入的最小股数限制，不满这个数量则不买入
MIN_SHARES = 100  # 例如: 100, 200, 500 或 None(不限制)

# 配置：选择策略类型
# 'volatility': 波动率策略（从 buy_socks_sell_bodong.py 导入）
# 'volume': 成交量策略（从 analyze_volume.py 导入）
STRATEGY_TYPE = 'volume'  # 可选值: 'volatility', 'volume'


def load_stock_data(stock_code, cache_dir=CACHE_DIR):
    """从缓存加载股票数据"""
    cache_file = os.path.join(cache_dir, f"{stock_code}_daily.json")
    if not os.path.exists(cache_file):
        return None
    
    try:
        with open(cache_file, 'r', encoding='utf-8') as f:
            cache_data = json.load(f)
        
        data = cache_data.get('daily_data', cache_data)
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)
        
        # 根据策略类型计算技术指标
        if STRATEGY_TYPE == 'volatility':
            # 波动率策略需要计算波动率
            df['ma20'] = df['收盘'].rolling(window=20, min_periods=20).mean()
            
            prev_close = df['收盘'].shift(1)
            tr1 = df['最高'] - df['最低']
            tr2 = (df['最高'] - prev_close).abs()
            tr3 = (df['最低'] - prev_close).abs()
            df['tr'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            df['atr14'] = df['tr'].rolling(window=14, min_periods=14).mean()
            
            df = calculate_slope_atr(df, ma_period=20, atr_period=14, n=5)
        elif STRATEGY_TYPE == 'volume':
            # 成交量策略只需要计算成交量变化
            df['成交量变化%'] = df['成交量'].pct_change() * 100
        else:
            raise ValueError(f"未知的策略类型: {STRATEGY_TYPE}")
        
        return df
    except Exception as e:
        print(f"加载股票 {stock_code} 数据失败: {e}")
        return None


def select_random_stocks(all_stock_codes, num_stocks=10):
    """
    随机选择股票组合
    
    Args:
        all_stock_codes: 所有可用的股票代码列表
        num_stocks: 需要随机选择的股票数量（默认10个）
    
    Returns:
        随机选择的股票列表
    """
    # 如果可用股票不足，返回所有可用股票
    if len(all_stock_codes) <= num_stocks:
        selected_stocks = all_stock_codes
    else:
        # 随机选择 num_stocks 个股票
        selected_stocks = random.sample(all_stock_codes, num_stocks)
    
    return selected_stocks


class StockStrategy:
    """单只股票策略状态机"""
    
    def __init__(self, stock_code, df):
        self.stock_code = stock_code
        self.df = df
        self.idx = 0
        
        # 交易状态
        self.position = 0
        self.buy_price = 0
        self.cash = 0
        
        # 波动率策略的买入条件计数器
        self.volatility_declining_days = 0
        self.prev_volatility = None
        
        # 成交量策略的连续正增长天数
        self.consecutive_volume_growth_days = 0
        
        # 交易记录
        self.trades = []
    
    def update_state(self, current_idx):
        """
        只更新状态，不执行买入卖出操作
        用于跟踪波动率变化和买入条件计数器
        """
        if current_idx >= len(self.df):
            return
        
        # 波动率策略的状态更新
        if STRATEGY_TYPE == 'volatility':
            volatility = self.df.iloc[current_idx]['波动率']
            
            if pd.isna(volatility):
                return
            
            prev_volatility = self.df.iloc[current_idx-1]['波动率'] if current_idx > 0 else None
            
            # 更新买入条件计数器
            is_volatility_increasing_toward_zero = False
            if prev_volatility is not None and not pd.isna(prev_volatility):
                if volatility > prev_volatility and volatility < 0 and prev_volatility < 0:
                    is_volatility_increasing_toward_zero = True
            
            if is_volatility_increasing_toward_zero:
                self.volatility_declining_days += 1
            else:
                self.volatility_declining_days = 0
            
            self.prev_volatility = volatility
        
        # 成交量策略的状态更新
        elif STRATEGY_TYPE == 'volume':
            volume_change = self.df.iloc[current_idx]['成交量变化%']
            
            # 更新连续正增长天数
            if current_idx > 0 and not pd.isna(volume_change) and volume_change > 0:
                self.consecutive_volume_growth_days += 1
            else:
                self.consecutive_volume_growth_days = 0
    
    def process_day(self, current_idx):
        """处理单日的买入卖出逻辑"""
        if current_idx >= len(self.df):
            return None
        
        self.idx = current_idx
        row = self.df.iloc[current_idx]
        
        date_str = row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date'])[:10]
        close_price = row['收盘']
        
        trade_info = None
        
        # 波动率策略的买入卖出逻辑
        if STRATEGY_TYPE == 'volatility':
            volatility = row['波动率']
            
            if pd.isna(volatility):
                return None
            
            prev_volatility = self.df.iloc[current_idx-1]['波动率'] if current_idx > 0 else None
            
            is_volatility_increasing_toward_zero = False
            if prev_volatility is not None and not pd.isna(prev_volatility):
                if volatility > prev_volatility and volatility < 0 and prev_volatility < 0:
                    is_volatility_increasing_toward_zero = True
            
            # 卖出逻辑
            if self.position > 0:
                should_sell = False
                sell_reason = ""
                
                if volatility > 0 and prev_volatility is not None and not pd.isna(prev_volatility):
                    if volatility < prev_volatility:
                        volatility_ratio = volatility / prev_volatility if prev_volatility > 0 else 1.0
                        if volatility_ratio <= SELL_RATIO_THRESHOLD:
                            should_sell = True
                            sell_reason = "比率卖出"
                
                if should_sell:
                    profit = (close_price - self.buy_price) * self.position
                    trade_info = {
                        'date': date_str,
                        'stock': self.stock_code,
                        'action': '卖出',
                        'price': close_price,
                        'shares': self.position,
                        'profit': profit,
                        'reason': sell_reason
                    }
                    self.trades.append(trade_info)
                    self.position = 0
                    self.buy_price = 0
                    self.volatility_declining_days = 0
                    return trade_info
            
            # 买入逻辑
            if self.position == 0:
                if self.volatility_declining_days >= BUY_DECLINE_DAYS_REQUIRED and volatility < 0:
                    shares = int(self.cash / close_price) if self.cash > 0 else 0
                    if shares > 0:
                        self.position = shares
                        self.buy_price = close_price
                        self.volatility_declining_days = 0
                        
                        trade_info = {
                            'date': date_str,
                            'stock': self.stock_code,
                            'action': '买入',
                            'price': close_price,
                            'shares': shares,
                            'profit': 0,
                            'reason': 'A条件'
                        }
                        self.trades.append(trade_info)
                        return trade_info
        
        # 成交量策略的买入卖出逻辑
        elif STRATEGY_TYPE == 'volume':
            volume_change = row['成交量变化%']
            
            if pd.isna(volume_change):
                return None
            
            # 卖出逻辑：连续3天成交量正增长，且至少有一天涨幅 > 200%
            if self.position > 0 and self.consecutive_volume_growth_days >= SELL_CONSECUTIVE_DAYS:
                if current_idx >= SELL_CONSECUTIVE_DAYS:
                    recent_changes = self.df.loc[current_idx-SELL_CONSECUTIVE_DAYS+1:current_idx+1, '成交量变化%'].values
                    if any(change > SELL_VOLUME_THRESHOLD for change in recent_changes if not pd.isna(change)):
                        profit = (close_price - self.buy_price) * self.position
                        trade_info = {
                            'date': date_str,
                            'stock': self.stock_code,
                            'action': '卖出',
                            'price': close_price,
                            'shares': self.position,
                            'profit': profit,
                            'reason': '成交量连续增长'
                        }
                        self.trades.append(trade_info)
                        self.position = 0
                        self.buy_price = 0
                        self.consecutive_volume_growth_days = 0
                        return trade_info
            
            # 买入逻辑：成交量涨幅超过阈值
            if self.position == 0 and volume_change > BUY_VOLUME_THRESHOLD:
                shares = int(self.cash / close_price) if self.cash > 0 else 0
                if shares > 0:
                    self.position = shares
                    self.buy_price = close_price
                    self.consecutive_volume_growth_days = 0
                    
                    trade_info = {
                        'date': date_str,
                        'stock': self.stock_code,
                        'action': '买入',
                        'price': close_price,
                        'shares': shares,
                        'profit': 0,
                        'reason': f'成交量涨幅{volume_change:.2f}%'
                    }
                    self.trades.append(trade_info)
                    return trade_info
        
        return None


def run_backtest_with_start_stock(start_stock_code, all_strategies, all_dates):
    """
    使用指定的起始股票进行回测
    逻辑：等待start_stock_code首次触发买入条件时才买入
    返回: (final_value, total_return, all_trades, trade_count, daily_values)
    """
    cash = INITIAL_CAPITAL
    current_strategy = None
    all_trades = []
    daily_values = []  # 记录每日市值
    
    # 标记是否已经进行过首次买入（即start_stock_code是否已经触发过买入）
    has_first_buy = False
    
    for current_date in all_dates:
        date_str = current_date.strftime('%Y-%m-%d')
        year = current_date.year
        
        # 如果有持仓，先处理当前股票的卖出
        if current_strategy is not None:
            if current_date in current_strategy.df['date'].values:
                row_idx = current_strategy.df[current_strategy.df['date'] == current_date].index[0]
                current_strategy.cash = cash
                
                result = current_strategy.process_day(row_idx)
                if result and result['action'] == '卖出':
                    cash += result['shares'] * result['price']
                    all_trades.append(result)
                    current_strategy = None
        
        # 如果没有持仓，寻找买入机会
        if current_strategy is None:
            if not has_first_buy:
                # 还没有首次买入，只关注start_stock_code的买入信号
                if start_stock_code in all_strategies:
                    start_strategy = all_strategies[start_stock_code]
                    if current_date in start_strategy.df['date'].values:
                        # 尝试买入起始股票
                        row_idx = start_strategy.df[start_strategy.df['date'] == current_date].index[0]
                        start_strategy.cash = cash
                        
                        result = start_strategy.process_day(row_idx)
                        if result and result['action'] == '买入':
                            cash -= result['shares'] * result['price']
                            all_trades.append(result)
                            current_strategy = start_strategy
                            has_first_buy = True
            else:
                # 已经有过首次买入（start_stock_code已触发过），现在可以遍历所有股票
                for code, strategy in all_strategies.items():
                    if current_date in strategy.df['date'].values:
                        row_idx = strategy.df[strategy.df['date'] == current_date].index[0]
                        strategy.cash = cash
                        
                        result = strategy.process_day(row_idx)
                        if result and result['action'] == '买入':
                            cash -= result['shares'] * result['price']
                            all_trades.append(result)
                            current_strategy = strategy
                            break
        
        # 计算当日市值
        if current_strategy is not None and current_date in current_strategy.df['date'].values:
            row = current_strategy.df[current_strategy.df['date'] == current_date].iloc[0]
            market_value = cash + current_strategy.position * row['收盘']
        else:
            market_value = cash
        
        daily_values.append({
            'date': current_date,
            'year': year,
            'market_value': market_value
        })
    
    # 计算最终市值
    final_value = cash
    if current_strategy is not None and current_strategy.position > 0:
        final_value += current_strategy.position * current_strategy.buy_price
    
    total_return = (final_value - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100
    trade_count = len([t for t in all_trades if t['action'] == '卖出'])
    
    return final_value, total_return, all_trades, trade_count, daily_values


def run_single_backtest(args):
    """
    多进程回测函数
    args: (test_id, stock_data_dicts, all_dates_list, selected_stocks, seed)
    """
    test_id, stock_data_dicts, all_dates_list, selected_stocks, seed = args
    
    INITIAL_CAPITAL = 100000
    
    # 设置随机种子
    random.seed(seed + test_id)
    
    # 从字典重建DataFrame
    all_strategies = {}
    for code, df_dict in stock_data_dicts.items():
        df = pd.DataFrame(df_dict)
        df['date'] = pd.to_datetime(df['date'])
        all_strategies[code] = StockStrategy(code, df)
    
    # 如果指定了股票组合，则只使用这些股票
    if selected_stocks is not None:
        all_strategies = {code: all_strategies[code] for code in selected_stocks if code in all_strategies}
    
    all_dates = pd.to_datetime(all_dates_list)
    
    # 运行回测
    cash = INITIAL_CAPITAL
    current_strategies = {}  # 改为字典，存储多只持仓股票 {stock_code: strategy}
    all_trades = []
    daily_values = []
    
    for current_date in all_dates:
        date_str = current_date.strftime('%Y-%m-%d')
        year = current_date.year
        
        # 每天都更新所有股票的状态（即使没有持仓）
        # 这样可以正确跟踪波动率变化和买入条件计数器
        for code, strategy in all_strategies.items():
            if current_date in strategy.df['date'].values:
                row_idx = strategy.df[strategy.df['date'] == current_date].index[0]
                strategy.update_state(row_idx)
        
        # 如果有持仓，先处理当前股票的卖出
        for stock_code, strategy in list(current_strategies.items()):
            if current_date in strategy.df['date'].values:
                row_idx = strategy.df[strategy.df['date'] == current_date].index[0]
                strategy.cash = cash
                
                result = strategy.process_day(row_idx)
                if result and result['action'] == '卖出':
                    cash += result['shares'] * result['price']
                    all_trades.append(result)
                    del current_strategies[stock_code]
        
        # 寻找买入机会（不管当前是否有持仓，只要有空闲资金就可以买入）
        # 打乱股票顺序，避免总是优先买入同一只股票
        stock_codes = list(all_strategies.keys())
        random.shuffle(stock_codes)
        
        if ENABLE_MULTI_STOCK_HOLDING:
            # 找出所有满足买入条件的股票（排除已持仓的）
            buy_candidates = []
            for code in stock_codes:
                # 跳过已持仓的股票
                if code in current_strategies:
                    continue
                    
                strategy = all_strategies[code]
                if current_date in strategy.df['date'].values:
                    row_idx = strategy.df[strategy.df['date'] == current_date].index[0]
                    strategy.cash = cash
                    
                    result = strategy.process_day(row_idx)
                    if result and result['action'] == '买入':
                        buy_candidates.append((code, strategy, result))
            
            # 如果有多只股票满足条件，平均分配可用资金
            if buy_candidates:
                cash_per_stock = cash / len(buy_candidates)
                for code, strategy, result in buy_candidates:
                    # 重新计算买入股数
                    shares = int(cash_per_stock / result['price'])
                    # 检查是否满足最小股数限制
                    if MIN_SHARES is not None and shares < MIN_SHARES:
                        continue
                    if shares > 0:
                        strategy.position = shares
                        strategy.buy_price = result['price']
                        strategy.volatility_declining_days = 0
                        cash -= shares * result['price']
                        
                        # 更新交易记录
                        trade_info = result.copy()
                        trade_info['shares'] = shares
                        all_trades.append(trade_info)
                        current_strategies[code] = strategy
        else:
            # 单股票模式：只买入一只股票（排除已持仓的）
            for code in stock_codes:
                # 跳过已持仓的股票
                if code in current_strategies:
                    continue
                    
                strategy = all_strategies[code]
                if current_date in strategy.df['date'].values:
                    row_idx = strategy.df[strategy.df['date'] == current_date].index[0]
                    strategy.cash = cash
                    
                    result = strategy.process_day(row_idx)
                    if result and result['action'] == '买入':
                        # 检查是否满足最小股数限制
                        if MIN_SHARES is not None and result['shares'] < MIN_SHARES:
                            continue
                        cash -= result['shares'] * result['price']
                        all_trades.append(result)
                        current_strategies[code] = strategy
                        break
        
        # 计算当日市值（所有持仓股票的总市值）
        market_value = cash
        for stock_code, strategy in current_strategies.items():
            if current_date in strategy.df['date'].values:
                row = strategy.df[strategy.df['date'] == current_date].iloc[0]
                market_value += strategy.position * row['收盘']
        
        daily_values.append({
            'date': current_date,
            'year': year,
            'market_value': market_value
        })
    
    # 计算最终市值（现金 + 所有持仓股票的市值）
    final_value = cash
    for stock_code, strategy in current_strategies.items():
        if strategy.position > 0:
            final_value += strategy.position * strategy.buy_price
    
    total_return = (final_value - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100
    trade_count = len([t for t in all_trades if t['action'] == '卖出'])
    
    # 计算首次买入时间
    first_buy_date = None
    for t in all_trades:
        if t['action'] == '买入':
            first_buy_date = t['date']
            break
    
    # 计算年度收益
    df_daily = pd.DataFrame(daily_values)
    years = sorted(df_daily['year'].unique())
    year_returns = []
    for year in years:
        year_data = df_daily[df_daily['year'] == year]
        if len(year_data) > 0:
            start_value = year_data.iloc[0]['market_value']
            end_value = year_data.iloc[-1]['market_value']
            year_profit = end_value - start_value
            year_return = (end_value - start_value) / start_value * 100 if start_value > 0 else 0
            year_returns.append({
                'year': year,
                'start': start_value,
                'end': end_value,
                'profit': year_profit,
                'return': year_return
            })
    
    return {
        'test_id': test_id,
        'selected_stocks': selected_stocks if selected_stocks is not None else list(all_strategies.keys()),
        'first_buy_date': first_buy_date,
        'final_value': final_value,
        'total_return': total_return,
        'trade_count': trade_count,
        'year_returns': year_returns,
        'all_trades': all_trades  # 返回所有交易记录
    }


def run_robustness_test():
    """运行鲁棒性测试"""
    
    # 设置随机种子
    if RANDOM_SEED is not None:
        random.seed(RANDOM_SEED)
        actual_seed = RANDOM_SEED
    else:
        # 生成随机种子并打印，方便复现
        actual_seed = random.randint(0, 999999)
        random.seed(actual_seed)
    
    print(f"使用随机种子: {actual_seed}\n")
    
    # 打印策略类型
    strategy_name = '波动率策略' if STRATEGY_TYPE == 'volatility' else '成交量策略'
    print(f"策略类型: {STRATEGY_TYPE} ({strategy_name})")
    if STRATEGY_TYPE == 'volume':
        print(f"买入规则: 成交量涨幅 > {BUY_VOLUME_THRESHOLD}%")
        print(f"卖出规则: 连续 {SELL_CONSECUTIVE_DAYS} 天成交量正增长，且至少有一天涨幅 > {SELL_VOLUME_THRESHOLD}%")
    print()
    
    # 获取所有缓存的股票代码
    cache_files = glob(os.path.join(CACHE_DIR, "*_daily.json"))
    stock_codes = [os.path.basename(f).replace("_daily.json", "") for f in cache_files]
    
    print(f"找到 {len(stock_codes)} 只股票")
    print(f"回测区间: {ACTUAL_START_DATE.strftime('%Y-%m-%d')} 至 {ACTUAL_END_DATE.strftime('%Y-%m-%d')}")
    print(f"将对每只股票作为起始点进行 {len(stock_codes)} 次独立回测\n")
    
    # 加载所有股票数据
    all_strategies = {}
    for code in stock_codes:
        df = load_stock_data(code)
        if df is not None and len(df) >= 60:
            df = df[(df['date'] >= ACTUAL_START_DATE) & (df['date'] <= ACTUAL_END_DATE)].copy()
            if len(df) >= 20:
                df = df.reset_index(drop=True)
                all_strategies[code] = StockStrategy(code, df)
    
    print(f"成功加载 {len(all_strategies)} 只股票数据")
    
    if len(all_strategies) == 0:
        print("没有可用的股票数据")
        return
    
    # 获取所有交易日的并集
    all_dates_set = set()
    for strategy in all_strategies.values():
        all_dates_set.update(strategy.df['date'].tolist())
    all_dates = sorted(list(all_dates_set))
    
    # 将股票数据转换为字典格式（可序列化，用于多进程）
    stock_data_dicts = {}
    for code, strategy in all_strategies.items():
        stock_data_dicts[code] = strategy.df.to_dict('records')
    
    # 将日期转换为字符串列表
    all_dates_list = [d.strftime('%Y-%m-%d') for d in all_dates]
    
    # 获取所有可用的股票代码列表
    all_stock_codes = list(all_strategies.keys())
    
    # 准备多进程参数
    task_args = []
    
    if SPECIFIC_STOCKS is not None:
        # 使用指定的股票组合进行测试
        # 过滤掉不存在的股票
        valid_stocks = [code for code in SPECIFIC_STOCKS if code in all_strategies]
        if len(valid_stocks) == 0:
            print(f"指定的股票组合中没有有效股票: {SPECIFIC_STOCKS}")
            return
        
        print(f"使用指定股票组合进行测试: {valid_stocks}")
        print(f"共 {len(valid_stocks)} 只股票\n")
        
        # 只进行一轮测试，使用指定的股票组合
        task_args.append((0, stock_data_dicts, all_dates_list, valid_stocks, actual_seed))
        
    elif ENABLE_RANDOM_STOCK_SELECTION:
        # 确定测试轮数
        if TEST_ROUNDS is None:
            num_tests = len(all_stock_codes)  # 测试次数等于股票数量
        else:
            num_tests = TEST_ROUNDS
        
        print(f"将进行 {num_tests} 次随机组合回测\n")
        
        for test_id in range(num_tests):
            # 随机选择指定数量的股票
            selected_stocks = select_random_stocks(all_stock_codes, num_stocks=RANDOM_STOCK_COUNT)
            task_args.append((test_id, stock_data_dicts, all_dates_list, selected_stocks, actual_seed))
    else:
        # 使用所有股票
        for test_id, code in enumerate(all_stock_codes):
            selected_stocks = [code]
            task_args.append((test_id, stock_data_dicts, all_dates_list, selected_stocks, actual_seed))
    
    # 存储所有回测结果
    results = []
    
    print("\n" + "="*80)
    if USE_MULTIPROCESSING:
        print("开始鲁棒性测试 (多进程并行)")
    else:
        print("开始鲁棒性测试 (单进程)")
    if ENABLE_RANDOM_STOCK_SELECTION:
        print(f"模式: 随机股票组合 (每个测试{RANDOM_STOCK_COUNT}只股票)")
    else:
        print("模式: 全部股票")
    print("="*80)
    
    # 使用多进程或单进程
    use_multiprocessing = USE_MULTIPROCESSING
    if use_multiprocessing:
        # 获取CPU核心数，根据配置确定进程数
        if MAX_PROCESSES is None:
            num_processes = cpu_count()
        else:
            num_processes = min(cpu_count(), MAX_PROCESSES)
        print(f"使用 {num_processes} 个进程并行回测\n")
        
        # 使用多进程并行回测
        try:
            # 先测试一下多进程是否能正常工作（使用1个进程测试）
            print("测试多进程环境...")
            test_pool = Pool(processes=1)
            test_pool.close()
            test_pool.join()
            print("多进程环境测试通过\n")
            
            # 正式运行
            with Pool(processes=num_processes) as pool:
                results = pool.map(run_single_backtest, task_args)
        except Exception as e:
            print(f"\n多进程执行出错: {e}")
            print("错误类型:", type(e).__name__)
            print("自动切换到单进程模式运行...\n")
            use_multiprocessing = False
            
    if not use_multiprocessing:
        # 单进程运行
        print(f"使用单进程运行 {len(task_args)} 次回测\n")
        results = []
        for i, args in enumerate(task_args):
            print(f"[{i+1}/{len(task_args)}] 正在回测测试 #{args[0]}...")
            try:
                result = run_single_backtest(args)
                results.append(result)
                print(f"[测试#{result['test_id']}] 完成 - 收益率: {result['total_return']:.2f}%")
            except Exception as e:
                print(f"[测试#{args[0]}] 失败: {e}")
    
    # 输出完成信息（如果多进程成功）
    if use_multiprocessing and results and len(results) == len(task_args):
        for r in results:
            print(f"[测试#{r['test_id']}] 完成 - 收益率: {r['total_return']:.2f}%")
    
    # 输出汇总统计
    print("\n" + "="*80)
    print("鲁棒性测试结果汇总")
    print("="*80)
    
    # 按收益率排序
    results_sorted = sorted(results, key=lambda x: x['total_return'], reverse=True)
    
    print(f"\n{'排名':<6} {'测试ID':<8} {'首次买入':<12} {'最终市值':>15} {'总收益率':>12} {'交易次数':>10}")
    print("-" * 80)
    
    for i, r in enumerate(results_sorted):
        first_buy = r['first_buy_date'] if r['first_buy_date'] else 'N/A'
        print(f"{i+1:<6} #{r['test_id']:<6} {first_buy:<12} {r['final_value']:>15,.2f} {r['total_return']:>11.2f}% {r['trade_count']:>10}")
        if ENABLE_RANDOM_STOCK_SELECTION:
            print(f"       股票组合: {', '.join(r['selected_stocks'])}")
    
    # 按交易次数排序
    results_sorted_by_trades = sorted(results, key=lambda x: x['trade_count'], reverse=True)
    
    print(f"\n{'排名':<6} {'测试ID':<8} {'首次买入':<12} {'最终市值':>15} {'总收益率':>12} {'交易次数':>10}")
    print("-" * 80)
    print("(按交易次数排序)")
    
    for i, r in enumerate(results_sorted_by_trades):
        first_buy = r['first_buy_date'] if r['first_buy_date'] else 'N/A'
        print(f"{i+1:<6} #{r['test_id']:<6} {first_buy:<12} {r['final_value']:>15,.2f} {r['total_return']:>11.2f}% {r['trade_count']:>10}")
        if ENABLE_RANDOM_STOCK_SELECTION:
            print(f"       股票组合: {', '.join(r['selected_stocks'])}")
    
    # 统计指标
    returns = [r['total_return'] for r in results]
    final_values = [r['final_value'] for r in results]
    trade_counts = [r['trade_count'] for r in results]
    
    print("\n" + "="*80)
    print("统计指标")
    print("="*80)
    print(f"回测次数: {len(results)}")
    print(f"\n最终市值统计:")
    print(f"  最高: {max(final_values):,.2f}")
    print(f"  最低: {min(final_values):,.2f}")
    print(f"  平均: {np.mean(final_values):,.2f}")
    print(f"  中位数: {np.median(final_values):,.2f}")
    print(f"  标准差: {np.std(final_values):,.2f}")
    
    print(f"\n收益率统计:")
    print(f"  最高: {max(returns):.2f}%")
    print(f"  最低: {min(returns):.2f}%")
    print(f"  平均: {np.mean(returns):.2f}%")
    print(f"  中位数: {np.median(returns):.2f}%")
    print(f"  标准差: {np.std(returns):.2f}%")
    
    print(f"\n交易次数统计:")
    print(f"  最多: {max(trade_counts)}")
    print(f"  最少: {min(trade_counts)}")
    print(f"  平均: {np.mean(trade_counts):.1f}")
    
    # 收益率与交易次数关系分析
    print("\n" + "="*80)
    print("收益率与交易次数关系分析")
    print("="*80)
    
    # 计算相关系数
    if len(results) > 1:
        correlation = np.corrcoef(returns, trade_counts)[0, 1]
        print(f"\n相关系数: {correlation:.4f}")
        if correlation > 0.3:
            print("  -> 正相关：交易次数越多，收益率越高")
        elif correlation < -0.3:
            print("  -> 负相关：交易次数越多，收益率越低")
        else:
            print("  -> 相关性较弱")
    
    # 按交易次数区间统计
    min_trades = min(trade_counts)
    max_trades = max(trade_counts)
    
    # 根据交易次数范围创建区间
    trade_range = max_trades - min_trades
    if trade_range > 0:
        num_bins = min(4, len(set(trade_counts)))
        bin_size = trade_range / num_bins
        trade_bins = []
        for i in range(num_bins):
            low = min_trades + i * bin_size
            high = min_trades + (i + 1) * bin_size
            if i == num_bins - 1:
                high = max_trades + 1
            trade_bins.append((int(low), int(high)))
    else:
        trade_bins = [(min_trades, max_trades + 1)]
    
    print(f"\n{'交易次数区间':<20} {'次数':<8} {'平均收益率':<15} {'最高收益率':<15} {'最低收益率':<15}")
    print("-" * 80)
    
    for low, high in trade_bins:
        bin_results = [r for r in results if low <= r['trade_count'] < high]
        if bin_results:
            bin_returns = [r['total_return'] for r in bin_results]
            avg_return = np.mean(bin_returns)
            max_return = max(bin_returns)
            min_return = min(bin_returns)
            if high - 1 > low:
                range_label = f"{low}-{high-1}"
            else:
                range_label = str(low)
            print(f"{range_label:<20} {len(bin_results):<8} {avg_return:>14.2f}% {max_return:>14.2f}% {min_return:>14.2f}%")
    
    # 收益率分布
    print("\n" + "="*80)
    print("收益率分布")
    print("="*80)
    
    bins = [-float('inf'), 0, 50, 100, 200, 300, 500, float('inf')]
    labels = ['<0%', '0-50%', '50-100%', '100-200%', '200-300%', '300-500%', '>500%']
    
    distribution = defaultdict(int)
    for r in returns:
        for i, (low, high) in enumerate(zip(bins[:-1], bins[1:])):
            if low <= r < high:
                distribution[labels[i]] += 1
                break
    
    for label in labels:
        count = distribution[label]
        percentage = count / len(results) * 100
        bar = '█' * int(percentage / 2)
        print(f"{label:<10} {count:>3} ({percentage:>5.1f}%) {bar}")
    
    # 年度收益统计
    print("\n" + "="*80)
    print("年度收益统计")
    print("="*80)
    
    # 收集所有年份
    all_years = set()
    for r in results:
        for yr in r['year_returns']:
            all_years.add(yr['year'])
    all_years = sorted(list(all_years))
    
    if all_years:
        print(f"\n{'年份':<8}", end='')
        for year in all_years:
            print(f"{year:<12}", end='')
        print()
        print("-" * (8 + 12 * len(all_years)))
        
        # 每个测试的年度收益
        for r in results_sorted[:10]:  # 只显示前10名
            print(f"#{r['test_id']:<6}", end='')
            year_dict = {yr['year']: yr for yr in r['year_returns']}
            for year in all_years:
                if year in year_dict:
                    ret = year_dict[year]['return']
                    print(f"{ret:>10.1f}%", end='  ')
                else:
                    print(f"{'N/A':>10}", end='  ')
            print()
        
        # 年度统计
        print("\n" + "-" * (8 + 12 * len(all_years)))
        print(f"{'平均':<8}", end='')
        for year in all_years:
            year_returns_list = []
            for r in results:
                for yr in r['year_returns']:
                    if yr['year'] == year:
                        year_returns_list.append(yr['return'])
                        break
            if year_returns_list:
                avg_ret = np.mean(year_returns_list)
                print(f"{avg_ret:>10.1f}%", end='  ')
            else:
                print(f"{'N/A':>10}", end='  ')
        print()
        
        print(f"{'最高':<8}", end='')
        for year in all_years:
            year_returns_list = []
            for r in results:
                for yr in r['year_returns']:
                    if yr['year'] == year:
                        year_returns_list.append(yr['return'])
                        break
            if year_returns_list:
                max_ret = max(year_returns_list)
                print(f"{max_ret:>10.1f}%", end='  ')
            else:
                print(f"{'N/A':>10}", end='  ')
        print()
        
        print(f"{'最低':<8}", end='')
        for year in all_years:
            year_returns_list = []
            for r in results:
                for yr in r['year_returns']:
                    if yr['year'] == year:
                        year_returns_list.append(yr['return'])
                        break
            if year_returns_list:
                min_ret = min(year_returns_list)
                print(f"{min_ret:>10.1f}%", end='  ')
            else:
                print(f"{'N/A':>10}", end='  ')
        print()
    
    # 输出详细交易记录（如果启用）
    if ENABLE_DETAILED_TRADES and results:
        print("\n" + "="*80)
        print("详细交易记录")
        print("="*80)
        
        for r in results:
            print(f"\n{'='*80}")
            print(f"测试 #{r['test_id']} - 股票组合: {', '.join(r['selected_stocks'])}")
            print(f"{'='*80}")
            print(f"{'日期':<12} {'股票':<10} {'操作':<8} {'价格':>10} {'股数':>8} {'盈亏':>12} {'原因':<15}")
            print("-" * 80)
            
            for t in r['all_trades']:
                profit_str = f"{t.get('profit', 0):,.2f}" if t['action'] == '卖出' else '-'
                reason = t.get('reason', '')
                print(f"{t.get('date', ''):<12} {t['stock']:<10} {t['action']:<8} {t['price']:>10.2f} {t['shares']:>8} {profit_str:>12} {reason:<15}")
            
            print(f"{'-'*80}")
            print(f"总收益率: {r['total_return']:.2f}% | 交易次数: {r['trade_count']} | 最终市值: {r['final_value']:,.2f}")
    
    print("\n" + "="*80)
    print("结论")
    print("="*80)
    
    positive_count = sum(1 for r in returns if r > 0)
    negative_count = len(returns) - positive_count
    
    print(f"盈利次数: {positive_count}/{len(results)} ({positive_count/len(results)*100:.1f}%)")
    print(f"亏损次数: {negative_count}/{len(results)} ({negative_count/len(results)*100:.1f}%)")
    
    if np.std(returns) < 50:
        print("\n策略鲁棒性: 优秀 (收益率标准差 < 50%)")
    elif np.std(returns) < 100:
        print("\n策略鲁棒性: 良好 (收益率标准差 < 100%)")
    else:
        print("\n策略鲁棒性: 一般 (收益率标准差 >= 100%)")
    
    print("="*80)
    
    return results


if __name__ == "__main__":
    try:
        results = run_robustness_test()
    except KeyboardInterrupt:
        print("\n\n程序被用户中断")
    except Exception as e:
        print(f"\n\n程序运行出错: {e}")
        import traceback
        traceback.print_exc()
