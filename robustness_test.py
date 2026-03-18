"""
策略鲁棒性测试脚本

测试逻辑：
1. 遍历 cache 下的每只股票作为初始买入候选
2. 对每只股票作为起始点进行独立回测
3. 调用 buy_socks_sell_bodong.py 的 run_backtest 函数进行回测
4. 比较不同起始点的回测结果，验证策略鲁棒性

输出：多种回测数据的对比分析
"""

import pandas as pd
import numpy as np
import os
import json
import random
import sys
from glob import glob
from datetime import datetime
from collections import defaultdict
from io import StringIO

# 导入 buy_socks_sell_bodong 的 run_backtest 函数
from buy_socks_sell_bodong import run_backtest

# 配置参数
CACHE_DIR = "./cache"
INITIAL_CAPITAL = 100000  # 初始资金

# 配置：是否使用随机股票组合测试
# 如果为 True，每次测试会从所有股票中随机选择 RANDOM_STOCK_COUNT 个股票
# 如果为 False，使用所有股票进行测试
ENABLE_RANDOM_STOCK_SELECTION = False

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
SPECIFIC_STOCKS = None
# SPECIFIC_STOCKS = ['302132', '603496', '000543']

# 配置：随机种子
# 设置为 None 则每次运行结果不同
# 设置为具体数字则每次运行结果一致（用于复现结果）
RANDOM_SEED = None  # 例如: 42, 123, 456 或 None(随机)


def get_all_stock_codes(cache_dir=CACHE_DIR):
    """从缓存目录获取所有股票代码"""
    cache_files = glob(os.path.join(cache_dir, "*_daily.json"))
    stock_codes = []
    for f in cache_files:
        basename = os.path.basename(f)
        code = basename.replace('_daily.json', '')
        stock_codes.append(code)
    return sorted(stock_codes)


def select_random_stocks(all_stock_codes, num_stocks=10, seed=None):
    """
    随机选择股票组合
    
    Args:
        all_stock_codes: 所有可用的股票代码列表
        num_stocks: 需要随机选择的股票数量（默认10个）
        seed: 随机种子
    
    Returns:
        随机选择的股票列表
    """
    if seed is not None:
        random.seed(seed)
    
    # 如果可用股票不足，返回所有可用股票
    if len(all_stock_codes) <= num_stocks:
        selected_stocks = all_stock_codes
    else:
        # 随机选择 num_stocks 个股票
        selected_stocks = random.sample(all_stock_codes, num_stocks)
    
    return selected_stocks


def run_single_stock_backtest(stock_code):
    """
    对单只股票运行回测
    调用 buy_socks_sell_bodong.py 的 run_backtest 函数
    
    Args:
        stock_code: 股票代码
    
    Returns:
        dict: 回测结果
    """
    print(f"\n{'='*80}")
    print(f"正在回测股票: {stock_code}")
    print(f"{'='*80}")
    
    # 捕获标准输出，避免打印过多信息
    old_stdout = sys.stdout
    sys.stdout = StringIO()
    
    try:
        # 调用 buy_socks_sell_bodong 的 run_backtest
        result = run_backtest(stock_code)
        
        # 恢复标准输出
        sys.stdout = old_stdout
        
        if result is None:
            print(f"股票 {stock_code} 回测失败: 数据不足")
            return None
        
        total_return, yearly_returns, trade_info = result
        
        # 构建结果字典
        result_dict = {
            'stock_code': stock_code,
            'total_return': total_return,
            'yearly_returns': yearly_returns,
            'trades': trade_info.get('trades', []),
            'final_value': trade_info.get('final_value', INITIAL_CAPITAL),
            'trade_count': len([t for t in trade_info.get('trades', []) if t.get('action') == '卖出'])
        }
        
        print(f"回测完成: 收益率 {total_return:.2f}%, 交易次数 {result_dict['trade_count']}")
        
        return result_dict
        
    except Exception as e:
        # 恢复标准输出
        sys.stdout = old_stdout
        print(f"股票 {stock_code} 回测出错: {e}")
        import traceback
        traceback.print_exc()
        return None


def run_robustness_test():
    """
    运行鲁棒性测试
    串行地对每只股票运行回测
    """
    # 设置随机种子
    if RANDOM_SEED is not None:
        random.seed(RANDOM_SEED)
        print(f"使用随机种子: {RANDOM_SEED}")
    
    # 获取所有股票代码
    all_stock_codes = get_all_stock_codes()
    print(f"发现 {len(all_stock_codes)} 只股票")
    
    # 确定要测试的股票列表
    if SPECIFIC_STOCKS is not None:
        # 使用指定的股票组合
        test_stocks = [code for code in SPECIFIC_STOCKS if code in all_stock_codes]
        print(f"使用指定股票组合: {test_stocks}")
    elif ENABLE_RANDOM_STOCK_SELECTION:
        # 随机选择股票
        test_stocks = select_random_stocks(all_stock_codes, RANDOM_STOCK_COUNT, RANDOM_SEED)
        print(f"随机选择 {len(test_stocks)} 只股票: {test_stocks}")
    else:
        # 使用所有股票
        test_stocks = all_stock_codes
        print(f"使用所有 {len(test_stocks)} 只股票进行测试")
    
    # 确定测试轮数
    num_rounds = TEST_ROUNDS if TEST_ROUNDS is not None else len(test_stocks)
    num_rounds = min(num_rounds, len(test_stocks))
    
    print(f"\n开始鲁棒性测试，共 {num_rounds} 轮...")
    print(f"{'='*80}")
    
    # 串行运行回测
    results = []
    for i, stock_code in enumerate(test_stocks[:num_rounds]):
        print(f"\n[{i+1}/{num_rounds}] ", end="")
        result = run_single_stock_backtest(stock_code)
        if result is not None:
            results.append(result)
    
    # 输出统计结果
    print_results_summary(results)
    
    return results


def print_results_summary(results):
    """打印测试结果摘要"""
    if not results:
        print("\n没有有效的回测结果")
        return
    
    print("\n" + "="*80)
    print("鲁棒性测试结果摘要")
    print("="*80)
    
    # 按收益率排序
    results_sorted = sorted(results, key=lambda x: x['total_return'], reverse=True)
    
    # 基本统计
    returns = [r['total_return'] for r in results]
    final_values = [r['final_value'] for r in results]
    trade_counts = [r['trade_count'] for r in results]
    
    print(f"\n测试股票数量: {len(results)}")
    print(f"\n收益率统计:")
    print(f"  最高: {max(returns):.2f}%")
    print(f"  最低: {min(returns):.2f}%")
    print(f"  平均: {np.mean(returns):.2f}%")
    print(f"  中位数: {np.median(returns):.2f}%")
    print(f"  标准差: {np.std(returns):.2f}%")
    
    print(f"\n最终市值统计:")
    print(f"  最高: {max(final_values):,.2f}")
    print(f"  最低: {min(final_values):,.2f}")
    print(f"  平均: {np.mean(final_values):,.2f}")
    print(f"  中位数: {np.median(final_values):,.2f}")
    
    print(f"\n交易次数统计:")
    print(f"  最多: {max(trade_counts)}")
    print(f"  最少: {min(trade_counts)}")
    print(f"  平均: {np.mean(trade_counts):.1f}")
    print(f"  中位数: {np.median(trade_counts):.1f}")
    
    # 收益率分布
    print("\n" + "="*80)
    print("收益率分布")
    print("="*80)
    
    # 定义区间
    bins = [-float('inf'), -50, -20, -10, 0, 10, 20, 50, float('inf')]
    labels = ['<-50%', '-50~-20%', '-20~-10%', '-10~0%', '0~10%', '10~20%', '20~50%', '>50%']
    
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
        for year in r['yearly_returns']:
            all_years.add(year)
    all_years = sorted(list(all_years))
    
    if all_years:
        print(f"\n{'股票':<10}", end='')
        for year in all_years:
            print(f"{year:<10}", end='')
        print(f"{'总计':<10}")
        print("-" * (10 + 10 * len(all_years) + 10))
        
        # 每个股票的年度收益
        for r in results_sorted[:10]:  # 只显示前10名
            print(f"{r['stock_code']:<10}", end='')
            for year in all_years:
                if year in r['yearly_returns']:
                    ret = r['yearly_returns'][year]
                    print(f"{ret:>8.1f}%", end='  ')
                else:
                    print(f"{'N/A':>8}", end='  ')
            print(f"{r['total_return']:>8.1f}%")
        
        # 年度统计
        print("\n" + "-" * (10 + 10 * len(all_years) + 10))
        print(f"{'平均':<10}", end='')
        for year in all_years:
            year_returns_list = []
            for r in results:
                if year in r['yearly_returns']:
                    year_returns_list.append(r['yearly_returns'][year])
            if year_returns_list:
                avg_ret = np.mean(year_returns_list)
                print(f"{avg_ret:>8.1f}%", end='  ')
            else:
                print(f"{'N/A':>8}", end='  ')
        print(f"{np.mean(returns):>8.1f}%")
        
        print(f"{'最高':<10}", end='')
        for year in all_years:
            year_returns_list = []
            for r in results:
                if year in r['yearly_returns']:
                    year_returns_list.append(r['yearly_returns'][year])
            if year_returns_list:
                max_ret = max(year_returns_list)
                print(f"{max_ret:>8.1f}%", end='  ')
            else:
                print(f"{'N/A':>8}", end='  ')
        print(f"{max(returns):>8.1f}%")
        
        print(f"{'最低':<10}", end='')
        for year in all_years:
            year_returns_list = []
            for r in results:
                if year in r['yearly_returns']:
                    year_returns_list.append(r['yearly_returns'][year])
            if year_returns_list:
                min_ret = min(year_returns_list)
                print(f"{min_ret:>8.1f}%", end='  ')
            else:
                print(f"{'N/A':>8}", end='  ')
        print(f"{min(returns):>8.1f}%")
    
    # 输出详细交易记录
    print("\n" + "="*80)
    print("详细交易记录 (前5名)")
    print("="*80)
    
    for r in results_sorted[:5]:
        print(f"\n{'='*80}")
        print(f"股票: {r['stock_code']} | 总收益率: {r['total_return']:.2f}% | 交易次数: {r['trade_count']}")
        print(f"{'='*80}")
        print(f"{'日期':<12} {'操作':<8} {'价格':>10} {'股数':>8} {'盈亏':>12}")
        print("-" * 60)
        
        for t in r['trades']:
            profit_str = f"{t.get('profit', 0):,.2f}" if t.get('action') == '卖出' else '-'
            print(f"{t.get('date', ''):<12} {t.get('action', ''):<8} {t.get('price', 0):>10.2f} {t.get('shares', 0):>8} {profit_str:>12}")
        
        print(f"{'-'*60}")
        print(f"最终市值: {r['final_value']:,.2f}")
    
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
