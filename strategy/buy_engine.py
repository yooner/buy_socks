"""
股票交易策略 - 买入策略模块
纯函数，无副作用
"""

from typing import List, Tuple, Optional, Dict, Any


def should_buy(
    current_price: float,
    ma20: float,
    buy_count: int,
    buy_levels: List[float],
    buy_ratios: List[float],
    left_cash: float
) -> Tuple[bool, int, float, str]:
    """
    判断是否应该买入
    
    Args:
        current_price: 当前收盘价
        ma20: 20周均线
        buy_count: 已买入档位数量
        buy_levels: 买入档位阈值列表
        buy_ratios: 买入比例列表
        left_cash: 剩余资金
    
    Returns:
        (是否买入, 买入档位索引, 买入金额, 操作描述)
    """
    if left_cash < 100:
        return False, -1, 0, ""
    
    price_drop_rate = (ma20 - current_price) / ma20
    
    for i in range(buy_count, len(buy_levels)):
        if price_drop_rate >= abs(buy_levels[i]):
            buy_ratio = buy_ratios[i]
            buy_amount = left_cash * buy_ratio
            new_shares = int(buy_amount / current_price)
            
            if new_shares > 0:
                if buy_count == 0:
                    operation = f"买入{buy_ratio*100:.0f}%"
                else:
                    operation = f"再买{buy_ratio*100:.0f}%"
                return True, i, buy_amount, operation
    
    return False, -1, 0, ""


def execute_buy(
    current_price: float,
    buy_amount: float,
    left_cash: float,
    shares: int,
    buy_level_idx: int,
    buy_count: int
) -> Dict[str, Any]:
    """
    执行买入操作
    
    Args:
        current_price: 当前价格
        buy_amount: 买入金额
        left_cash: 剩余资金
        shares: 持有股数
        buy_level_idx: 买入档位索引
        buy_count: 已买入档位数量
    
    Returns:
        更新后的状态字典
    """
    new_shares = int(buy_amount / current_price)
    
    if new_shares <= 0:
        return {
            'shares': shares,
            'left_cash': left_cash,
            'buy_count': buy_count,
            'sell_count': 0
        }
    
    cost = new_shares * current_price
    new_shares_total = shares + new_shares
    
    return {
        'shares': new_shares_total,
        'left_cash': left_cash - cost,
        'buy_count': buy_level_idx + 1,
        'sell_count': 0
    }


def get_next_buy_info(
    current_price: float,
    ma20: float,
    buy_count: int,
    buy_levels: List[float]
) -> Optional[int]:
    """
    获取下一个应该买入的档位
    
    Args:
        current_price: 当前价格
        ma20: 20周均线
        buy_count: 已买入档位数量
        buy_levels: 买入档位阈值列表
    
    Returns:
        应该买入的档位索引，如果没有则返回None
    """
    if buy_count >= len(buy_levels):
        return None
    
    price_drop_rate = (ma20 - current_price) / ma20
    
    for i in range(buy_count, len(buy_levels)):
        if price_drop_rate >= abs(buy_levels[i]):
            return i
    
    return None


def calculate_price_drop_rate(current_price: float, ma20: float) -> float:
    """
    计算价格相对MA20的跌幅
    
    Args:
        current_price: 当前价格
        ma20: 20周均线
    
    Returns:
        跌幅比例 (正数表示下跌)
    """
    return (ma20 - current_price) / ma20
