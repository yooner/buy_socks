"""
股票交易策略 - 卖出策略模块
纯函数，无副作用
"""

from typing import List, Tuple, Optional, Dict, Any


def should_sell(
    current_price: float,
    ma20: float,
    sell_count: int,
    shares: int,
    ma20_history: List[float],
    sell_thresholds: List[float],
    sell_ratios: List[float],
    min_shares: int = 100
) -> Tuple[bool, float, str]:
    """
    判断是否应该卖出
    
    Args:
        current_price: 当前收盘价
        ma20: 20周均线
        sell_count: 已卖出档位数量
        shares: 持有股数
        ma20_history: MA20历史列表
        sell_thresholds: 卖出档位阈值列表
        sell_ratios: 卖出比例列表
        min_shares: 最小保留股数
    
    Returns:
        (是否卖出, 卖出股数, 操作描述)
    """
    if shares < min_shares:
        return False, 0, ""
    
    if sell_count >= len(sell_thresholds):
        return False, 0, ""
    
    if is_ma20_rising(ma20_history, weeks=3):
        return False, 0, ""
    
    threshold = sell_thresholds[sell_count]
    if current_price >= ma20 * (1 + threshold):
        sell_ratio = sell_ratios[sell_count]
        
        if sell_count == len(sell_ratios) - 1:
            sell_shares = shares
        else:
            sell_shares = int(shares * sell_ratio)
        
        if sell_shares >= min_shares:
            return True, sell_shares, ""
    
    return False, 0, ""


def is_ma20_rising(ma20_history: List[float], weeks: int = 3) -> bool:
    """
    判断MA20是否连续上行
    
    Args:
        ma20_history: MA20历史列表
        weeks: 连续上行周数
    
    Returns:
        是否连续上行
    """
    if len(ma20_history) < weeks:
        return False
    
    recent = ma20_history[-weeks:]
    for i in range(len(recent) - 1):
        if recent[i+1] < recent[i]:
            return False
    return True


def is_ma20_falling_or_flat(ma20_history: List[float], weeks: int = 2) -> bool:
    """
    判断MA20是否走平或下行
    
    Args:
        ma20_history: MA20历史列表
        weeks: 连续判断周数
    
    Returns:
        是否走平或下行
    """
    if len(ma20_history) < weeks:
        return True
    
    recent = ma20_history[-weeks:]
    return recent[-1] <= recent[0]


def execute_sell(
    current_price: float,
    sell_shares: int,
    left_cash: float,
    shares: int,
    sell_count: int
) -> Dict[str, Any]:
    """
    执行卖出操作
    
    Args:
        current_price: 当前价格
        sell_shares: 卖出股数
        left_cash: 剩余资金
        shares: 持有股数
        sell_count: 已卖出档位数量
    
    Returns:
        更新后的状态字典
    """
    if sell_shares <= 0:
        return {
            'shares': shares,
            'left_cash': left_cash,
            'sell_count': sell_count,
            'buy_count': 0
        }
    
    sell_amount = sell_shares * current_price
    new_shares = shares - sell_shares
    
    operation = ""
    if new_shares == 0:
        total_asset = left_cash + sell_amount
        operation = f"清仓 总资产{total_asset:.0f}"
    else:
        sell_ratio = sell_shares / (shares + sell_shares)
        total_asset = left_cash + new_shares * current_price
        operation = f"减仓{sell_ratio*100:.0f}% 总资产{total_asset:.0f}"
    
    return {
        'shares': new_shares,
        'left_cash': left_cash + sell_amount,
        'sell_count': sell_count + 1,
        'buy_count': 0 if new_shares == 0 else None
    }


def get_next_sell_info(
    current_price: float,
    ma20: float,
    sell_count: int,
    sell_thresholds: List[float]
) -> Optional[float]:
    """
    获取下一个卖出档位需要的涨幅
    
    Args:
        current_price: 当前价格
        ma20: 20周均线
        sell_count: 已卖出档位数量
        sell_thresholds: 卖出档位阈值列表
    
    Returns:
        需要的涨幅比例，如果没有则返回None
    """
    if sell_count >= len(sell_thresholds):
        return None
    
    return sell_thresholds[sell_count]


def can_sell(
    ma20_history: List[float],
    current_price: float,
    ma20: float,
    sell_count: int,
    sell_thresholds: List[float]
) -> bool:
    if sell_count >= len(sell_thresholds):
        return False
    
    threshold = sell_thresholds[sell_count]
    price_ratio = current_price / ma20
    
    if is_ma20_rising(ma20_history, weeks=3) and current_price > ma20:
        if price_ratio >= 1 + threshold:
            return True
        return False
    
    return price_ratio >= 1 + threshold
