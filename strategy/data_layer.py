"""
股票交易策略 - 数据获取模块
"""

import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any


def get_stock_daily_data(
    stock_code: str,
    days: int = 900
) -> pd.DataFrame:
    """
    获取股票日线数据
    
    Args:
        stock_code: 股票代码
        days: 获取天数
    
    Returns:
        日线数据DataFrame
    """
    END_DATE = datetime.now()
    START_DATE = END_DATE - timedelta(days=days)
    
    df = ak.stock_zh_a_hist(
        symbol=stock_code,
        period="daily",
        start_date=START_DATE.strftime("%Y%m%d"),
        end_date=END_DATE.strftime("%Y%m%d"),
        adjust="qfq"
    )
    
    df['date'] = pd.to_datetime(df['日期'])
    df = df.sort_values('date').reset_index(drop=True)
    
    return df


def get_stock_weekly_data(
    stock_code: str,
    weeks: int = 52
) -> pd.DataFrame:
    """
    获取股票周线数据
    
    Args:
        stock_code: 股票代码
        weeks: 获取周数
    
    Returns:
        周线数据DataFrame，包含日期、收盘价
    """
    daily_data = get_stock_daily_data(stock_code, days=weeks * 7 * 2 + 140)
    
    daily_data['week'] = daily_data['date'].dt.isocalendar().week
    daily_data['year'] = daily_data['date'].dt.year
    
    weekly_data = daily_data.groupby(['year', 'week']).agg({
        '日期': 'last',
        '收盘': 'last',
        '开盘': 'first',
        '最高': 'max',
        '最低': 'min',
        '成交量': 'sum'
    }).reset_index()
    
    weekly_data = weekly_data.sort_values('日期').reset_index(drop=True)
    weekly_data.rename(columns={
        '日期': 'date',
        '收盘': 'close',
        '开盘': 'open',
        '最高': 'high',
        '最低': 'low',
        '成交量': 'volume'
    }, inplace=True)
    
    return weekly_data


def calculate_ma20(prices: List[float]) -> float:
    """
    计算20日/周均线
    
    Args:
        prices: 价格列表
    
    Returns:
        MA20值
    """
    if len(prices) < 20:
        return sum(prices) / len(prices) if prices else 0
    return sum(prices[-20:]) / 20


def prepare_price_history(weekly_data: pd.DataFrame) -> List[float]:
    """
    准备价格历史列表
    
    Args:
        weekly_data: 周线数据
    
    Returns:
        价格历史列表
    """
    return weekly_data['close'].tolist()


def get_date_range(weekly_data: pd.DataFrame) -> Dict[str, Any]:
    """
    获取数据的时间范围
    
    Args:
        weekly_data: 周线数据
    
    Returns:
        时间范围信息
    """
    if len(weekly_data) == 0:
        return {
            'start_date': None,
            'end_date': None,
            'total_weeks': 0,
            'years': []
        }
    
    start_date = weekly_data.iloc[0]['date']
    end_date = weekly_data.iloc[-1]['date']
    years = weekly_data['year'].unique().tolist()
    
    return {
        'start_date': start_date,
        'end_date': end_date,
        'total_weeks': len(weekly_data),
        'years': sorted(years)
    }
