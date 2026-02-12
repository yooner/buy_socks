import tushare as ts
import pandas as pd
import json
import os
import time
import requests
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Dict, List, Optional, Tuple

STOCK_CODE = "603496"
CACHE_DIR = "cache"
CACHE_EXPIRY_DAYS = 1

CACHE_DAYS = 365 * 10  # 缓存10年数据
BACKTEST_YEARS = 2.5  # 回测默认5年（所有程序统一使用）

END_DATE = pd.to_datetime("20260211")  # 结束日期，None表示当前日期，也可以设置为 "20250101" 格式
START_DATE = END_DATE - timedelta(days=CACHE_DAYS)  # 起始日期（10年前）

TS_TOKEN = "357e7bb25c0bbc3f0d42b2981cbaac63ea797062ef921f469cd89090"
if TS_TOKEN:
    ts.set_token(TS_TOKEN)
    pro = ts.pro_api()
else:
    pro = None


def get_date_range(days: int) -> Tuple[datetime, datetime]:
    if END_DATE is None:
        end_date = datetime.now()
    else:
        end_date = pd.to_datetime(END_DATE)
    start_date = end_date - timedelta(days=days)
    return start_date, end_date


def get_year_range(years: int) -> Tuple[int, int]:
    if END_DATE is None:
        end_year = datetime.now().year
    else:
        end_year = pd.to_datetime(END_DATE).year
    start_year = end_year - years + 1
    return start_year, end_year

def get_daily_cache_path(stock_code: str) -> str:
    return os.path.join(CACHE_DIR, f"{stock_code}_daily.json")

def get_daily_data_from_cache(stock_code: str) -> Optional[pd.DataFrame]:
    cache_path = get_daily_cache_path(stock_code)
    if not os.path.exists(cache_path):
        return None
    
    try:
        with open(cache_path, 'r', encoding='utf-8') as f:
            cache_data = json.load(f)
        
        cache_time = datetime.fromisoformat(cache_data['cache_time'])
        if (datetime.now() - cache_time).days >= CACHE_EXPIRY_DAYS:
            return None
        
        return pd.DataFrame(cache_data['daily_data'])
    except:
        return None

def save_daily_to_cache(stock_code: str, daily_data: pd.DataFrame) -> None:
    cache_path = get_daily_cache_path(stock_code)
    
    daily_data_copy = daily_data.copy()
    
    if 'date' in daily_data_copy.columns:
        daily_data_copy['date'] = daily_data_copy['date'].dt.strftime('%Y-%m-%d')
    
    cache_data = {
        'cache_time': datetime.now().isoformat(),
        'daily_data': daily_data_copy.to_dict(orient='records')
    }
    with open(cache_path, 'w', encoding='utf-8') as f:
        json.dump(cache_data, f, ensure_ascii=False, indent=2)

def aggregate_weekly(daily_data: pd.DataFrame) -> pd.DataFrame:
    if '日期' in daily_data.columns:
        date_col = '日期'
        open_col = '开盘'
        close_col = '收盘'
        high_col = '最高'
        low_col = '最低'
        vol_col = '成交量'
    elif 'date' in daily_data.columns:
        date_col = 'date'
        open_col = '开盘'
        close_col = '收盘'
        high_col = '最高'
        low_col = '最低'
        vol_col = '成交量'
    else:
        date_col = 'trade_date'
        open_col = 'open'
        close_col = 'close'
        high_col = 'high'
        low_col = 'low'
        vol_col = 'vol'
    
    weekly_data = daily_data.groupby(['year', 'week']).agg({
        date_col: 'last',
        open_col: 'first',
        close_col: 'last',
        high_col: 'max',
        low_col: 'min',
        vol_col: 'sum'
    }).reset_index()
    
    weekly_data = weekly_data.sort_values(date_col).reset_index(drop=True)
    weekly_data.rename(columns={
        date_col: 'date',
        open_col: 'open',
        close_col: 'close',
        high_col: 'high',
        low_col: 'low',
        vol_col: 'volume'
    }, inplace=True)
    
    return weekly_data

def ensure_daily_cache(stock_code: str = STOCK_CODE, max_retries: int = 5, retry_delay: int = 3) -> pd.DataFrame:
    cached = get_daily_data_from_cache(stock_code)
    if cached is not None:
        print(f"缓存已有 {stock_code} 日线数据")
        return cached
    
    print(f"从 tushare 获取 {stock_code} 日线数据...")
    if END_DATE is None:
        end = datetime.now()
    else:
        end = pd.to_datetime(END_DATE)
    start = end - timedelta(days=CACHE_DAYS)
    start_date = start.strftime("%Y%m%d")
    end_date = end.strftime("%Y%m%d")
    
    session = requests.Session()
    retries = Retry(total=max_retries, backoff_factor=1, status_forcelist=[500, 502, 503, 504, 429])
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))
    
    for attempt in range(1, max_retries + 1):
        try:
            print(f"尝试第 {attempt}/{max_retries} 次请求...")
            
            ts_code = f"{stock_code}.SH" if stock_code.startswith("6") else f"{stock_code}.SZ"
            
            if pro is None:
                daily_data = ts.pro_api().daily(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date
                )
            else:
                daily_data = pro.daily(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date
                )
            break
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            print(f"连接失败: {e}")
            if attempt < max_retries:
                wait_time = retry_delay * attempt
                print(f"等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
            else:
                print("重试次数耗尽，抛出异常")
                raise e
    
    daily_data['date'] = pd.to_datetime(daily_data['trade_date'])
    
    if 'open' in daily_data.columns:
        daily_data.rename(columns={
            'open': '开盘',
            'close': '收盘',
            'high': '最高',
            'low': '最低',
            'vol': '成交量'
        }, inplace=True)
    
    daily_data['week'] = daily_data['date'].dt.isocalendar().week
    daily_data['year'] = daily_data['date'].dt.year
    
    save_daily_to_cache(stock_code, daily_data)
    
    return daily_data


def get_weekly_data(
    stock_code: str = STOCK_CODE,
    start_date: str = None,
    end_date: str = None,
    days: int = BACKTEST_YEARS * 365
) -> pd.DataFrame:
    """从缓存获取日线，聚合为周线，按日期范围筛选返回"""
    daily_data = ensure_daily_cache(stock_code)
    weekly_data = aggregate_weekly(daily_data)
    
    if start_date is None or end_date is None:
        start, end = get_date_range(days)
        if start_date is None:
            start_date = start
        if end_date is None:
            end_date = end
    
    weekly_data = weekly_data.copy()
    weekly_data['date'] = pd.to_datetime(weekly_data['date'], errors='coerce')
    start_ts = pd.to_datetime(start_date)
    end_ts = pd.to_datetime(end_date)
    
    mask = (weekly_data['date'] >= start_ts) & (weekly_data['date'] <= end_ts)
    filtered = weekly_data[mask]
    
    return filtered.reset_index(drop=True)


def get_daily_data(
    stock_code: str = STOCK_CODE,
    start_date: str = None,
    end_date: str = None,
    days: int = BACKTEST_YEARS * 365
) -> pd.DataFrame:
    """从缓存获取日线数据"""
    daily_data = ensure_daily_cache(stock_code)
    
    if start_date is None or end_date is None:
        start, end = get_date_range(days)
        if start_date is None:
            start_date = start
        if end_date is None:
            end_date = end
    else:
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
    
    filtered = daily_data[(daily_data['date'] >= start_date) & (daily_data['date'] <= end_date)]
    return filtered.reset_index(drop=True)


def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df['ma20'] = df['close'].rolling(20).mean()
    return df


STOCK_CODE_EXPORT = STOCK_CODE
BACKTEST_YEARS_EXPORT = BACKTEST_YEARS
END_DATE_EXPORT = END_DATE
ensure_daily_cache = ensure_daily_cache
aggregate_weekly = aggregate_weekly
get_year_range = get_year_range
get_weekly_data = get_weekly_data
get_daily_data = get_daily_data

def calculate_weekly_avg(weekly_data: pd.DataFrame, start_week: int = 1, end_week: int = 20) -> Dict[str, float]:
    selected_weeks = weekly_data[(weekly_data['周序号'] >= start_week) & (weekly_data['周序号'] <= end_week)]
    
    if selected_weeks.empty:
        return {}
    
    avg_data = {
        'avg_close': selected_weeks['收盘价'].mean(),
        'start_week': start_week,
        'end_week': end_week,
        'count': len(selected_weeks)
    }
    
    return avg_data

def print_weekly_report(weekly_data: pd.DataFrame, avg_data_list: List[Tuple[int, int, Dict[str, float]]]):
    print(f"\n{'='*60}")
    print(f"股票代码: {STOCK_CODE}")
    print(f"分析周期: {START_DATE.strftime('%Y-%m-%d')} 至 {END_DATE.strftime('%Y-%m-%d')}")
    print(f"总周数: {len(weekly_data)}")
    print(f"{'='*60}\n")
    
    print(f"{'周序号':<8} {'日期':<12} {'收盘价':>10}")
    print("-" * 35)
    
    for _, row in weekly_data.iterrows():
        date_str = row['日期'].strftime('%Y-%m-%d')
        print(f"{row['周序号']:<8} {date_str:<12} {row['收盘价']:>10.2f}")
    
    print("\n" + "=" * 60)
    print("周平均价格:")
    print(f"{'='*60}")
    
    for start_week, end_week, avg in avg_data_list:
        print(f"\n第{start_week}周 ~ 第{end_week}周 (共{avg['count']}周):")
        print(f"  平均收盘价: {avg['avg_close']:.2f}")

def main():
    stock_code = STOCK_CODE
    
    print(f"正在获取股票 {stock_code} 的历史数据...")
    daily_data = ensure_daily_cache(stock_code)
    weekly_data = aggregate_weekly(daily_data)
    
    print(f"数据处理完成，共 {len(weekly_data)} 周数据\n")
    
    avg_ranges = [
        (1, 20),
        (2, 21),
        (3, 22),
        (4, 23),
        (5, 24),
    ]
    
    avg_data_list = []
    for start_week, end_week in avg_ranges:
        if end_week <= len(weekly_data):
            avg = calculate_weekly_avg(weekly_data, start_week, end_week)
            avg_data_list.append((start_week, end_week, avg))
    
    print_weekly_report(weekly_data, avg_data_list)
    
    return weekly_data, avg_data_list

if __name__ == "__main__":
    main()
