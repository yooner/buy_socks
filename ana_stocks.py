import akshare as ak
import pandas as pd
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

STOCK_CODE = "603496"
CACHE_DIR = "cache"
CACHE_EXPIRY_DAYS = 1

DEFAULT_DAYS = 365 * 10


def get_date_range(days: int = DEFAULT_DAYS) -> Tuple[datetime, datetime]:
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    return start_date, end_date

def get_cache_path(stock_code: str) -> str:
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, f"{stock_code}.json")

def get_cached_data(stock_code: str) -> Optional[Dict]:
    cache_path = get_cache_path(stock_code)
    if not os.path.exists(cache_path):
        return None
    
    try:
        with open(cache_path, 'r', encoding='utf-8') as f:
            cache_data = json.load(f)
        
        cache_time = datetime.fromisoformat(cache_data['cache_time'])
        if (datetime.now() - cache_time).days >= CACHE_EXPIRY_DAYS:
            return None
        
        return cache_data
    except:
        return None

def save_to_cache(stock_code: str, data: Dict) -> None:
    cache_path = get_cache_path(stock_code)
    data['cache_time'] = datetime.now().isoformat()
    with open(cache_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def get_stock_weekly_data(stock_code: str = STOCK_CODE, 
                          start_date: str = None, 
                          end_date: str = None,
                          days: int = DEFAULT_DAYS,
                          use_cache: bool = True) -> pd.DataFrame:
    if start_date is None or end_date is None:
        start, end = get_date_range(days)
        if start_date is None:
            start_date = start.strftime("%Y%m%d")
        if end_date is None:
            end_date = end.strftime("%Y%m%d")
    if use_cache:
        cached = get_cached_data(stock_code)
        if cached and 'weekly_data' in cached:
            print(f"从缓存读取股票 {stock_code} 数据")
            weekly_df = pd.DataFrame(cached['weekly_data'])
            return weekly_df

    print(f"从 akshare 获取股票 {stock_code} 数据...")
    daily_data = ak.stock_zh_a_hist(symbol=stock_code, period="daily", 
                                   start_date=start_date, end_date=end_date, adjust="qfq")
    
    daily_data['date'] = pd.to_datetime(daily_data['日期'])
    daily_data['week'] = daily_data['date'].dt.isocalendar().week
    daily_data['year'] = daily_data['date'].dt.year
    
    weekly_data = daily_data.groupby(['year', 'week']).agg({
        '日期': 'last',
        '开盘': 'first',
        '收盘': 'last',
        '最高': 'max',
        '最低': 'min',
        '成交量': 'sum'
    }).reset_index()
    
    weekly_data = weekly_data.sort_values('日期').reset_index(drop=True)
    weekly_data.rename(columns={
        '日期': 'date',
        '开盘': 'open',
        '收盘': 'close',
        '最高': 'high',
        '最低': 'low',
        '成交量': 'volume'
    }, inplace=True)
    
    if use_cache:
        cache_data = {'weekly_data': weekly_data.to_dict(orient='records')}
        save_to_cache(stock_code, cache_data)
    
    return weekly_data


def get_weekly_data(stock_code: str = STOCK_CODE) -> pd.DataFrame:
    return get_stock_weekly_data(stock_code)


def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df['ma20'] = df['close'].rolling(20).mean()
    return df


STOCK_CODE_EXPORT = STOCK_CODE
DEFAULT_DAYS_EXPORT = DEFAULT_DAYS
get_date_range = get_date_range

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
    weekly_data = get_stock_weekly_data(stock_code)
    
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
