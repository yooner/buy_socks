import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

STOCK_CODE = "603496"
INITIAL_CAPITAL = 100000

buy_levels = [-0.04, -0.08, -0.13, -0.19, -0.26]
buy_ratios = [0.10, 0.15, 0.20, 0.25, 0.30]

sell_thresholds = [0.05, 0.10, 0.15]
sell_ratios = [0.30, 0.30, 0.40]

def get_weekly_data(stock_code: str = STOCK_CODE) -> pd.DataFrame:
    END_DATE = datetime.now()
    START_DATE = END_DATE - timedelta(days=365 * 4)
    
    daily_data = ak.stock_zh_a_hist(symbol=stock_code, period="daily", 
                                   start_date=START_DATE.strftime("%Y%m%d"), 
                                   end_date=END_DATE.strftime("%Y%m%d"), adjust="qfq")
    
    daily_data['date'] = pd.to_datetime(daily_data['日期'])
    daily_data['week'] = daily_data['date'].dt.isocalendar().week
    daily_data['year'] = daily_data['date'].dt.year
    
    weekly_data = daily_data.groupby(['year', 'week']).agg({
        '日期': 'last',
        '收盘': 'last'
    }).reset_index()
    
    weekly_data = weekly_data.sort_values('日期').reset_index(drop=True)
    weekly_data.rename(columns={'收盘': '收盘价'}, inplace=True)
    
    return weekly_data

def calculate_ma20(prices: List[float]) -> float:
    if len(prices) < 20:
        return sum(prices) / len(prices) if prices else 0
    return sum(prices[-20:]) / 20

def run_backtest():
    weekly_data = get_weekly_data()
    
    if len(weekly_data) < 21:
        print(f"数据不足，需要至少21周数据，当前只有{len(weekly_data)}周")
        return
    
    left_cash = INITIAL_CAPITAL
    shares = 0
    buy_count = 0
    sell_count = 0
    trade_history = []
    
    price_history = []
    
    yearly_summary = {}
    last_year = None
    prev_year_end_asset = INITIAL_CAPITAL
    
    print(f"\n{'='*80}")
    print(f"股票代码: {STOCK_CODE}")
    print(f"起始资金: {INITIAL_CAPITAL:.2f} 元")
    print(f"买入档位: {buy_levels}")
    print(f"买入比例: {buy_ratios}")
    print(f"卖出档位: {sell_thresholds}")
    print(f"卖出比例: {sell_ratios}")
    print(f"{'='*80}\n")
    
    print(f"{'周序号':<6} {'日期':<12} {'收盘价':>10} {'MA20':>10} {'持有股数':>10} {'剩余资金':>12} {'总资产':>12} {'操作':<12}")
    print("-" * 80)
    
    for week_idx in range(len(weekly_data)):
        current_week = week_idx + 1
        row = weekly_data.iloc[week_idx]
        current_price = row['收盘价']
        current_date = row['日期'].strftime('%Y-%m-%d')
        current_year = row['year']
        
        price_history.append(current_price)
        ma20 = calculate_ma20(price_history)
        
        operation = ""
        
        if current_week >= 21:
            if shares == 0:
                if buy_count < len(buy_levels):
                    level = buy_levels[buy_count]
                    expected_price = ma20 * (1 + level)
                    if current_price <= expected_price:
                        buy_ratio = buy_ratios[buy_count]
                        buy_amount = left_cash * buy_ratio
                        new_shares = int(buy_amount / current_price)
                        
                        if new_shares > 0:
                            cost = new_shares * current_price
                            left_cash -= cost
                            shares += new_shares
                            buy_count += 1
                            operation = f"买入{buy_ratio*100:.0f}%"
                            trade_history.append({
                                'week': current_week,
                                'date': current_date,
                                'price': current_price,
                                'shares': new_shares,
                                'cost': cost,
                                'total_cost': cost,
                                'type': 'BUY',
                                'buy_count': buy_count
                            })
            else:
                sold_all = False
                
                if sell_count < len(sell_thresholds):
                    threshold = sell_thresholds[sell_count]
                    if current_price >= ma20 * (1 + threshold):
                        sell_ratio = sell_ratios[sell_count]
                        if sell_count == len(sell_ratios) - 1:
                            sell_shares = shares
                        else:
                            sell_shares = int(shares * sell_ratio)
                        if sell_shares > 0:
                            sell_amount = sell_shares * current_price
                            left_cash += sell_amount
                            shares -= sell_shares
                            sell_count += 1
                            total_asset = left_cash + shares * current_price
                            if shares == 0:
                                operation = f"清仓 总资产{total_asset:.0f}"
                                sold_all = True
                            else:
                                operation = f"减仓{sell_ratio*100:.0f}% 总资产{total_asset:.0f}"
                
                if sold_all:
                    buy_count = 0
                    sell_count = 0
                elif shares > 0 and buy_count < len(buy_levels):
                    level = buy_levels[buy_count]
                    expected_price = ma20 * (1 + level)
                    if current_price <= expected_price:
                        buy_ratio = buy_ratios[buy_count]
                        buy_amount = left_cash * buy_ratio
                        new_shares = int(buy_amount / current_price)
                        
                        if new_shares > 0:
                            cost = new_shares * current_price
                            left_cash -= cost
                            shares += new_shares
                            buy_count += 1
                            operation = f"再买{buy_ratio*100:.0f}%"
                            trade_history.append({
                                'week': current_week,
                                'date': current_date,
                                'price': current_price,
                                'shares': new_shares,
                                'cost': cost,
                                'total_cost': sum(t['cost'] for t in trade_history if t['type'] == 'BUY') + cost,
                                'type': 'BUY',
                                'buy_count': buy_count
                            })
        
        total_asset = left_cash + shares * current_price
        
        print(f"{current_week:<6} {current_date:<12} {current_price:>10.2f} {ma20:>10.2f} {shares:>10} {left_cash:>12.2f} {total_asset:>12.2f} {operation:<12}")
        
        if last_year is None:
            last_year = current_year
            yearly_summary[current_year] = {'start_asset': prev_year_end_asset, 'end_asset': None}
        
        if current_year != last_year:
            yearly_summary[last_year]['end_asset'] = total_asset
            prev_year_end_asset = total_asset
            yearly_summary[current_year] = {'start_asset': prev_year_end_asset, 'end_asset': None}
            last_year = current_year
        
        yearly_summary[current_year]['end_asset'] = total_asset
    
    print(f"\n{'='*80}")
    final_asset = left_cash + shares * weekly_data.iloc[-1]['收盘价']
    print(f"\n{'='*80}")
    print(f"起始资金: {INITIAL_CAPITAL:.2f} 元")
    print(f"最终资产: {final_asset:.2f} 元")
    print(f"收益率: {(final_asset / INITIAL_CAPITAL - 1) * 100:.2f}%")
    print(f"{'='*80}\n")
    
    print(f"{'='*60}")
    print(f"年度收益:")
    print(f"{'='*60}")
    sorted_years = sorted(yearly_summary.keys())
    for year in sorted_years:
        if 'end_asset' in yearly_summary[year]:
            start = yearly_summary[year]['start_asset']
            end = yearly_summary[year]['end_asset']
            diff = end - start
            print(f"{year}年: {start:.2f} → {end:.2f} ({diff:+.2f}元)")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    run_backtest()
