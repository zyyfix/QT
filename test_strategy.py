import backtrader as bt
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
import xgboost as xgb
import akshare as ak

def get_stock_data(code, start_date, end_date):
    """获取股票数据"""
    print(f'获取 {code} 的数据...')
    try:
        # 转换日期格式
        start_date = pd.to_datetime(start_date).strftime('%Y%m%d')
        end_date = pd.to_datetime(end_date).strftime('%Y%m%d')
        
        # 获取股票代码（去掉.SZ/.SH后缀）
        symbol = code.split('.')[0]
        
        # 获取日线数据
        df = ak.stock_zh_a_hist(symbol=symbol, 
                              period='daily',
                              start_date=start_date,
                              end_date=end_date,
                              adjust='qfq')
        
        if df.empty:
            print(f'未获取到数据: {code}')
            return None
            
        # 重命名列
        df = df.rename(columns={
            '日期': 'trade_time',
            '开盘': 'open',
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume'
        })
        
        # 设置索引
        df['trade_time'] = pd.to_datetime(df['trade_time'])
        df = df.set_index('trade_time')
        
        print(f'成功获取数据，数据点数量: {len(df)}')
        return df
        
    except Exception as e:
        print(f'获取数据时出错: {str(e)}')
        return None

if __name__ == '__main__':
    # 测试数据获取
    code = '000001.SZ'
    start_date = '20250218'
    end_date = '20250221'
    
    df = get_stock_data(code, start_date, end_date)
    if df is not None:
        print('\n数据样例：')
        print(df.head())
        print('\n数据统计：')
        print(df.describe())