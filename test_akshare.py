import akshare as ak
import pandas as pd
import backtrader as bt
import asyncio
import aiohttp
from datetime import datetime


# 自定义AKShare数据加载类
class AKShareData(bt.feeds.PandasData):
    params = (
        ('datetime', None),  # 使用默认索引
        ('open', 'open'),
        ('high', 'high'),
        ('low', 'low'),
        ('close', 'close'),
        ('volume', 'volume'),
        ('openinterest', -1),  # 无持仓量字段
    )

import concurrent.futures

def get_realtime_spot():
    try:
        # 获取全市场实时行情
        df_spot = ak.stock_zh_a_spot()
        print("实时行情数据获取成功，总数：", len(df_spot))
        # 筛选ETF股票
        
        etf_list = df_spot[df_spot['名称'].str.contains('ETF')]['代码'].tolist()
        print("筛选出的ETF列表：", etf_list)

        selected_etfs = []
        total_etfs = len(etf_list)
        
        def process_etf(symbol):
            # 获取过去五天的历史数据
            df_hist = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date="20250211", end_date="20250214", adjust="qfq")
            print(f"{symbol} 历史数据行数：", len(df_hist))
            
            if df_hist.empty or len(df_hist) < 5:
                print(f"{symbol} 数据为空或长度不足")
                return None
            
            # 计算技术指标
            df_hist['MA5'] = df_hist['收盘'].rolling(window=5).mean()
            df_hist['MA10'] = df_hist['收盘'].rolling(window=10).mean()
            df_hist['CCI'] = (df_hist['收盘'] - df_hist['收盘'].rolling(window=14).mean()) / (0.015 * df_hist['收盘'].rolling(window=14).std())
            df_hist['BB_mid'] = df_hist['收盘'].rolling(window=20).mean()
            df_hist['Volume_MA5'] = df_hist['成交量'].rolling(window=5).mean()

             # 打印技术指标和最后几行数据，帮助调试
            print(f"{symbol} 最新技术指标：")
            print(df_hist[['收盘', 'MA5', 'MA10', 'CCI', 'BB_mid', 'Volume_MA5']].tail(6))
            
            # 筛选满足条件的ETF
            if (df_hist['MA5'].iloc[-1] > df_hist['MA10'].iloc[-1] and df_hist['MA5'].iloc[-2] <= df_hist['MA10'].iloc[-2] and
                df_hist['CCI'].iloc[-1] > -100 and
                df_hist['收盘'].iloc[-1] > df_hist['BB_mid'].iloc[-1] and
                df_hist['成交量'].iloc[-1] > df_hist['Volume_MA5'].iloc[-1] * 1.1):
                print(f"{symbol} 满足条件！")
                return symbol
            else:
                print(f"{symbol} 不满足条件")
            return None
        
        # 使用多线程处理ETF
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(process_etf, symbol) for symbol in etf_list]
            for index, future in enumerate(concurrent.futures.as_completed(futures)):
                result = future.result()
                if result:
                    selected_etfs.append(result)
                # 显示进度
                print(f"已处理 {index + 1}/{total_etfs} 个ETF")
        
        print("满足条件的ETF股票代码：", selected_etfs)
    except Exception as e:
        print("获取数据失败:", e)
    # try:
    #     # 获取全市场实时行情（包含最新价、涨跌幅、成交量等字段）
    #     df = ak.stock_zh_a_spot()
    #     # 筛选并打印部分列（示例：代码、名称、最新价、涨跌幅）
    #     selected_columns = ["代码", "名称", "最新价", "涨跌幅", "成交量"]
    #     print("全市场实时行情示例（前5行）：\n", df[selected_columns].head())
    #     # 按需保存到CSV
    #     # df.to_csv("realtime_spot.csv", index=False)
    # except Exception as e:
    #     print("获取数据失败:", e)


def get_intraday_minutes(symbol="sh600000"):
    try:
        # 获取当日分时数据（1分钟频度）
        df = ak.stock_zh_a_minute(symbol=symbol, period='1', adjust="")
        # 格式化时间戳
        df['day'] = pd.to_numeric(df['day'], errors='coerce')  # 确保 'day' 列为数值类型
        df['time'] = df['day'].apply(lambda x: datetime.fromtimestamp(x / 1000).strftime('%H:%M') if pd.notna(x) else '')
        
        # 打印关键列
        print(f"股票 {symbol} 当日分时数据：\n", df[['time', 'open', 'high', 'low', 'close', 'volume']])
    except Exception as e:
        print("获取分时数据失败:", e)

if __name__ == '__main__':
    get_realtime_spot()
    #get_intraday_minutes(symbol="sh600000")  # 示例代码为浦发