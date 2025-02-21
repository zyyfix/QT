import akshare as ak 
import pandas as pd
import asyncio
import aiohttp
import backtrader as bt
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


async def fetch_etf_history(session, symbol, start_date, end_date):
    """ 异步获取ETF的历史数据 """
    try:
        # 获取历史数据
        df_hist = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
        if df_hist.empty:
            print(f"{symbol} 数据为空")
            return None
        print(f"{symbol} 历史数据行数：", len(df_hist))
        # 等待 0.1 秒后继续
        await asyncio.sleep(1)
        return df_hist
    except Exception as e:
        print(f"{symbol} 数据获取失败:", e)
        return None


async def process_etf(session, symbol):
    """ 处理单个ETF的数据 """
    # 获取过去五天的历史数据
    df_hist = await fetch_etf_history(session, symbol, "20240211", "20240214")
    
    if df_hist is None or df_hist.empty or len(df_hist) < 5:
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
    if (df_hist['MA5'].iloc[-1] > df_hist['MA10'].iloc[-1] and 
    #df_hist['MA5'].iloc[-2] <= df_hist['MA10'].iloc[-2] and
        df_hist['CCI'].iloc[-1] > -100 and
        df_hist['收盘'].iloc[-1] > df_hist['BB_mid'].iloc[-1] and
        df_hist['成交量'].iloc[-1] > df_hist['Volume_MA5'].iloc[-1] * 1.1):
        print(f"{symbol} 满足条件！")
        return symbol
    else:
        print(f"{symbol} 不满足条件")
    return None


async def get_realtime_spot():
    """ 获取实时行情数据并筛选ETF """
    try:
        # 获取全市场实时行情
        df_spot = ak.stock_zh_a_spot()
        print("实时行情数据获取成功，总数：", len(df_spot))
        
        # 筛选ETF股票
        etf_list = df_spot[df_spot['名称'].str.contains('ETF')]['代码'].tolist()
        print("筛选出的ETF列表：", etf_list)

        selected_etfs = []
        total_etfs = len(etf_list)

        # 使用异步并发获取ETF历史数据
        async with aiohttp.ClientSession() as session:
            tasks = [process_etf(session, symbol) for symbol in etf_list]
            for idx, task in enumerate(asyncio.as_completed(tasks)):
                result = await task
                if result:
                    selected_etfs.append(result)
                # 显示进度
                print(f"已处理 {idx + 1}/{total_etfs} 个ETF")

        print("满足条件的ETF股票代码：", selected_etfs)
    except Exception as e:
        print("获取数据失败:", e)


async def main():
    """ 主函数，启动异步任务 """
    await get_realtime_spot()


if __name__ == '__main__':
    asyncio.run(main())
