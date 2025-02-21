import backtrader as bt
import talib as ta
import akshare as ak
import pandas as pd
from datetime import datetime

def get_stock_data(code, start_date, end_date):
    """获取股票分钟级数据"""
    # 转换股票代码格式（去掉.SZ/.SH后缀）
    symbol = code.split('.')[0]
    
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # 获取分钟级数据
            df = ak.stock_zh_a_hist_min_em(
                symbol=symbol, 
                period='1', 
                adjust='qfq',
                timeout=30  # Increase timeout
            )
            
            if df.empty:
                raise ValueError(f"No data retrieved for {code}")
                
            # 重命名列以匹配backtrader需要的格式
            df = df.rename(columns={
                '时间': 'datetime',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume'
            })
            
            # 将时间列转换为datetime格式并设为索引
            df['datetime'] = pd.to_datetime(df['datetime'])
            df.set_index('datetime', inplace=True)
            
            # 筛选时间范围
            df = df[(df.index >= start_date) & (df.index <= end_date)]
            
            if df.empty:
                print(f"No data available for the specified date range")
                return None
                
            return df
            
        except Exception as e:
            print(f"Attempt {retry_count + 1} failed: {str(e)}")
            retry_count += 1
            if retry_count < max_retries:
                print(f"Retrying... ({retry_count}/{max_retries})")
                import time
                time.sleep(5)  # Add delay between retries
    
    print(f"Failed to retrieve data after {max_retries} attempts")
    return None

# MultiIndicatorStrategy类保持不变
class MultiIndicatorStrategy(bt.Strategy):
    params = (
        ('ma_period1', 5),
        ('ma_period2', 10),
        ('cci_period', 14),
        ('bb_period', 20),
        ('bb_dev', 2),
        ('volume_ratio', 1.5),
        ('stop_loss', 0.05),
    )

    def __init__(self):
        # 计算指标
        self.ma5 = bt.indicators.SMA(self.data.close, period=self.p.ma_period1)
        self.ma10 = bt.indicators.SMA(self.data.close, period=self.p.ma_period2)
        self.cci = bt.indicators.CCI(self.data, period=self.p.cci_period)
        
        # 布林带
        self.bb = bt.indicators.BollingerBands(self.data.close, 
                                             period=self.p.bb_period,
                                             devfactor=self.p.bb_dev)
        # 成交量均线
        self.vol_ma5 = bt.indicators.SMA(self.data.volume, period=5)
        
        # 跟踪订单和持仓状态
        self.order = None
        self.stop_price = None

    def next(self):
        if self.order:  # 有未完成订单则跳过
            return
        
        # 条件1：均线金叉
        ma_cross = (self.ma5[0] > self.ma10[0]) and (self.ma5[-1] <= self.ma10[-1])
        # 条件2：CCI从<-100回升至>-100
        cci_signal = (self.cci[0] > -100) and (self.cci[-1] <= -100)
        # 条件3：价格突破布林带中轨
        price_above_bbmid = self.data.close[0] > self.bb.mid[0]
        # 条件4：成交量放量
        volume_spike = self.data.volume[0] > self.vol_ma5[0] * self.p.volume_ratio
        
        # 入场条件（多头）
        if ma_cross and cci_signal and price_above_bbmid and volume_spike:
            # 计算头寸（假设使用总资金的90%）
            size = self.broker.getcash() * 0.9 / self.data.close[0]
            self.order = self.buy(size=size)
            # 设置止损（5%止损）
            self.stop_price = self.data.close[0] * (1 - self.p.stop_loss)
        
        # 离场条件
        elif self.position:
            # 条件1：均线死叉
            ma_death = (self.ma5[0] < self.ma10[0]) and (self.ma5[-1] >= self.ma10[-1])
            # 条件2：CCI从>100回落
            cci_exit = (self.cci[0] < 100) and (self.cci[-1] >= 100)
            # 条件3：价格跌破布林带下轨
            price_below_bblower = self.data.close[0] < self.bb.bot[0]
            # 止损触发
            stop_trigger = self.data.close[0] <= self.stop_price
            
            if ma_death or cci_exit or price_below_bblower or stop_trigger:
                self.order = self.sell(size=self.position.size)
                self.stop_price = None  # 重置止损

    def notify_order(self, order):
        if order.status in [order.Completed]:
            self.order = None

if __name__ == '__main__':
    # 创建回测引擎
    cerebro = bt.Cerebro()
    
    # 获取数据 - 使用更短的时间范围
    stock_code = '000001.SZ'  # 平安银行
    start_date = '2023-08-28'  # Use more recent dates
    end_date = '2023-08-31'
    
    print(f"Fetching data for {stock_code} from {start_date} to {end_date}...")
    df = get_stock_data(stock_code, start_date, end_date)
    
    if df is not None and not df.empty:
        # 将数据加载到backtrader
        data = bt.feeds.PandasData(dataname=df)
        cerebro.adddata(data)
        
        # 添加策略
        cerebro.addstrategy(MultiIndicatorStrategy)
        
        # 设置初始资金
        cerebro.broker.setcash(1000000.0)
        # 设置交易手续费
        cerebro.broker.setcommission(commission=0.0003)
        
        # 打印初始资金
        print(f'初始资金: {cerebro.broker.getvalue():.2f}')
        
        # 运行回测
        cerebro.run()
        
        # 打印最终资金
        print(f'最终资金: {cerebro.broker.getvalue():.2f}')
        
        # 绘制结果
        cerebro.plot()
    else:
        print("未能获取有效数据，请检查股票代码和日期范围")