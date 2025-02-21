import backtrader as bt
import akshare as ak
import pandas as pd
from datetime import datetime
import time

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

def fetch_data(symbol="600000", start_date="20200101", end_date="20231231"):
    """使用AKShare获取股票数据"""
    try:
        df = ak.stock_zh_a_hist(
            symbol=symbol, 
            period="daily", 
            start_date=start_date, 
            end_date=end_date, 
            adjust="hfq"
        )
        
        # 格式转换
        df['日期'] = pd.to_datetime(df['日期'])
        df.rename(columns={ 
            '日期': 'datetime',
            '开盘': 'open',
            '最高': 'high',
            '最低': 'low',
            '收盘': 'close',
            '成交量': 'volume'
        }, inplace=True)
        df.set_index('datetime', inplace=True)
        df.sort_index(ascending=True, inplace=True)
        return df
        
    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        return pd.DataFrame()

# 原策略类（无需修改）
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

def live_trading():
    """实时交易函数"""
    cerebro = bt.Cerebro(live=True)  # 启用实时模式
    
    # 初始化历史数据
    hist_data = fetch_data(symbol="600000", start_date="20230101")
    data = AKShareData(dataname=hist_data)
    cerebro.adddata(data)
    
    # 添加策略
    cerebro.addstrategy(MultiIndicatorStrategy)
    
    # 设置初始资金和手续费
    cerebro.broker.setcash(100000.0)
    cerebro.broker.setcommission(commission=0.0003)
    
    print('Starting live trading...')
    print('Initial Portfolio Value: %.2f' % cerebro.broker.getvalue())
    
    # 定时任务：每5分钟更新数据
    try:
        while True:
            current_date = datetime.now().strftime("%Y%m%d")
            print(f"Updating data for {current_date}")
            
            new_data = fetch_data(symbol="600000", start_date=current_date)
            if not new_data.empty:
                new_data_feed = AKShareData(dataname=new_data)
                cerebro.adddata(new_data_feed)  # 动态添加新数据
                cerebro.run(runonce=False)  # 运行策略
                
            print(f"Current Portfolio Value: %.2f" % cerebro.broker.getvalue())
            time.sleep(300)  # 5分钟间隔
            
    except KeyboardInterrupt:
        print("\nStopping live trading...")
        print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())

if __name__ == '__main__':
    # 选择运行模式：回测或实时交易
    mode = input("请选择运行模式（1: 回测, 2: 实时交易）: ")
    
    if mode == '1':
        cerebro = bt.Cerebro()
        
        # 1. 加载数据（示例使用浦发银行600000.SH）
        data = AKShareData(dataname=fetch_data(symbol="600000", start_date="20200101"))
        cerebro.adddata(data)
        
        # 2. 添加策略
        cerebro.addstrategy(MultiIndicatorStrategy)
        
        # 3. 设置初始资金和手续费
        cerebro.broker.setcash(100000.0)
        cerebro.broker.setcommission(commission=0.0003)  # 0.03%手续费
        
        # 4. 运行回测
        print('初始资金: %.2f' % cerebro.broker.getvalue())
        cerebro.run()
        print('最终资金: %.2f' % cerebro.broker.getvalue())
        
        # 5. 可视化
        cerebro.plot(style='candlestick', volume=True)
    
    elif mode == '2':
        live_trading()
    else:
        print("无效的选择，请输入1或2。")
