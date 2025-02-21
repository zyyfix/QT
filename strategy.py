import backtrader as bt
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import talib as ta
import akshare as ak

class MLStrategy(bt.Strategy):
    params = (
        ('ma_period1', 5),
        ('ma_period2', 10),
        ('cci_period', 14),
        ('bb_period', 20),
        ('bb_dev', 2),
        ('volume_ratio', 1.5),
        ('stop_loss', 0.05),
        ('rf_model', None),
        ('xgb_model', None)
    )

    def __init__(self):
        # 技术指标
        self.ma5 = bt.indicators.SMA(self.data.close, period=self.p.ma_period1)
        self.ma10 = bt.indicators.SMA(self.data.close, period=self.p.ma_period2)
        self.cci = bt.indicators.CCI(self.data, period=self.p.cci_period)
        self.bb = bt.indicators.BollingerBands(self.data.close, 
                                             period=self.p.bb_period,
                                             devfactor=self.p.bb_dev)
        self.vol_ma5 = bt.indicators.SMA(self.data.volume, period=5)
        
        self.order = None
        self.stop_price = None
        
        # 加载机器学习模型
        self.rf_model = self.p.rf_model
        self.xgb_model = self.p.xgb_model

    def get_features(self):
        """获取特征数据"""
        return np.array([
            self.ma5[0] / self.ma10[0] - 1,  # 均线差值比
            self.cci[0],
            (self.data.close[0] - self.bb.mid[0]) / self.bb.mid[0],  # BB带位置
            self.data.volume[0] / self.vol_ma5[0] - 1,  # 成交量比
            (self.data.high[0] - self.data.low[0]) / self.data.low[0],  # 振幅
            (self.data.close[0] - self.data.open[0]) / self.data.open[0]  # 涨跌幅
        ]).reshape(1, -1)

    def next(self):
        if self.order:
            return
            
        # 获取当前特征
        features = self.get_features()
        
        # 使用机器学习模型预测
        rf_pred = self.rf_model.predict_proba(features)[0][1]
        xgb_pred = self.xgb_model.predict_proba(features)[0][1]
        
        # 综合预测概率
        ml_signal = (rf_pred + xgb_pred) / 2 > 0.7  # 设置较高的阈值
        
        # 技术指标信号
        ma_cross = (self.ma5[0] > self.ma10[0]) and (self.ma5[-1] <= self.ma10[-1])
        cci_signal = (self.cci[0] > -100) and (self.cci[-1] <= -100)
        price_above_bbmid = self.data.close[0] > self.bb.mid[0]
        volume_spike = self.data.volume[0] > self.vol_ma5[0] * self.p.volume_ratio
        
        # 入场条件:技术指标 + 机器学习确认
        if not self.position:
            if ma_cross and cci_signal and price_above_bbmid and volume_spike and ml_signal:
                size = self.broker.getcash() * 0.9 / self.data.close[0]
                self.order = self.buy(size=size)
                self.stop_price = self.data.close[0] * (1 - self.p.stop_loss)
        
        # 离场条件
        elif self.position:
            ma_death = (self.ma5[0] < self.ma10[0]) and (self.ma5[-1] >= self.ma10[-1])
            cci_exit = (self.cci[0] < 100) and (self.cci[-1] >= 100)
            price_below_bblower = self.data.close[0] < self.bb.bot[0]
            stop_trigger = self.data.close[0] <= self.stop_price
            
            # 机器学习模型预测下跌概率高
            ml_exit = (rf_pred + xgb_pred) / 2 < 0.3
            
            if ma_death or cci_exit or price_below_bblower or stop_trigger or ml_exit:
                self.order = self.sell(size=self.position.size)
                self.stop_price = None

    def notify_order(self, order):
        if order.status in [order.Completed]:
            if order.isbuy():
                print(f'买入执行价格: {order.executed.price:.2f}')
            else:
                print(f'卖出执行价格: {order.executed.price:.2f}')
            self.order = None

def prepare_data(code, start_date, end_date):
    """准备分钟级数据并计算特征"""
    # 转换股票代码格式（去掉.SZ/.SH后缀）
    symbol = code.split('.')[0]
    
    # 添加重试机制
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # 尝试获取数据
            df = ak.stock_zh_a_hist_min_em(
                symbol=symbol, 
                period='1', 
                adjust='qfq',
                timeout=30  # 增加超时时间
            )
            
            if df.empty:
                raise ValueError(f"No data retrieved for {code}")
                
            # 重命名列以匹配原有代码
            df = df.rename(columns={
                '时间': 'trade_time',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume'
            })
            
            # 将时间列转换为datetime格式
            df['trade_time'] = pd.to_datetime(df['trade_time'])
            df = df.set_index('trade_time')
            
            # 筛选时间范围
            df = df[(df.index >= start_date) & (df.index <= end_date)]
            
            # 如果成功获取数据，计算技术指标
            if not df.empty:
                # 计算技术指标作为特征
                df['ma5'] = ta.SMA(df['close'].values, timeperiod=5)
                df['ma10'] = ta.SMA(df['close'].values, timeperiod=10)
                df['cci'] = ta.CCI(df['high'].values, df['low'].values, df['close'].values, timeperiod=14)
                upper, middle, lower = ta.BBANDS(df['close'].values, timeperiod=20)
                df['bb_pos'] = (df['close'] - middle) / middle
                df['vol_ratio'] = df['volume'] / ta.SMA(df['volume'].values, timeperiod=5)
                df['amplitude'] = (df['high'] - df['low']) / df['low']
                df['return'] = df['close'].pct_change()
                
                # 生成标签
                df['target'] = (df['close'].shift(-5) / df['close'] - 1 > 0.001).astype(int)
                
                return df
                
            return pd.DataFrame()
            
        except Exception as e:
            print(f"Attempt {retry_count + 1} failed for {code}: {str(e)}")
            retry_count += 1
            if retry_count < max_retries:
                print(f"Retrying... ({retry_count}/{max_retries})")
                import time
                time.sleep(5)  # 添加延迟，避免请求过于频繁
    
    print(f"Failed to retrieve data for {code} after {max_retries} attempts")
    return pd.DataFrame()

def train_models(train_data):
    """训练机器学习模型"""
    features = ['ma5/ma10', 'cci', 'bb_pos', 'vol_ratio', 'amplitude', 'return']
    X = train_data[features]
    y = train_data['target']
    
    # 随机森林
    rf_model = RandomForestClassifier(n_estimators=100, max_depth=5)
    rf_model.fit(X, y)
    
    # XGBoost
    xgb_model = xgb.XGBClassifier(max_depth=5, learning_rate=0.1)
    xgb_model.fit(X, y)
    
    return rf_model, xgb_model

def run_strategy(codes=['159920.SZ', '513050.SH'], # QDII ETF示例
                train_start='20130101',
                train_end='20151231',
                valid_start='20160101',
                valid_end='20191231',
                cash=1000000.0):
    """运行策略"""
    
    # 训练模型
    train_dfs = []
    valid_codes = []
    
    for code in codes:
        df = prepare_data(code, train_start, train_end)
        if not df.empty:
            train_dfs.append(df)
            valid_codes.append(code)
    
    if not train_dfs:
        raise ValueError("No valid data available for any of the provided codes")
    
    train_data = pd.concat(train_dfs)
    
    rf_model, xgb_model = train_models(train_data)
    
    # 回测
    cerebro = bt.Cerebro()
    
    for code in codes:
        # 获取验证期数据
        data = prepare_data(code, valid_start, valid_end)
        feed = bt.feeds.PandasData(dataname=data)
        cerebro.adddata(feed)
    
    # 添加策略
    cerebro.addstrategy(MLStrategy, 
                        rf_model=rf_model,
                        xgb_model=xgb_model)
    
    cerebro.broker.setcash(cash)
    cerebro.broker.setcommission(commission=0.0003)  # 设置较低的手续费
    
    print(f'初始资金: {cerebro.broker.getvalue():.2f}')
    cerebro.run()
    print(f'最终资金: {cerebro.broker.getvalue():.2f}')
    
    cerebro.plot()

if __name__ == '__main__':
    run_strategy(
        codes=['000001.SZ', '600000.SH'],  # 平安银行和浦发银行
        train_start='20230801',  # 使用更近的时间
        train_end='20230815',
        valid_start='20230816',
        valid_end='20230831',
        cash=1000000.0
    )