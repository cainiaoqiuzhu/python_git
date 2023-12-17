# -*- coding: utf-8 -*-
"""
Created on Sun Oct 22 17:41:29 2023

计算交易相关特征。

@author: zhangshuai
"""

import datetime as dt

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from libs import tools
from libs import tools_data
from libs import tools_general
from libs.log import lg


def update_turnover(unit_id_list, begin_date, end_date):
    """
    更新换手率
    """
    lg.info('updating turnover...')
    # 因为计算过程会涉及前1天的仓位，因此这里将begin_date前推2周(考虑节假日)
    t0 = dt.datetime.strptime(begin_date, '%Y%m%d') - relativedelta(weeks=2)
    close = tools_data.load('stock/quote_a', t0, end_date, pivot_columns=[
        'date', 'stk_code', 'close'])
    close_hk = tools_data.load('stock/quote_hk', t0, end_date,
                               pivot_columns=['date', 'stk_code', 'close'])
    close = tools.mergeAHdata(close, close_hk)
    ret_stk = close.pct_change()  # 股票日收益率
    for unit_id in unit_id_list:
        lg.info('updating turnover: %s...' % unit_id)
        data = tools.load_unit_stock_ims(unit_id, t0, end_date)
        data1 = tools.load_unit_ims(unit_id, t0, end_date).set_index('date')
        net_asset = data1['net_asset']

        # ===== 换手率（成交额推算）=====
        # 日买卖额
        buy_amount = data['amount'][data['amount'] > 0].groupby(data['date']).sum()
        buy_amount.name = 'buy_amount'
        sell_amount = data['amount'][data['amount'] < 0].groupby(data['date']).sum()
        sell_amount.name = 'sell_amount'
        # 日换手率
        buy_turn1 = (buy_amount / net_asset.shift(1))
        buy_turn1.name = 'buy_turn1'
        sell_turn1 = (sell_amount / net_asset.shift(1))
        sell_turn1.name = 'sell_turn1'

        # ===== 换手率（持仓权重推算）=====
        ret_stk_tmp = ret_stk.T[ret_stk.columns.isin(data['stk_code'])].T
        ret_fund = data1['ret_daily']  # 基金日收益率
        weight = data.pivot('date', 'stk_code', 'weight')  # 股票当日权重
        # 昨天的组合在今天的权重（不考虑调仓，仅由股价涨跌引起的变动）
        weight1 = weight.shift(1) * (1 + ret_stk_tmp)
        weight1 /= pd.DataFrame([(1 + ret_fund)], index=weight1.columns).T
        weight1[weight1.isnull()] = 0
        weight[weight.isnull()] = 0
        turn = (weight - weight1)
        buy_turn2 = turn[turn > 0].sum(axis=1)
        buy_turn2.name = 'buy_turn2'
        # 第一天的数据调整为0（因为weight1第一天的数据全为0）
        buy_turn2.iloc[0] = 0
        sell_turn2 = turn[turn < 0].sum(axis=1)
        sell_turn2.name = 'sell_turn2'

        result = pd.concat([buy_turn1, -sell_turn1, buy_turn2, -sell_turn2],
                           axis=1).reset_index()
        result = result[result['date'] >= begin_date]
        result['unit_id'] = unit_id
        result = result[['unit_id'] + list(result.columns[:-1])]
        tools_data.save(result, 'result/unit_turnover', primary_key=[
            'unit_id', 'date'])


def update_swing_trade_ret(unit_id_list, begin_date, end_date):
    """
    计算波段交易能力

    window: 几个月窗口内的交易算波段
    """
    lg.info('updating swing_trade_ret...')
    # 由于后续要计算滚动12个月窗口期的指标，因此这里多取13个月的原始数据
    t0 = dt.datetime.strptime(begin_date, '%Y%m%d') - relativedelta(months=13)
    for unit_id in unit_id_list:
        lg.info('updating swing_trade_ret: unit_id=%s...' % unit_id)
        data = tools.load_unit_stock_ims(unit_id, t0, end_date)
        if len(data) <= 0:
            lg.info('empty data found when updating swing_trade_ret: unit_id=%s...' % unit_id)
            continue
        data = tools.ex_ipo(data)
        if len(data) <= 0:
            lg.info('empty data found when updating swing_trade_ret after ex_ipo: unit_id=%s...' % unit_id)
            continue
        data = tools.add_dividend(data)
        data = tools.add_price(data, fields='adj_factor')
        data['vwap_trade'] = data['amount'] / data['vol']

        data1 = tools.load_unit_ims(unit_id, t0, end_date).set_index('date')
        net_asset = data1['net_asset']

        dates = data['date'].drop_duplicates().sort_values()
        dates = dates[dates > begin_date]
        dates = tools_general.get_target_day(dates, freq='m', which_day=31)
        result = pd.DataFrame()
        for window in [1, 2, 3, 4, 5, 6, 9, 12]:
            lg.info('updating swing_trade_ret: unit_id=%s, window=%s...' % (
                unit_id, window))
            ret_trade = pd.DataFrame()
            for t in dates:
                start_date = dt.datetime.strptime('%s-%s-01' %
                                                  (t.year, t.month), '%Y-%m-%d')
                df = data[(data['date'] > start_date - relativedelta(
                    months=window - 1)) & (data['date'] <= t)]
                # 期间有除权的股票代码
                codes_stk_div = df[df['stk_div'] > 0]['stk_code'].tolist()
                df1 = df[df['amount'] != 0]
                count = df1['amount'].groupby([df1['stk_code'], df1['vol'] > 0
                                               ]).count().reset_index()
                count.columns = ['stk_code', 'direction', 'count']
                # 筛选窗口期内有双向交易的股票
                count1 = count['direction'].groupby(count['stk_code']).count()
                codes_swing = list(count1[count1 > 1].index)
                ret_trade_tmp = []
                for code in codes_swing:
                    df2 = df[df['stk_code'] == code].copy().sort_values('date')
                if code in codes_stk_div:
                    # 若交易的股票在窗口期内有除权，则将持仓股数、交易量和
                    # 交易均价按照后复权进行调整
                    adj_stk = 1
                    adj_factor = df2['adj_factor'].iloc[0]
                    for i, t1 in enumerate(df2['date']):
                        if df2['stk_div'].iloc[i] > 0:
                            adj_stk *= 1 + df2['stk_div'].iloc[i]
                # 后复权处理
                df2['position'].iloc[i] /= adj_stk
                df2['vol'].iloc[i] /= adj_stk
                df2['vwap_trade'].iloc[i] *= (
                        df2['adj_factor'].iloc[i] / adj_factor)
                df2 = df2[df2['amount'] != 0]
                # 窗口期初始持股数
                vol_begin = df2['position'].iloc[0] - df2['vol'].iloc[0]
                # 窗口期结束持股数（后复权）
                vol_end = df2['position'].iloc[-1]
                # 窗口期内持股数变化（后复权），即主动加减仓
                vol_delta = vol_end - vol_begin
                # 从最新一天往前推，把主动加减仓的股数去掉，保证波段交易持股数变化为0
                for i in range(len(df2) - 1, -1, -1):
                    vol_tmp = df2['vol'].iloc[i]  # 当天交易量
                if np.sign(vol_tmp) == np.sign(vol_delta):
                    # 若整个区间为减仓，则只对卖出日的卖出量做调整
                    # 若整个区间为加仓，则只对买入日的买入量做调整
                    if abs(vol_tmp) >= abs(vol_delta):
                        # 当天卖出量大于整个窗口期减仓量，则将当天卖出量
                        # 进行调整，减掉整个窗口期减仓量，然后结束循环
                        df2['vol'].iloc[i] -= vol_delta
                        break
                    else:
                        # 否则，当天卖出量小于整个窗口期减仓量，说明在该天之前
                        # 的卖出量还有属于减仓的操作，因此将当天的交易量调整为0
                        # （即当天的所有交易都不属于波段交易），并计算新的“整
                        # 个窗口期减仓量”（即剔除当天的卖出量），然后继续循环
                        df2['vol'].iloc[i] = 0
                        vol_delta -= vol_tmp
                # 波段操作的金额收益
                gain_trade = -(df2['vol'] * df2['vwap_trade']).sum()
                # 用波段操作实际区间的规模均值作为分母
                t_begin = df2['date'].iloc[0]  # 波段操作起点
                # 波段操作实际终点(因为之前对vol进行了调整，所以vol有些为0)
                t_end = df2[df2['vol'] != 0]['date'].iloc[-1]
                asset_mean = net_asset[t_begin:t_end].mean()
                # 波段操作的收益率
                ret_trade_tmp.append(gain_trade / asset_mean)
            ret_trade_tmp = pd.Series(ret_trade_tmp, index=codes_swing)
            ret_trade_tmp = ret_trade_tmp.reset_index()
            ret_trade_tmp.columns = ['stk_code', 'swing_trade_ret']
            ret_trade_tmp['date'] = t
            ret_trade = ret_trade.append(ret_trade_tmp)
            ret_trade_total = ret_trade['swing_trade_ret'].groupby(
                ret_trade['date']).sum()
            ret_trade_total = tools_general.tradingday2natural(ret_trade_total)
            ret_trade_total = ret_trade_total.reset_index()
            ret_trade_total['unit_id'] = unit_id
            ret_trade_total['window'] = window
            result = result.append(ret_trade_total)
        tools_data.save(result, 'result/unit_swing_trade_ret', primary_key=[
            'unit_id', 'window', 'date'])

def map_ind(codes=None, name=None, field='name', short=True):
    indname = tools_data.load('stock/ind_all_name').set_index('ind_code')[
            'ind_name']
    if isinstance(codes, list):
        codes = pd.Series(codes, index=codes)
    if codes is not None:
        codes1 = codes[codes.notnull()]
        name = indname[codes1]
    if isinstance(codes, pd.Series):
        name.index = codes1.index
    name = name.apply(lambda x: x.replace('Ⅱ', ''))
    name = name.apply(lambda x: x.replace('Ⅲ', ''))
    if short:
        shortnames = {
            '医药生物': '医药', '食品饮料': '饮食', '机械设备': '机械',
            '基础化工': '化工', '家用电器': '家电', '建筑材料': '建材',
            '纺织服饰': '纺服', '轻工制造': '轻工', '建筑装饰': '建筑',
            '电力设备': '电设', '有色金属': '有色', '美容护理': '美容',
            '公用事业': '公用', '商贸零售': '商贸', '社会服务': '社服',
            '农林牧渔': '农业', '房地产': '地产', '国防军工': '军工',
            '交通运输': '交运', '石油石化': '石油', '非银金融': '非银',
            '纺织服装': '纺服', '非银行金融': '非银', '电力设备及新能源': '电新',
            '综合金融': '综金', '消费者服务': '服务', '电力及公用事业': '公用',
        }
        for s in shortnames.keys():
            name[name == s] = shortnames[s]
    return name


class add_up_data:
    """
    Add daily data to weekly/monthly/quarterly/... data.
    Use the last day of each week as the index.
    """

    def __init__(self, data, freq='week', name=None):

        """
        data: Series, with datetime as index.
        """
        self.data = data
        self.name = name
        self.index = pd.Series(data.index, index=data.index)
        freq = unify_freq(freq)
        self.freq = freq
        if freq == 'weekly':
            # 这里不能直接按照(index.year,index.week)进行groupby
            # 否则遇到20141230这种，处于年末但周已经算作次年第一周
            self.groupby_id = self.index.apply(lambda x: x.isocalendar()[0:2])
        elif freq == 'monthly':
            self.groupby_id = self.index.apply(lambda x: (x.year, x.month))
        elif freq == 'quarterly':
            self.groupby_id = self.index.apply(lambda x: (x.year, x.quarter))
        elif self.freq == 'semiannually':
            self.groupby_id = self.index.apply(lambda x: (x.year, 1 if x.month <= 6 else 2))
        elif self.freq == 'annually':
            self.groupby_id = self.index.apply(lambda x: x.year)
        elif freq == 'daily':
            self.groupby_id = self.index.apply(lambda x: (x.year, x.month, x.day))
            self.group_index = self.index.groupby(self.groupby_id).apply(lambda x: x[-1])

    def sum(self):

        data = self.data.groupby(self.groupby_id).sum()
        data.index = self.group_index
        if self.name is not None:
            data.name = self.name
        return data

    def mean(self):

        data = self.data.groupby(self.groupby_id).mean()
        data.index = self.group_index
        if self.name is not None:
            data.name = self.name
        return data

    def median(self):

        data = self.data.groupby(self.groupby_id).median()
        data.index = self.group_index
        if self.name is not None:
            data.name = self.name
        return data

    def count(self):

        data = self.data.groupby(self.groupby_id).count()
        data.index = self.group_index
        if self.name is not None:
            data.name = self.name
        return data

    def lastvalue(self):

        data = self.data.groupby(self.groupby_id).apply(lambda x: x[-1])
        data.index = self.group_index
        if self.name is not None:
            data.name = self.name
        return data

    def change(self, pct=False, period=1):

        data = self.data.groupby(self.groupby_id).apply(lambda x: x[-1])
        if pct is True:
            if period >= 0:
                data = data / data.shift(period) - 1
            else:
                data = data.shift(period) / data - 1
        else:
            if period >= 0:
                data = data - data.shift(period)
            else:
                data = data.shift(period) - data
        data.index = self.group_index
        if self.name is not None:
            data.name = self.name
        return data

    def std(self):

        data = self.data.groupby(self.groupby_id).std()
        data.index = self.group_index
        if self.name is not None:
            data.name = self.name
        return data

    def std_d(self):

        data = self.data.groupby(self.groupby_id).apply(downside_std)
        data.index = self.group_index
        if self.name is not None:
            data.name = self.name
        return data

    def MDD(self):

        """
        data should be a series of ret
        """
        data = self.data.groupby(self.groupby_id).apply(period_drawdown)
        data.index = self.group_index
        if self.name is not None:
            data.name = self.name
        return data


def update_trade_efficiency(unit_id_list, begin_date, end_date):
    """
    计算操作效率
    """
    lg.info('updating trade_efficiency...')
    # 由于后续要计算滚动12个月窗口期的指标，因此这里多取1年多的原始数据
    # 注意这里需要将起点设置为自然年末，因为后续会更新年度频率的数据
    t0 = '%s1231' % (dt.datetime.strptime(begin_date, '%Y%m%d').year - 2)
    close = tools_data.load('stock/quote_a', t0, end_date, pivot_columns=[
        'date', 'stk_code', 'close'])
    close_hk = tools_data.load('stock/quote_hk', t0, end_date,
                               pivot_columns=['date', 'stk_code', 'close'])
    close = tools.mergeAHdata(close, close_hk)
    for unit_id in unit_id_list:
        data = tools.load_unit_stock_ims(unit_id, t0, end_date)
        if len(data) <= 0:
            lg.info('empty data found when updating trade_efficiency: unit_id=%s...' % unit_id)
            continue
        data = tools.ex_ipo(data)
        if len(data) <= 0:
            lg.info('empty data found when updating trade_efficiency after ex_ipo: unit_id=%s...'
                    % unit_id)
            continue
        data = tools.add_dividend(data)
        weight = data.pivot(index='date', columns='stk_code', values='weight')
        mv_total = data['mv'].groupby(data['date']).sum()
        data1 = tools.load_unit_ims(unit_id, t0, end_date).set_index('date')
        net_asset = data1['net_asset']

    # ===== 实际收益：考虑实际交易价格，而非收盘价计算的收益率
    buy = data['amount'].groupby(data['date']).apply(lambda x: x[x > 0].sum())
    sell = data['amount'].groupby(data['date']).apply(lambda x: -x[x < 0].sum())
    div = data['div'].groupby(data['date']).sum()
    pnl_intraday1 = sell * (1 - 0.001 - 0.0008) - buy * (1 + 0.0008)
    pnl_intraday2 = sell * (1 - 0.0005 - 0.0004) - buy * (1 + 0.0004)
    t = '20230828'  # 这天下调印花税，后续还降佣金，统一从这一天考虑改变
    pnl_intraday = pd.concat([pnl_intraday1[:t], pnl_intraday2[t:]])
    ret_actual = (mv_total - mv_total.shift(1) + pnl_intraday + div
                  ) / net_asset.shift(1)
    # TODO：这里直接将首日的真实收益率取0，会使得结果有一些偏差
    # 但更不能直接不这样处理，否则其是缺失值。
    # 之后再来处理
    ret_actual.iloc[0] = 0
    ret_actual = ret_actual[ret_actual.notnull()]

    # ===== 操作效率：固定频率
    result = pd.DataFrame()
    for i, freq in enumerate(['m', 'q', 'a']):
        lg.info('updating trade_efficiency_periodical: unit_id=%s, freq=%s...' % (
            unit_id, freq))
    dates_m = tools_general.get_target_day(weight.index, freq=freq,
                                           which_day=366, begin=False)
    if weight.index[0] not in dates_m:
        dates_m.append(weight.index[0])
    dates_m.sort()
    # 期初持仓模拟区间收益
    ret_simu_m = (weight.loc[dates_m].shift(1) * close.loc[
        dates_m].pct_change()).sum(axis=1)
    # 实际区间收益
    ret_actual_m = (1 + ret_actual).cumprod().loc[dates_m].pct_change()
    efficiency_m = (ret_actual_m - ret_simu_m).iloc[1:]
    efficiency_m.name = 'efficiency'
    efficiency_m = tools_general.tradingday2natural(efficiency_m, freq=freq)
    efficiency_m = efficiency_m.reset_index()
    efficiency_m['unit_id'] = unit_id
    efficiency_m['freq'] = freq
    result = pd.concat([result, efficiency_m])
    tools_data.save(result, 'result/unit_trade_efficiency_periodical',
                    primary_key=['unit_id', 'freq', 'date'])

    # ===== 操作效率：滚动未来N个交易日
    result2 = pd.DataFrame()
    for i, window in enumerate([60, 120, 243]):
        lg.info('updating trade_efficiency_continuous: unit_id=%s, window=%s...' % (
            unit_id, window))
    close_tmp = close.T[close.columns.isin(weight.columns)].T
    # 期初持仓模拟区间收益
    # TODO:这一步耗时较长，后续可考虑优化一下
    # 注意这里不能直接用pct_change，因为min_periods=1，因此窗口期是可变的
    ret_tmp = close_tmp[::-1].rolling(window + 1, min_periods=1).apply(
        lambda x: x.iloc[0] / x.iloc[-1] - 1)
    ret_simu_m = (weight * ret_tmp).sum(axis=1).sort_index()
    # 实际区间收益
    ret_actual_m = (1 + ret_actual).cumprod()
    ret_actual_m = ret_actual_m[::-1].rolling(window + 1, min_periods=1).apply(
        lambda x: x.iloc[0] / x.iloc[-1] - 1).sort_index()
    efficiency_m = (ret_actual_m - ret_simu_m)
    efficiency_m.name = 'efficiency'
    efficiency_m = efficiency_m.reset_index()
    efficiency_m['unit_id'] = unit_id
    efficiency_m['window'] = window
    result2 = pd.concat([result2, efficiency_m])
    tools_data.save(result2, 'result/unit_trade_efficiency_continuous',
                    primary_key=['unit_id', 'window', 'date'])
