# -*- coding: utf-8 -*-
"""
Created on Tue Jul 10 11:21:40 2018

这个脚本里会放很多自己写的工具函数。

@author: zhangshuai
"""

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from libs import tools_data


def get_trading_days(begin_date=None, end_date=None, market='SSE'):
    data = tools_data.load('basic/basic_trading_day', begin_date, end_date,
                           date_name='trading_day')
    market = 'HKEX' if market in ['HK', 'hk'] else 'SSE' if market in \
                                                            ['A', 'a'] else market
    trading_day = data[data['market'] == market]['trading_day']
    trading_day.index = trading_day
    return trading_day


def mergeAHdata(df_A, df_hk, trading_day='A'):
    """
    合并AH数据，并只保留A股交易日
    """
    if trading_day == 'A':
    # 将A股交易日港股休市的数据向后填充，但最多填充10天，把节假日跨过就行
        if len(df_A.T) > 0:
            df_hk = pd.concat([df_A.iloc[:, 0], df_hk], axis=1).ffill(
                limit=10).iloc[:, 1:]
        else:
            df_hk = pd.concat([df_A, df_hk], axis=1).ffill(limit=10)
        df_hk = df_hk.loc[df_A.index]
    elif trading_day == 'HK':
        if len(df_hk.T) > 0:
            df_A = pd.concat([df_hk.iloc[:, 0], df_A], axis=1).ffill(
                limit=10).iloc[:, 1:]
        else:
            df_A = pd.concat([df_hk, df_A], axis=1).ffill(limit=10)
        df_A = df_A.loc[df_hk.index]
    df = pd.concat([df_A, df_hk], axis=1)
    return df


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


def load_unit_stock_ims(unit_id_list, begin_date, end_date):
    """
    读取内部资产单元股票持仓、交易数据
    """
    if isinstance(unit_id_list, int):
        unit_id_list = [unit_id_list]
    # 这里需要提前把市值、交易额等空值转换为0
    data = tools_data.load('efund/unit_stock_ims', begin_date, end_date,
                           unit_id_list=unit_id_list)
    return data


def load_unit_ims(unit_id_list, begin_date, end_date):
    """
    读取内部资产单元整体数据
    """
    if isinstance(unit_id_list, int):
        unit_id_list = [unit_id_list]
    data = tools_data.load('efund/unit_ims', begin_date, end_date,
                           unit_id_list=unit_id_list)
    return data


def add_ind(data):
    """
    给持仓个股添加行业代码.

    Parameters
    ----------
    data: DataFrame，内部基金日度持仓交易数据，格式如下：
    （至少包含date和stk_code两列）
    date stk_code weight mv vol amount
    2021-01-04 002050.SZ 0.0 0.0 -422693.0 -11400614.96
    2021-01-04 002153.SZ 0.097 25071319.8 13800.0 428042.0
    2021-01-04 002410.SZ 0.100 25823474.57 0.0 0.0
    2021-01-05 002568.SZ 0.063 16393923.0 0.0 0.0

    Returns
    -------
    DataFrame, 在输入的data右边加上申万一级、二级行业代码
    """
    begin_date = data['date'].min()
    end_date = data['date'].max()
    stock_codes = data['stk_code'].drop_duplicates()
    for level in [1, 2]:
        ind = get_stock_ind(begin_date=begin_date, end_date=end_date,
                            ind_level=level, stock_codes=stock_codes)
        if ind is None:
            ind = pd.DataFrame([], columns=['date', 'stk_code', 'ind%s' % level])
            ind['date'] = ind['date'].astype('datetime64[ns]')
        else:
            ind = ind.ffill().bfill().stack().reset_index()
            ind.columns = ['date', 'stk_code', 'ind%s' % level]
        data = data.merge(ind, how='left')
    return data


def ex_ipo(data):
    """
    剔除持仓中打新的部分

    Parameters
    ----------
    data: DataFrame，内部基金日度持仓交易数据，格式如下：
    （至少包含date、stk_code、vol三列）
    date stk_code weight mv vol amount
    2021-01-04 002050.SZ 0.0 0.0 -422693.0 -11400614.96
    2021-01-04 002153.SZ 0.097 25071319.8 13800.0 428042.0
    2021-01-04 002410.SZ 0.100 25823474.57 0.0 0.0
    2021-01-05 002568.SZ 0.063 16393923.0 0.0 0.0

    Returns
    -------
    DataFrame, 删除了data中打新中的新股数据
    """
    # 加上上市时间
    ipo_date = tools_data.load('basic/basic_stock_a')[['stk_code',
                                                       'list_date']]
    ipo_date_H = tools_data.load('basic/basic_stock_hk')[[
        'stk_code', 'list_date']]
    ipo_date = pd.concat([ipo_date, ipo_date_H])
    ipo_date = ipo_date[ipo_date['stk_code'].isin(data['stk_code'])]
    ipo_date.columns = ['stk_code', 'list_date']
    data = data.merge(ipo_date, how='left')
    data['list_date'] = data['list_date'].astype('datetime64[ns]')
    data['list_days'] = (data['date'] - data['list_date']).dt.days

    # 剔除新股
    # S1:获取新股代码
    ipo_share = data[(data['list_days'] < 0) & (data['vol'] > 0)]
    ipo_share = ipo_share['vol'].groupby(ipo_share['stk_code']).sum(
    ).reset_index()
    ipo_share.columns = ['stk_code', 'ipo_share']
    data = data.merge(ipo_share, how='left')

    # S2:首先剔除未上市新股
    data = data[data['list_days'] >= 0]

    # S3:剔除上市至当天买入量为0的新股
    # 若上市时间超过200天仍没有卖出，但也没买入，也将该记录保留，认为是主动操作
    trade_vol = data.pivot('date', 'stk_code', 'vol')
    trade_vol[trade_vol.isnull()] = 0
    trade_vol[trade_vol < 0] = 0
    cum_buy = trade_vol[trade_vol >= 0].cumsum()
    if len(cum_buy) > 0:
        cum_buy = cum_buy.stack()
        cum_buy.name = 'cum_buy'
        cum_buy = cum_buy.reset_index()
        data = data.merge(cum_buy, how='left')
    else:
        data['cum_buy'] = 0
    data = data[~((data['cum_buy'] == 0)
                  & (data['list_days'] < 200)
                  & (data['stk_code'].isin(ipo_share['stk_code'])))]
    data_ex_ipo = data.copy()
    data_ex_ipo = data_ex_ipo.drop(['list_date', 'list_days', 'ipo_share',
                                    'cum_buy'], axis=1)
    return data_ex_ipo


def get_stock_ind(begin_date, end_date, ind_type='sw', ind_level=1, area=None,
                  stock_codes=None):
    """
    获取股票所属行业代码

    Parameters
    ----------
    ind_type: 'sw' or 'ci', default 'sw'.
    ind_level: 1\2\3, default 1.
    area: 'a' or 'hk', default 'a'.
    stock_codes: list, set, or Series, default None.

    Returns
    -------
    DataFrame, with date as index, stockcode as columns and industry_code as
    values.
    """
    stock_ind = tools_data.load('stock/stock_ind')
    stock_ind = stock_ind[(stock_ind['ind_type'] == ind_type)
                          & (stock_ind['ind_level'] == ind_level)
                          & (stock_ind['entry_date'] <= end_date)
                          & (~(stock_ind['remove_date'] <= begin_date))]
    if area is not None:
        stock_ind = stock_ind[stock_ind['area'] == area.lower()]
    if stock_codes is not None:
        stock_ind = stock_ind[stock_ind['stk_code'].isin(stock_codes)]
    if len(stock_ind) <= 0:
        return None
    stock_ind = stock_ind.pivot('entry_date', 'stk_code', 'ind_code').ffill()
    trading_day = get_trading_days(begin_date, end_date, market=area if area
                                                                        is not None else 'A')
    stock_ind = pd.concat([trading_day, stock_ind], axis=1).iloc[:, 1:].ffill(
    ).loc[trading_day]
    return stock_ind


def get_scale_weight(unit_id_list, begin_date=None, end_date=None):
    """
    获取多个资产单元合成指标的加权权重。
    """
    # 查询资产规模
    scale = tools_data.load('efund/unit_ims', begin_date, end_date,
                            unit_id_list=unit_id_list)
    scale_total = scale['net_asset'].groupby(scale['date']).sum()
    scale_total.name = 'scale_total'
    scale = scale.merge(scale_total.reset_index(), 'left')
    # 计算规模权重
    scale['scale_weight'] = scale['net_asset'] / scale['scale_total']
    scale = scale[['date', 'unit_id', 'scale_weight']]
    return scale


def add_dividend(data):
    """
    给基金持仓信息中添加股票分红除权数据。
    """
    stk_codes = data['stk_code'].drop_duplicates().sort_values()
    dates = data['date'].drop_duplicates().sort_values()
    dates.index = dates
    codes_hk = stk_codes[stk_codes.apply(lambda x: x[-2:] == 'HK')]
    codes_hk = codes_hk.drop_duplicates().sort_values()

    div0 = tools_data.load('stock/dividend_a')
    div0 = div0[div0['stk_code'].isin(stk_codes) & (div0['ex_dt'] >= dates[0]) &
                (div0['ex_dt'] <= dates[-1])]
    # A股现金分红
    div = div0[['ex_dt', 'stk_code', 'cash_div']].drop_duplicates()
    div = div[div['cash_div'] > 0]
    div = div[~div[['ex_dt', 'stk_code']].duplicated()]
    div = div.pivot('ex_dt', 'stk_code', 'cash_div')
    # A股送转股
    stk_div = div0[['ex_dt', 'stk_code', 'stk_div']].drop_duplicates()
    stk_div = stk_div[stk_div['stk_div'] > 0]
    stk_div = stk_div[~stk_div[['ex_dt', 'stk_code']].duplicated()]
    stk_div = stk_div.pivot('ex_dt', 'stk_code', 'stk_div')

    if len(codes_hk) > 0:
        div_hk0 = tools_data.load('stock/dividend_hk')
        div_hk0 = div_hk0[div_hk0['stk_code'].isin(stk_codes) &
                          (div_hk0['ex_dt'] >= dates[0]) &
                          (div_hk0['ex_dt'] <= dates[-1])]
        # 港股现金分红
        div_hk = div_hk0[['ex_dt', 'stk_code', 'cash_div_ratio']].drop_duplicates()
        div_hk = div_hk[div_hk['cash_div_ratio'] > 0]
        div_hk = div_hk[~div_hk[['ex_dt', 'stk_code']].duplicated()]
        div_hk = div_hk.pivot('ex_dt', 'stk_code', 'cash_div_ratio')
        # 港股送转股
        stk_div_hk = div_hk0[['ex_dt', 'stk_code', 'stk_div']].drop_duplicates()
        stk_div_hk = stk_div_hk[stk_div_hk['stk_div'] > 0]
        stk_div_hk = stk_div_hk[~stk_div_hk[['ex_dt', 'stk_code']].duplicated()]
        stk_div_hk = stk_div_hk.pivot('ex_dt', 'stk_code', 'stk_div')

        # 只保留有除权的日期
        div_hk = div_hk[div_hk.notnull().any(axis=1)]
        if len(div_hk) > 0:
            # 若港股除权日在非A股交易日，则将其滞后到相邻A股交易日
            div_hk = pd.concat([dates, div_hk], axis=1).iloc[:, 1:]
            div_hk1 = div_hk.copy()
            t0 = None
            for i, t in enumerate(div_hk.index):
                if (t not in dates) & (t0 is None):
                    # 非A股交易日，记录下来
                    t0 = t
                elif (t in dates) & (t0 is not None):
                    # A股交易日，且在之前A股休市期间有港股除权，将这几天的除权相加
                    div_hk1.loc[t] = div_hk[t0:t].sum()
                    t0 = None
            div_hk1 = div_hk1[div_hk1.index.isin(dates)]
            div_hk1[div_hk1 == 0] = np.nan
            div = pd.concat([div, div_hk1], axis=1)

        if len(stk_div_hk) > 0:
            stk_div_hk = pd.concat([dates, stk_div_hk], axis=1).iloc[:, 1:]
            stk_div_hk1 = stk_div_hk.copy()
            t0 = None
            for i, t in enumerate(stk_div_hk.index):
                if (t not in dates) & (t0 is None):
                    # 非A股交易日，记录下来
                    t0 = t
                elif (t in dates) & (t0 is not None):
                      # A股交易日，且在之前A股休市期间有港股除权，将这几天的除权相乘
                    stk_div_hk1.loc[t] = (1 + stk_div_hk[t0:t]).cumprod().iloc[-1] - 1
                    t0 = None
            stk_div_hk1 = stk_div_hk1[stk_div_hk1.index.isin(dates)]
            stk_div = pd.concat([stk_div, stk_div_hk1], axis=1)

        vol = data.pivot('date', 'stk_code', 'position')
        div_total = (vol.shift(1) * div).stack().reset_index()
        div_total.columns = ['date', 'stk_code', 'div']
        data = data.merge(div_total, how='left')

        if len(stk_div) > 0:
            stk_div = stk_div.stack().reset_index()
            stk_div.columns = ['date', 'stk_code', 'stk_div']
            stk_div.date = stk_div.date.astype('datetime64[ns]')
            data = data.merge(stk_div, how='left')
        else:
            data['stk_div'] = np.nan
        return data


def add_price(data, fields=None):
    """
    给基金持仓信息中添加股票行情数据

    Parameters
    ----------
    data: DataFrame，内部基金日度持仓交易数据，格式如下：
    date stk_code weight mv vol amount
    2021-01-04 002050.SZ 0.0 0.0 -422693.0 -11400614.96
    2021-01-04 002410.SZ 0.100 25823474.57 0.0 0.0
    2021-01-05 002568.SZ 0.063 16393923.0 0.0 0.0
    field: list, 要添加的行情数据类型，如['close', 'vwap', 'high', 'low', 'open',
    'pre_close', 'adj_factor', 'vol', 'adj_close']

    Returns
    -------
    DataFrame, 删除了data中打新中的新股数据
    """
    stk_codes = data['stk_code'].drop_duplicates().sort_values()
    dates = data['date'].drop_duplicates()
    codes_hk = stk_codes[stk_codes.apply(lambda x: x[-2:] == 'HK')]
    codes_hk = codes_hk.drop_duplicates().sort_values()

    t0 = dates.min() - relativedelta(days=14)
    t1 = dates.max()
    if isinstance(fields, str):
        fields = [fields]

    for field in fields:
        close = tools_data.load('stock/quote_a', t0, t1, pivot_columns=[
            'date', 'stk_code', field])
    if len(codes_hk) > 0:
        close_hk = tools_data.load('stock/quote_hk', t0, t1, pivot_columns=[
            'date', 'stk_code', field])
    close = mergeAHdata(close, close_hk)
    close = close[close.index.isin(dates)]
    close = close.stack().reset_index()
    field_name = ('volume' if field == 'vol' else field)
    close.columns = ['date', 'stk_code', field_name]
    data = data.merge(close, how='left')
    return data
