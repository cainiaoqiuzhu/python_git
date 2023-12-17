# -*- coding: utf-8 -*-
"""
Created on Sun Oct 22 21:28:31 2023

@author: zhangshuai
"""

import pandas as pd

from libs import tools
from libs import tools_general
from libs import tools_data


def get_turnover(unit_id_list, begin_date=None, end_date=None, freq='m',
                 turn_type=2):
    """
    查询股票组合换手率。
    对于该类指标，多个资产单元的合成指标可通过个单个资产单元指标按照规模加权而得。

    Parameters
    ----------
    unit_id_list: 资产单元代码的列表，如[1437, 6533]。
    freq: frequency of turnover, can be month/quarter/annual, default month.
    turn_type: type of turnover，if 1: calculated by trading amount;
    if 2: calculated by diffrence of stock weight. Default 2.

    Returns
    -------
    result: 指定频率的换手率时序值
    """
    data = tools_data.load('result/unit_turnover', begin_date=begin_date,
                           end_date=end_date, unit_id_list=unit_id_list)
    data1 = tools.load_unit_ims(unit_id_list, begin_date, end_date)
    net_purchase_ratio = data1[['unit_id', 'date', 'net_purchase_ratio']]
    data = data.merge(net_purchase_ratio, 'left')

    field1 = 'buy_turn%s' % turn_type
    field2 = 'sell_turn%s' % turn_type
    fields = [field1, field2, ' ']
    data = data[['unit_id', 'date'] + fields]

    scale_weight = tools.get_scale_weight(unit_id_list, begin_date, end_date)
    data = data.merge(scale_weight, 'left')
    # 将换手率按照规模加权
    for field in fields:
        data[field] = data[field] * data['scale_weight']
    data = data.drop(['unit_id', 'scale_weight'], axis=1)
    data = data.groupby('date').sum()

    def fun(df):

        # 将日度数据聚合为月度/季度等
        df = tools_general.add_up_data(df, freq=freq).sum()
        # 将index中的月末交易日转换为月末自然日
        df = tools_general.tradingday2natural(df)
        return df

    result = data[fields].apply(fun)
    return result


def get_swing_trade_ret(unit_id_list, begin_date=None, end_date=None, window=None):
    """
    查询股票组合波段交易收益。
    对于该类指标，多个资产单元的合成指标可通过个单个资产单元指标按照规模加权而得。

    Parameters
    ----------
    window: 波段交易的窗口期。值域为[1, 2, 3, 4, 5, 6, 9, 12]（与update_swing_trade_ret
    中用到的循环窗口期相对应）

    Returns
    -------
    data: 指定窗口期的波段交易月度时序值.
    ret_mean: 平均波段收益。
    win: 月度胜率.
    """
    t1 = tools_general.tradingday2natural(end_date)
    data = tools_data.load('result/unit_swing_trade_ret', begin_date, end_date=t1,
                           unit_id_list=unit_id_list)
    data = data[data['window'] == window]
    dates = data['date'].drop_duplicates().sort_values()
    dates.index = dates
    scale_weight = tools.get_scale_weight(unit_id_list, begin_date, end_date)
    scale_weight = scale_weight.pivot('date', 'unit_id', 'scale_weight')
    scale_weight = pd.concat([dates, scale_weight], axis=1).ffill().iloc[:, 1:]
    scale_weight = scale_weight.stack().reset_index()
    scale_weight.columns = ['date', 'unit_id', 'scale_weight']
    data = data.merge(scale_weight, 'left')
    # 将波段收益按照规模加权
    data['swing_trade_ret'] *= data['scale_weight']
    data = data['swing_trade_ret'].groupby(data['date']).sum()
    ret_mean = data.mean()
    win = (data > 0).sum() / (data != 0).sum()  # 胜率
    data = data.reset_index()
    return data, ret_mean, win
