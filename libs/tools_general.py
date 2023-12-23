# -*- coding: utf-8 -*-
"""
Created on Tue Oct 10 08:36:14 2023

该模块存放通用型函数，不涉及数据的读取。

@author: zhangshuai
"""

import datetime as dt

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta


def get_stock_area(stk_code):
    """
    判断股票所属地区
    stk_code: 股票万得代码
    """
    market = stk_code.split('.')[-1]
    area = ('A' if market in ['SZ', 'SH', 'BJ', 'NB'] else 'US' if market in ['O',
                                                                        'N'] else market)
    return area


def winsorize(df0, n=3, pct=0.005, method=1, min_value=None, max_value=None,
              axis=0, only_max=False, only_min=False):
    """
    缩尾处理。

    Parameters
    ----------
    df0: DataFrame/Series/list。
    n: method等于1时，n倍标准差以外的数据缩尾。
    pct: method等于2时，最大或最小pct的数据缩尾。
    method: 若为1，用标准差法缩尾；若为2，用分位数法缩尾；若为3，用给定的最大值或最
    小值缩尾，默认1。
    min_value: 数据下限，默认为空，若非空，则method更改为3。
    max_value: 数据上限，默认为空，若非空，则method更改为3。
    axis: 0 or 1，若为0，按列缩尾；若为1，按行缩尾。
    only_max: 若为True，只对上限缩尾。
    only_min: 若为True，只对下限缩尾。
    """
    if method == 3:  # 固定值法
        if (min_value is None) & (max_value is None):
            raise ValueError('please input min_value and max_value!')
    if isinstance(df0, pd.DataFrame):
        df = df0.copy()
        if axis == 1:
            df = df.T
        # return df.apply(lambda x: winsorize(x, n, pct, method, min_value,
        # max_value), raw=True)
        # 上面这种方式运行速度太慢，修改为下面这种。 -- by zs 20210614
        if (min_value is not None) | (max_value is not None):
            method = 3
        if method == 1:
            max_value = df.mean() + n * df.std()
            min_value = df.mean() - n * df.std()
        elif method == 2:
            max_value = df.quantile(1 - pct)
            min_value = df.quantile(pct)
        elif method == 3:
            max_value = pd.Series(max_value, index=df.columns)
            min_value = pd.Series(min_value, index=df.columns)
            max_value = pd.DataFrame([max_value], index=df.index)
            min_value = pd.DataFrame([min_value], index=df.index)
        if only_min is False:
            df[df > max_value] = max_value
        if only_max is False:
            df[df < min_value] = min_value
        if axis == 1:
            df = df.T
    else:
        if (isinstance(df0, pd.Series)) is False:
            df0 = pd.Series(df0)
        df = df0.copy()
        if (min_value is not None) | (max_value is not None):
            method = 3
        if method == 1:  # 标准差法
            max_value = df.mean() + n * df.std()
            min_value = df.mean() - n * df.std()
        elif method == 2:  # 分位数法
            min_value = df.quantile(pct)
            max_value = df.quantile(1 - pct)
            # df[df > max_value] = max_value
            # df[df < min_value] = min_value
        # 上面两行的方式运行速度太慢，下面快一些
        if only_min is False:
            df1 = pd.concat([df, pd.Series(max_value, index=df.index)],
                            axis=1).min(axis=1)
        if only_max is False:
            df1 = pd.concat([df1, pd.Series(min_value, index=df.index)],
                            axis=1).max(axis=1)
        # 上面2行会使得df里的空值也被赋值，因此要重新置空。--by zs 20210614
        df1[df.isnull()] = np.nan
        df = df1
        df.name = df0.name
    return df


def unify_stock_code(code):
    """
    对于纯数字股票代码，添加'.SH'等尾缀，并补足代码开头的0
    """
    if isinstance(code, pd.Series):
        return code.apply(unify_stock_code)
    if (len(code) == 6) & ('.' not in code):
        code = str(int(code))
        code = '0' * (6 - len(code)) + code
        if code[0:2] in ['00', '30']:
            code += '.SZ'
        elif code[0:2] in ['60', '68']:
            code += '.SH'
        elif code[0] in ['8', '4']:
            code += '.BJ'
    if (len(code) == 5) & ('.' not in code):
        if code[0] == '0':
            code = code[1:] + '.HK'
    return code


def unify_freq(freq='w'):
    freq = 'daily' if freq in ['d', 'D', 'day', 'Day'] else freq
    freq = 'weekly' if freq in ['w', 'W', 'week', 'Week'] else freq
    freq = 'monthly' if freq in ['m', 'M', 'month', 'Month'] else freq
    freq = 'quarterly' if freq in ['q', 'Q', 'quarter', 'Quarter'] else freq
    freq = 'semiannually' if freq in ['s', 'S', 'semiannual', 'Semiannual'
                                      ] else freq
    freq = 'annually' if freq in ['a', 'A', 'annual', 'Annual', 'y', 'year',
                                  'yearly'] else freq
    return freq


def downside_std(ret, method='Rf', Rf=0.02, freq='D'):
    '''
    This function calculates downside volatility.

    Parameters
    ----------
    ret: A T*n return.
    method:
    if == 'Rf': std_D = sqrt(1 / (T-1) * sum([min(R_t - Rf/243, 0)]^2));
    if == 'mean': std_D = sqrt(1 / (T-1) * sum([min(R_t - R_t_bar, 0)]^2)).
    Rf: annual risk free rate.
    freq: return frequency;
    D: daily; M: monthly; Q: quarterly; H: semiannually;
    A: annually.

    Returns
    -------
    std_d: A 1*n downside volatility.
    '''
    freq = unify_freq(freq)

    def fun(ret):

        '''
        ret: A T*1 Series.
        '''
        ret = np.matrix(ret)  # now ret is a 1 * T matrix
        if method == 'Rf':
            if freq == 'daily':
                T = 243
            elif freq == 'weekly':
                T = 50
            elif freq == 'monthly':
                T = 12
            elif freq == 'quarterly':
                T = 4
            elif freq == 'semiannually':
                T = 2
            elif freq == 'annually':
                T = 1
            deviation = ret - Rf / T
        elif method == 'mean':
            deviation = ret - np.nanmean(ret)
        if len(ret.T) <= 1:
            std_d = np.nan
        else:
            std_d = np.sqrt(np.sum(np.square(deviation[deviation < 0])) / (
                    len(ret.T) - 1))
        return std_d
    if isinstance(ret, pd.DataFrame):
        return ret.apply(fun)
    else:
        return fun(ret)


def period_drawdown(ret):
    """
    calculate period drawdown.

    Parameters
    ----------
    ret: A T*1 Series.
    """
    NAV = (1 + ret).cumprod()
    # 要加入初值1，否则计算回撤时会剔除第一天的跌幅
    initial_value = pd.Series([1], index=[NAV.index[0] - relativedelta(days=1)])
    NAV = pd.concat([initial_value, NAV])
    return max(1 - NAV / NAV.cummax())


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


def tradingday2natural(data, change_index=True, freq='m'):
    """
    将data的index中的交易日转换为相应周期最后1个自然日

    Parameters
    ----------
    data: a datetime, or Series/DataFrame with datetime index.
    """
    if isinstance(data, str):
        if len(data) == 8:
            data = dt.datetime.strptime(data, '%Y%m%d')
        elif len(data) == 10:
            data = dt.datetime.strptime(data, '%Y-%m-%d')
    if isinstance(data, dt.datetime):
        t = dt.datetime.strptime('%s-%s-01' % (data.year, data.month), '%Y-%m-%d'
                                 ) + relativedelta(months=1) - relativedelta(days=1)
        return t
    if change_index:
        index = pd.Series(data.index)
    else:
        index = data
    if freq == 'm':
        index = index.apply(lambda x: dt.date(x.year, x.month, 1) +
                                      relativedelta(months=1) - relativedelta(days=1))
    elif freq == 'q':
        month_delta = [1, 3, 2]
        index = index.apply(lambda x: dt.date(x.year, x.month, 1) +
                                      relativedelta(months=month_delta[x.month % 3]) -
                                      relativedelta(days=1))
    elif freq == 'a':
        index = index.apply(lambda x: dt.date(x.year, 1, 1) +
                                      relativedelta(years=1) - relativedelta(days=1))
    index = index.astype('datetime64[ns]')
    if change_index:
        data.index = index
    else:
        data = index
    return data


def get_target_day(dates, freq='daily', which_day=1, begin=True,
                   start_month=None):
    """
    获取指定频率下的目标交易日。

    freq: 'daily'\'weekly'\'monthly'\'quarterly'\'semiannually'\'annually'
    which_day: the serial number of the day in each period. eg: whichday=1, it
    represents we trade at the first day of a week/month.
    begin: select the begin or the end of each peirod(Q or H)
    start_month: if the freq is 'quarterly', we should furtherly define whether
    to choose [1,4,7,10], [2,5,8,11] or [3,6,9,12].
    """
    freq = unify_freq(freq)
    target_day = pd.Series(dates)
    target_day.index = target_day

    def get_week(t):

        return t.isocalendar()[0:2]  # this returns (year, week)

    def get_adjust_day(date, which_day):

        return date.tolist()[min(which_day, len(date)) - 1]

    if freq == 'weekly':
        which_day = 5 if which_day == -1 else which_day
        target_day = target_day.groupby(target_day.apply(get_week)).apply(
            get_adjust_day, which_day)
    elif freq in ['monthly', 'quarterly', 'semiannually']:
        which_day = 31 if which_day == -1 else which_day
        target_day = target_day.groupby([target_day.index.year, target_day.index.month
                                     ]).apply(get_adjust_day, which_day)
        target_day.index = target_day
        t_last = target_day.iloc[-1]
        if freq == 'quarterly':
            month = np.array([1, 4, 7, 10])
            if start_month is None:
                startmonth = 1 if begin is True else 3
            month += startmonth - 1
            target_day = target_day[target_day.index.month.isin(month)]
        elif freq == 'semiannually':
            month = np.array([1, 7])
            if startmonth is None:
                startmonth = 1 if begin is True else 6
            month += startmonth - 1
            target_day = target_day[target_day.index.month.isin(month)]
        if begin is False:
            if t_last not in target_day:
                target_day = target_day.append(pd.Series([t_last], index=[t_last]))
    elif freq == 'annually':
        target_day = target_day.groupby([target_day.index.year
                                        ]).apply(get_adjust_day, which_day)
    elif freq == 'daily':
        pass
    else:
        raise ValueError('input parameter "freq" is wrong!')
    target_day = target_day.tolist()
    return target_day
