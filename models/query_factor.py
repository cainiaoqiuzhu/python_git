# -*- coding: utf-8 -*-
"""
Created on Tue Oct 10 08:46:18 2023

@author: zhangshuai
"""

from libs import tools
from libs import tools_data


def get_indicator_table(indicator_code):
    """
    根据indicator_code返回其所在表。
    此处不同表里的indicator_code需确保不会重复。
    若在factor.py里新增了指标，也需在这里同步新增。
    """
    fac = {}
    # 基础指标
    fac['basic'] = ['mv', 'float_mv', 'mv_log', 'float_mv_log',
                    'list_years', 'list_years_log']
    # 拥挤度
    fac['crowding'] = ['analyst_coverage', 'analyst_coverage_error',
                       'holding_fund_num', 'holding_fund_num_error',
                       'LGT_holding_ratio', 'GGT_holding_ratio']
    # 技术指标
    fac['tech'] = ['beta_24m', 'distance_1y',
                   # 动量
                   'mom20', 'mom40', 'mom60', 'mom90', 'mom120', 'mom243',
                   # 波动
                   'std20', 'std40', 'std60', 'std90', 'std120', 'std243',
                   # 换手率
                   'turn20', 'turn40', 'turn60', 'turn90', 'turn120', 'turn243']
    # 估值
    fac['value'] = ['ep_ttm', 'ep_fttm', 'ep_f0', 'ep_f1', 'ep_f2', 'bp_lf',
                    'sp_ttm', 'dividend_ratio', 'ebitda_ev',
                    # 二维估值
                    'peg_fttm', 'peg_f1', 'pb_roe_error',
                    # 历史估值分位数
                    'ep_ttm_pct_10y', 'ep_fttm_pct_10y', 'ep_0_pct_10y',
                    'ep_1_pct_10y', 'bp_pct_10y', 'sp_pct_10y']
    # 预期调整
    fac['forecast'] = ['np_fttm_d40', 'np_fttm_d60', 'np_fttm_d90', 'np_fttm_d120',
                       'np_delta40', 'np_delta60', 'np_delta90', 'np_delta120',
                       'or_fttm_d40', 'or_fttm_d60', 'or_fttm_d90', 'or_fttm_d120',
                       'or_delta40', 'or_delta60', 'or_delta90', 'or_delta120',
                       'roe_fttm_d40', 'roe_fttm_d60', 'roe_fttm_d90', 'roe_fttm_d120',
                       'roe_delta40', 'roe_delta60', 'roe_delta90', 'roe_delta120']
    # 盈利能力
    fac['earnings'] = [
        # 资产收益率
        'roe_fttm', 'roa_ttm', 'roa_ttm3y', 'roe_ttm', 'roe_ttm3y',
        'roa_q', 'roe_q', 'roic_ttm',
        # 利润率
        'opt_ratio_ttm', 'opt_ratio_q',
        'net_ratio_ttm', 'net_ratio_q',
        'gross_rate_ttm', 'gross_rate_q']
    # 成长能力
    fac['growth'] = [
        # 预期增速
        'np_fttm_yoy', 'np_f0_yoy', 'np_f1_yoy',
        'or_fttm_yoy', 'or_f0_yoy', 'or_f1_yoy',
        # 历史利润表增速
        'revenue_ttm_yoy', 'revenue_q_yoy',
        'opt_profit_ttm_yoy', 'opt_profit_q_yoy',
        'np_ttm_yoy', 'np_q_yoy',
        # 历史利润表同比增速的环比变化
        'revenue_ttm_yoy_d', 'revenue_q_yoy_d',
        'opt_profit_ttm_yoy_d', 'opt_profit_q_yoy_d',
        'np_ttm_yoy_d', 'np_q_yoy_d',
        # 历史资产负债表增速
        'asset_yoy', 'equity_yoy', 'liability_yoy', 'fix_asset_yoy',
        'asset_qoq', 'equity_qoq', 'liability_qoq', 'fix_asset_qoq',
        # 其他间接增速
        'construction2asset', 'rd_ratio_ttm', 'rd_ratio_4q_mean', 'staff_num_yoy']
    # 营运能力
    fac['operation'] = ['turn_asset', 'turn_fix_asset', 'turn_inventory',
                        'turn_receivable']
    # 偿债能力
    fac['solvency'] = [
        # 静态水平
        'debt2asset_ratio', 'current_ratio', 'quick_ratio',
        'cash_ratio', 'i_debt2asset_ratio',
        # 动态变化
        'debt2asset_ratio_yoy', 'current_ratio_yoy',
        'quick_ratio_yoy', 'cash_ratio_yoy', 'i_debt2asset_ratio_yoy',
        'debt2asset_ratio_qoq', 'current_ratio_qoq',
        'quick_ratio_qoq', 'cash_ratio_qoq', 'i_debt2asset_ratio_qoq']
    fac['cashflow'] = [
        # 现金流比例
        'cash2revenue', 'cash2revenue_lag1', 'cash2revenue_lag2',
        'cash2revenue_lag3', 'cash2revenue_lag4',
        'cfo2opt_profit', 'cfo2opt_profit_lag1', 'cfo2opt_profit_lag2',
        'cfo2opt_profit_lag3', 'cfo2opt_profit_lag4',
        # 现金流结构
        'cfo_in_ratio', 'cfi_in_ratio', 'cff_in_ratio',
        'cfo_out_ratio', 'cfi_out_ratio', 'cff_out_ratio',
        'cfo_in2asset', 'cfi_in2asset', 'cff_in2asset',
        'cfo_out2asset', 'cfi_out2asset', 'cff_out2asset',
        'cfo_net2asset', 'cfi_net2asset', 'cff_net2asset']
    name = None
    for table_name in fac.keys():
        if indicator_code in fac[table_name]:
            name = table_name
    if name is not None:
        return name
    else:
        raise ValueError("indicator_code '%s' does not exists." % indicator_code)


def get_factor(indicator_code, unit_id_list, begin_date=None, end_date=None):
    """
    查询股票组合因子暴露。
    对于该类指标，多个资产单元的合成指标可通过个单个资产单元指标按照规模加权而得。

    Parameters
    ----------
    indicator_code：指标代码，以Excel“指标梳理”为准。
    unit_id_list: 资产单元代码的列表，如[1437, 6533]。
    """
    # 从数据库查询数据
    table_name = get_indicator_table(indicator_code)
    data = tools_data.load('result/unit_factor_%s' % table_name, begin_date,
                           end_date, unit_id_list=unit_id_list)
    data = data[['date', 'unit_id', indicator_code]]
    if len(data) <= 0:
        return

    scale_weight = tools.get_scale_weight(unit_id_list, begin_date, end_date)
    data = data.merge(scale_weight, 'left')
    # 将因子值按照规模加权
    data[indicator_code] *= data['scale_weight']
    data = data.drop(['scale_weight', 'unit_id'], axis=1)
    # 计算合并组合的因子暴露
    data = data.groupby('date').sum().reset_index()
    data = data.sort_values('date', ascending=True)
    return data


def get_barra(indicator_code, unit_id_list, begin_date=None, end_date=None,
              model='CNE5'):
    """
    查询股票组合barra因子暴露。
    对于该类指标，多个资产单元的合成指标可通过个单个资产单元指标按照规模加权而得。

    Parameters
    ----------
    indicator_code：指标代码，以Excel“指标梳理”为准。
    unit_id_list: 资产单元代码的列表，如[1437, 6533]。
    model: 'CNE5' or 'ACH1'
    """
    if model.upper() == 'CNE5':
        data = tools_data.load('result/unit_factor_barra_cne5', begin_date,
                               end_date, unit_id_list=unit_id_list)
    elif model.upper() == 'ACH1':
        # TODO:待后续补充
        pass
    data = data[['date', 'unit_id', indicator_code]]
    scale_weight = tools.get_scale_weight(unit_id_list, begin_date, end_date)
    data = data.merge(scale_weight, 'left')
    # 将因子值按照规模加权
    data[indicator_code] *= data['scale_weight']
    data = data.drop(['scale_weight', 'unit_id'], axis=1)
    # 计算合并组合的因子暴露
    data = data.groupby('date').sum().reset_index()
    data = data.sort_values('date', ascending=True)
    return data
