# coding: utf-8

import pandas as pd

from libs.db_connect import query_pd_infodb
from libs import tools
from libs import tools_data
from libs import tools_general
from libs.log import lg


def update_quote_a(begin_date, end_date):
    """
    下载A股价格数据
    """
    lg.info('update_quote_a...')
    query = '''select S_INFO_WINDCODE stk_code, TRADE_DT "date",
 S_DQ_PRECLOSE pre_close, S_DQ_OPEN "open",
 S_DQ_HIGH high, S_DQ_LOW low, S_DQ_CLOSE close,
 S_DQ_VOLUME * 100 vol, S_DQ_AMOUNT * 1e3 amount,
 S_DQ_TRADESTATUS trade_status, S_DQ_LIMIT up_limit,
 S_DQ_STOPPING down_limit, S_DQ_ADJFACTOR adj_factor,
 S_DQ_CLOSE * S_DQ_ADJFACTOR adj_close,
 case when S_DQ_VOLUME = 0 then S_DQ_CLOSE
 else S_DQ_AMOUNT / S_DQ_VOLUME * 10
 end vwap
 from tydw_filesync.AShareEODPrices
 where TRADE_DT >= %s and TRADE_DT <= %s
 ''' % (begin_date, end_date)
    data = query_pd_infodb(query)
    if len(data) == 0:
        return
    data['date'] = data['date'].astype('datetime64[ns]')
    data = data.sort_values(['date', 'stk_code'])
    tools_data.save(data, path='stock/quote_a', primary_key=[
        'date', 'stk_code'])


def update_derivative_a(begin_date, end_date):
    """
    下载A股衍生数据
    """
    lg.info('update_derivative_a...')
    query = '''select a.S_INFO_WINDCODE stk_code, a.TRADE_DT "date",
 S_VAL_PB_NEW pb_lf, S_VAL_PE_TTM pe_ttm,
 S_VAL_PS_TTM ps_ttm, TOT_SHR_TODAY total_share,
 FLOAT_A_SHR_TODAY float_share,
 FREE_SHARES_TODAY free_share,
 TOT_SHR_TODAY * b.S_DQ_CLOSE * 1e4 total_mv,
 FLOAT_A_SHR_TODAY * b.S_DQ_CLOSE * 1e4 float_mv,
 FREE_SHARES_TODAY * b.S_DQ_CLOSE * 1e4 free_mv
 from tydw_filesync.AShareEODDerivativeIndicator a
 left join tydw_filesync.AShareEODPrices b
 on a.S_INFO_WINDCODE = b.S_INFO_WINDCODE
 and a.TRADE_DT = b.TRADE_DT
 where a.TRADE_DT >= %s and a.TRADE_DT <= %s
 ''' % (begin_date, end_date)
    data = query_pd_infodb(query)
    if len(data) == 0:
        return
    data['date'] = data['date'].astype('datetime64[ns]')
    data = data.sort_values(['date', 'stk_code'])

    # 剔除非交易日数据
    tradingday = tools.get_trading_days(begin_date, end_date)
    data = data[data['date'].isin(tradingday)]

    tools_data.save(data, path='stock/derivative_a', primary_key=[
        'date', 'stk_code'])


def update_quote_hk(begin_date, end_date):
    """
    下载港股价格数据
    """
    lg.info('update_quote_hk...')
    query = '''select S_INFO_WINDCODE stk_code, TRADE_DT "date",
 S_DQ_PRECLOSE pre_close, S_DQ_OPEN "open",
 S_DQ_HIGH high, S_DQ_LOW low, S_DQ_CLOSE close,
 S_DQ_VOLUME vol, S_DQ_AMOUNT *1e3 amount,
 S_DQ_ADJFACTOR adj_factor,
 S_DQ_CLOSE * S_DQ_ADJFACTOR adj_close,
 case when S_DQ_VOLUME = 0 then S_DQ_CLOSE
 else S_DQ_AMOUNT / S_DQ_VOLUME * 1000
 end vwap
 from tydw_filesync.HKshareEODPrices
 where TRADE_DT >= %s and TRADE_DT <= %s
 ''' % (begin_date, end_date)
    data = query_pd_infodb(query)
    if len(data) == 0:
        return
    data['date'] = data['date'].astype('datetime64[ns]')
    data = data.sort_values(['date', 'stk_code'])
    tools_data.save(data, path='stock/quote_hk', primary_key=[
        'date', 'stk_code'])


def update_derivative_hk(begin_date, end_date):
    """
    下载港股衍生数据
    """
    lg.info('update_derivative_hk...')
    query = '''select S_INFO_WINDCODE stk_code, FINANCIAL_TRADE_DT "date",
 TOT_SHR_TODAY * 1e4 total_share,
 FLOAT_A_SHR_TODAY * 1e4 float_share,
 S_VAL_MV * 1e4 total_mv,
 S_DQ_MV * 1e4 float_mv,
 S_VAL_PB_NEW pb, S_VAL_PE_TTM pe_ttm, S_VAL_PS_TTM ps_ttm
 from tydw_filesync.HKShareEODDerivativeIndex
 where FINANCIAL_TRADE_DT >= %s and FINANCIAL_TRADE_DT <= %s
 ''' % (begin_date, end_date)
    data = query_pd_infodb(query)
    if len(data) == 0:
        return
    data['date'] = data['date'].astype('datetime64[ns]')
    data = data.sort_values(['date', 'stk_code'])

    # 剔除非交易日数据
    tradingday = tools.get_trading_days(begin_date, end_date, market='hk')
    data = data[data['date'].isin(tradingday)]

    tools_data.save(data, path='stock/derivative_hk', primary_key=[
        'date', 'stk_code'])


def update_industry(begin_date, end_date):
    """
    更新申万A股行业、申万港股行业、中信港股行业。
    该部分每次都需要更新全部历史数据
    """
    lg.info('update_industry...')
    stock_ind = pd.DataFrame(columns=['stk_code', 'ind_code', 'entry_date',
                                      'remove_date', 'ind_name', 'ind_type',
                                      'ind_level', 'area'])
    for level in [1, 2, 3]:
        # ind sw
        query = '''select a.s_info_windcode stk_code, a.SW_IND_CODE ind_code,
     a.ENTRY_DT entry_date, a.REMOVE_DT remove_date,
     b.Industriesname ind_name
     from tydw_filesync.AShareSWNIndustriesClass a,
     tydw_filesync.AShareIndustriesCode b
     where substr(a.SW_IND_CODE, 1, %s) = substr(
     b.IndustriesCode, 1, %s)
     and b.LEVELNUM = %s
     order by 1,3,2
     ''' % ((level + 1) * 2, (level + 1) * 2, level + 1)
        data = query_pd_infodb(query)
        data1 = data.copy()
        data1['ind_code'] = data1['ind_code'].apply(
            lambda x: x[:(level + 1) * 2])
        data1['ind_type'] = 'sw'
        data1['ind_level'] = level
        data1['area'] = 'a'
        stock_ind = stock_ind.append(data1)

        # ind sw hk
        query = '''select a.s_info_windcode stk_code, a.SW_IND_CODE ind_code,
     a.ENTRY_DT entry_date, a.REMOVE_DT remove_date,
     b.Industriesname ind_name
     from tydw_filesync.HKShareSWNIndustriesClass a,
     tydw_filesync.AShareIndustriesCode b
     where substr(a.SW_IND_CODE, 1, %s) = substr(
     b.IndustriesCode, 1, %s)
     and b.LEVELNUM = %s
     order by 1,3
     ''' % ((level + 1) * 2, (level + 1) * 2, level + 1)
        data = query_pd_infodb(query)
        data1 = data.copy()
        data1['ind_code'] = data1['ind_code'].apply(lambda x: x[:(level + 1) * 2])
        data1['ind_type'] = 'sw'
        data1['ind_level'] = level
        data1['area'] = 'hk'
        stock_ind = stock_ind.append(data1)

        # ind ci
        query = '''select a.s_info_windcode stk_code,
     a.CITICS_IND_CODE ind_code, a.ENTRY_DT entry_date,
     a.REMOVE_DT remove_date, b.Industriesname ind_name
     from tydw_filesync.AShareIndustriesClassCITICS a,
     tydw_filesync.AShareIndustriesCode b
     where substr(a.CITICS_IND_CODE, 1, %s) = substr(
     b.IndustriesCode, 1, %s)
     and b.LEVELNUM = %s
     order by 1,3
     ''' % ((level + 1) * 2, (level + 1) * 2, level + 1)
        data = query_pd_infodb(query)
        data1 = data.copy()
        data1['ind_code'] = data1['ind_code'].apply(lambda x: x[:(level + 1) * 2])
        data1['ind_type'] = 'ci'
        data1['ind_level'] = level
        data1['area'] = 'a'
        stock_ind = stock_ind.append(data1)

    stock_ind['entry_date'] = stock_ind['entry_date'].astype('datetime64[ns]')
    stock_ind['remove_date'] = stock_ind['remove_date'].astype(
        'datetime64[ns]')

    # 避免万得数据重复，作去重处理
    # 如834950.BJ这只股票在20230306更新时出现了两个归属一级行业，且纳入日期相同
    stock_ind = stock_ind[~stock_ind[['stk_code', 'ind_code',
                                      'entry_date']].duplicated()]
    tools_data.save(stock_ind, 'stock/stock_ind')

    # 行业代码与名称的对应
    ind_all_name = stock_ind[~stock_ind['ind_code'].duplicated()][[
        'ind_code', 'ind_name', 'ind_type', 'ind_level']]
    tools_data.save(ind_all_name, 'stock/ind_all_name')


def update_consensus_forecast_a(begin_date, end_date):
    """
    更新一致预期数据：A股
    """
    lg.info('update_consensus_forecast_a...')
    query = '''select stock_code stk_code, con_date "date", con_year, con_or,
 con_or_type, con_np, con_np_type, con_eps, con_eps_type,
 con_roe
 from ZYYX_RISK2.con_forecast_stk
 where con_date >= to_date(%s,'yyyymmdd')
 and con_date <= to_date(%s,'yyyymmdd')
 and con_np_type != 3
 ''' % (begin_date, end_date)
    data = query_pd_infodb(query)
    data['stk_code'] = tools_general.unify_stock_code(data['stk_code'])
    tools_data.save(data, 'stock/con_forecast_a', primary_key=[
        'date', 'stk_code', 'con_year'])


def update_consensus_forecast_hk(begin_date, end_date):
    """
    更新一致预期数据：港股
    """
    lg.info('update_consensus_forecast_hk...')
    query = '''select stock_code stk_code, con_date "date",
 con_year, con_cur, con_or, con_or_type, con_np,
 con_np_type, con_eps, con_eps_type
 from zyyx_hk.con_forecast_stk_hk
 where con_date >= to_date(%s,'yyyymmdd')
 and con_date <= to_date(%s,'yyyymmdd')
 and con_np_type not in (3, 4)
 ''' % (begin_date, end_date)
    data = query_pd_infodb(query)
    data['stk_code'] = tools_general.unify_stock_code(data['stk_code'])
    tools_data.save(data, 'stock/con_forecast_hk', primary_key=[
        'date', 'stk_code', 'con_year'])


def update_dividend_a(begin_date, end_date):
    """
    更新分红除权数据: A股
    """
    lg.info('update_dividend_a...')
    query = '''select S_INFO_WINDCODE stk_code, STK_DVD_PER_SH stk_div,
 CASH_DVD_PER_SH_PRE_TAX cash_div, EQY_RECORD_DT record_dt,
 EX_DT ex_dt, REPORT_PERIOD period,
 S_DIV_BASESHARE base_share, TOT_CASH_DVD cash_total
 from tydw_filesync.AShareDividend
 where EX_DT >= %s and EX_DT <= %s''' % (begin_date, end_date)
    data = query_pd_infodb(query)
    data['record_dt'] = data['record_dt'].astype('datetime64[ns]')
    data['ex_dt'] = data['ex_dt'].astype('datetime64[ns]')
    data['period'] = data['period'].astype('datetime64[ns]')
    data = data.sort_values(by=['ex_dt', 'stk_code'])
    tools_data.save(data, 'stock/dividend_a', primary_key=[
        'stk_code', 'record_dt', 'period'])


def update_dividend_hk(begin_date, end_date):
    """
    更新分红除权数据: 港股
    """
    lg.info('update_dividend_hk...')
    query = '''select S_INFO_WINDCODE stk_code, EVENT_TYPE, EX_DATE,
 PAYMENT_DATE, BONUS_SHARE_D_DATE, IN_SPECIE_DATE,
 CASH_DIV_RATIO, BONUS_SHARE_RATIO stk_div, IN_SPECIE_RATIO,
 CRNCY_CODE, RSTART_DATE, REND_DATE, RE_TYPE
 from tydw_filesync.HKShareEvent
 where EX_DATE >= %s and EX_DATE <= %s''' % (begin_date, end_date)
    data = query_pd_infodb(query)
    for field in data.columns:
        if 'date' in field:
            data[field] = data[field].astype('datetime64[ns]')
    data = data.rename({'ex_date': 'ex_dt'}, axis=1)
    data = data.sort_values(by=['ex_dt', 'stk_code'])
    tools_data.save(data, 'stock/dividend_hk', primary_key=[
        'stk_code', 'event_type', 'ex_dt', 'rstart_date', 'rend_date'])


def update_barra(begin_date, end_date):
    """
    更新分红除权数据: 港股
    """
    # ===== barra_factor info
    # lg.info('Download barra factor info...')
    # query = '''select * from barra_data.modelfactorinfo'''
    # data1 = query_pd_infodb(query)
    # tools_data.save(data, 'stock/barra_fac_info')

    # ===== barra_factor CNE5
    lg.info('Download barra_factor...')
    query = '''select TO_CHAR(TDATE, 'YYYYMMDD') AS "date",
 DECODE(SUBSTR(VALUE, 3, 1), '6',
 SUBSTR(VALUE, 3, 6) || '.SH',
 SUBSTR(VALUE, 3, 6) || '.SZ') AS stk_code,
 BETA, MOMENTUM, "SIZE", EARNYILD, RESVOL, GROWTH,
 BTOP, LEVERAGE, LIQUIDTY, SIZENL, INDNAME
 from barra_data.CNE5S_FACTOREXPOSURE d, barra_data.securitymaster m
 where exists (select 1 from barra_data.securitymaster
 where type = 'ROOTID' and m.id = value)
 and m.type = 'LOCALID'
 and m.id = d.id
 and d.tdate between m.startdate and m.enddate
 and d.tdate BETWEEN TO_DATE(%s,'YYYYMMDD') AND TO_DATE(%s,'YYYYMMDD')
 ''' % (begin_date, end_date)
    data = query_pd_infodb(query)
    data['date'] = data['date'].astype('datetime64[ns]')
    data = data.sort_values(['date', 'stk_code'])
    tools_data.save(data, 'stock/barra_factor_cne5', primary_key=[
        'stk_code', 'date'])
