# -*- coding: utf-8 -*-
"""
Created on Thu Sep 7 15:41:05 2023

更新一些基础数据

@author: zhangshuai
"""

import pandas as pd

from libs.db_connect import query_pd_infodb
from libs import tools_data
from libs.log import lg


def update_calender(begin_date, end_date):
    """
    更新交易日数据
    """
    lg.info('update_calender...')
    # A股交易日
    query = """select TRADE_DAYS trading_day, S_INFO_EXCHMARKET market
 from tydw_filesync.AShareCalendar
 where TRADE_DAYS >= %s and TRADE_DAYS <= %s
 order by 2, 1""" % (begin_date, end_date)
    data = query_pd_infodb(query)
    data['trading_day'] = data['trading_day'].astype('datetime64[ns]')

    # 港股交易日
    query = '''select TRADE_DAYS trading_day, S_INFO_EXCHMARKET market
 from tydw_filesync.HKEXCalendar
 where S_INFO_EXCHMARKET = 'HKEX'
 and TRADE_DAYS >= %s and TRADE_DAYS <= %s
 order by 1
 ''' % (begin_date, end_date)
    data_hk = query_pd_infodb(query)
    data_hk['trading_day'] = data_hk['trading_day'].astype('datetime64[ns]')

    data = pd.concat([data, data_hk]) # 合并A股和港股交易日数据
    tools_data.save(data, path='basic/basic_trading_day')


def update_stock_description():
    """
    下载A股、港股的股票基础信息。
    该函数不用设置起始日期和截止日期，历史记录会被新数据覆盖
    """
    lg.info('update_stock_description...')
    # AShareDescription
    query = '''select a.S_INFO_WINDCODE stk_code, a.S_INFO_NAME name,
 a.S_INFO_LISTDATE list_date,
 a.S_INFO_DELISTDATE delist_date,
 a.S_INFO_COMPCODE company_code, b.S_IPO_PRICE ipo_price
 from tydw_filesync.AShareDescription a left join
 tydw_filesync.AShareIPO b on
 a.S_INFO_WINDCODE = b.S_INFO_WINDCODE
 where a.S_INFO_WINDCODE not like 'A%'
 order by 1'''
    data = query_pd_infodb(query)
    data['list_date'] = data['list_date'].astype('datetime64[ns]')
    data['delist_date'] = data['delist_date'].astype('datetime64[ns]')
    tools_data.save(data, path='basic/basic_stock_a')

    # HKShareDescription
    query = '''select S_INFO_WINDCODE stk_code, S_INFO_NAME name,
 S_INFO_NAME_ENG name_eng, S_INFO_LISTBOARD list_board,
 S_INFO_STATUS status, CRNCY_CODE,
 S_INFO_LISTDATE list_date, S_INFO_DELISTDATE delist_date,
 S_INFO_LISTPRICE list_price, IS_HKSC,
 ISTEMPORARYSYMBOL is_temporary_symbol,
 IS_H, S_INFO_COMPCODE company_code
 from tydw_filesync.HKShareDescription
 where SECURITYSUBCLASS = 100001001
 order by 1'''
    data = query_pd_infodb(query)
    data['list_date'] = data['list_date'].astype('datetime64[ns]')
    data['delist_date'] = data['delist_date'].astype('datetime64[ns]')
    tools_data.save(data, path='basic/basic_stock_hk')
