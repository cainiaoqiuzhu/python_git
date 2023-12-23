# coding: utf-8

from flask import Blueprint, g
import pandas as pd

from libs.utils import response, auth_args
from models import query_factor
from models.const import ConfidentialLevel

factor_basic = bpt = Blueprint('factor_basic', __name__)

INDICATOR_CODE_MAP = {
    'mv': '总市值',
    'mv_log': '对数总市值',
    'float_mv': '流通市值',
    'float_mv_log': '对数流通市值',
    'list_years': '上市时间',
    'list_years_log': '对数上市时间',
    'mom20': 'mom20',
    'mom60': 'mom60',
    'std20': 'std20',
    'std60': 'std60',
    'turn20': 'turn20',
    'turn60': 'turn60',
    'std20': 'std20',
    'std20': 'std20',
    'ep_ttm': 'ep_ttm',
    'bp_lf': 'bp_lf',
    'roe_ttm': 'roe_ttm',
    'roe_fttm': 'roe_fttm',
    'roe_q': 'roe_q',
}


@bpt.route('/main', methods=['GET', 'POST'])
@response
@auth_args(ConfidentialLevel.Middle, indicator_code_map=INDICATOR_CODE_MAP)
def main():
    df_total = None
    for indicator_code, name in g.args['indicator_map'].items():
        df = query_factor.get_factor(indicator_code, g.args['unit_id_list'], g.args['begin_date'], g.args['end_date'])
        if df is None:
            continue
        df = df.drop(['unit_id'], axis=1, errors='ignore')
        df.columns = ['date', 'value', 'value_bm']
        df['name'] = name
        if df_total is None:
            df_total = df
        else:
            df_total = pd.concat([df_total, df])

    return df_total if df_total is not None else {}
