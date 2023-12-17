# coding: utf-8

from libs.log import lg
from libs.db_connect import query_pd_process, save_pd_process


def load(path, begin_date=None, end_date=None, unit_id_list=None, column_map=None, pivot_columns=None,
         date_name='date'):
    """
    参数说明
    path: ./data/下的路径+指标代码，其中指标代码以Excel“指标梳理”为准
    column_map: 列映射，key为数据库标准的列名，value为变更后的列名
    pivot_columns: 列表, 默认None，否则必须包含3个字符串，在pivot时将第一个作为
    index，第二个为columns，第三个为values.
    date_name: 若begin_date或end_date非空，则通过该字段筛选日期
    返回data: pandas.DataFrame格式
    """
    sql = f'select * from factor_{path.split("/")[1]}'
    params = None
    if begin_date or end_date:
        sql += f' where {date_name} >= %(begin_date)s and {date_name} <= %(end_date)s'
        params = {'begin_date': begin_date, 'end_date': end_date}
        if unit_id_list:
            # TODO: 这里字符串拼接没有考虑begin_date不存在的情况
            # TODO: 这里params直接用列表，会抛出Python type list cannot be converted
            sql += ' and unit_id in (%s)' % ','.join(map(str, unit_id_list))
            # sql += ' and unit_id in %(unit_id_list)s'
            # params['unit_id_list'] = tuple(unit_id_list)
    data = query_pd_process(sql, params)
    if date_name in data.columns:
        data[date_name] = data[date_name].astype('datetime64[ns]')
    if column_map:
        data = data.rename(columns=column_map)
    if pivot_columns:
        data = data.pivot(index=pivot_columns[0], columns=pivot_columns[1], values=pivot_columns[2])
    data = data.drop(['id'], axis=1, errors='ignore')
    return data


def save(data, path, column_map=None, unstack=False, on_duplicate='replace', primary_key=None):
    """
    保存数据，往数据库新增记录。

    参数说明
    data: pandas.DataFrame格式
    path: ./data/下的路径+指标代码，其中指标代码以Excel“指标梳理”为准
    column_map: 列映射，key为当前data的列名，value为数据库标准的列名
    unstack: 是否需要从二维表解回一维表
    on_duplicate: 遇到唯一键已存在记录时的行为，'ignore'为忽略跳过，'replace'为覆盖原数据
    primary_key: list，主键，若on_duplicate为replace，则不可重复
    """
    if unstack:
        data = data.stack().reset_index()
        data = data.rename(columns={0: 'value'})
    if column_map:
        data = data.rename(columns={v: k for k, v in column_map.items()})
    table_name = f'factor_{path.split("/")[1]}'
    try:
        save_pd_process(data, table_name, on_duplicate)
    except Exception as e:
        lg.error(data)
        raise e
