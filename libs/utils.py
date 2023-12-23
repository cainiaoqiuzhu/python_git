# coding: utf-8

from functools import wraps
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from traceback import format_exc

import json
import requests
import pandas as pd
from flask import jsonify, request, g

from etc import config
from libs.log import lg
from models.const import DELAY_DAYS, ConfidentialLevel  # PS：此处特例引入了models，需注意


def response(func):
    ''' 处理返回值 '''
    @wraps(func)
    def inner(*largs, **kwargs):

        res = func(*largs, **kwargs)
        if isinstance(res, str):
            # 返回格式：'message'
            return jsonify({'code': 200, 'success': False, 'message': res, 'data': []})
        elif isinstance(res, tuple):
            # 返回格式：True, 'message'
            success, message = res
            return jsonify({'code': 200, 'success': success, 'message': message, 'data': []})
        elif isinstance(res, pd.DataFrame):
            # 返回格式：DataFrame
            data = json.loads(res.to_json(orient='records', date_format='iso'))
            for row in data:
                row['date'] = row['date'].split('T')[0]
            return jsonify({
                'code': 200,
                'success': True,
                'message': '',
                'data': data,
            })
        elif isinstance(res, dict):
            # 返回格式：{'code': 200, 'success': True, 'message': 'message', 'data': [...]}
            return jsonify({
                'code': res.get('code', 200),
                'success': res.get('success', True),
                'message': res.get('message', ''),
                'data': res.get('data', []),
            })
        else:
            raise TypeError('未支持的格式', type(res), res)
    return inner


def auth_args(level, indicator_code_map=None):

    def wrapped_func(func):
        ''' 用户鉴权、权限校验及参数校验 '''

        @wraps(func)
        def inner(*largs, **kwargs):

            args = get_args(request.json)
            if isinstance(args, tuple):
                return args
            indicator_map = get_indicator_map(request.json, indicator_code_map)
            if isinstance(indicator_map, tuple):
                return indicator_map
            if indicator_map is not None:
                args['indicator_map'] = indicator_map
            result = check_permit(request.headers['User-Token'], args, level)
            if isinstance(result, tuple):
                return result
            args.update(result)
            g.args = args
            return func(*largs, **kwargs)

        return inner

    return wrapped_func


def get_args(json, skip_unit=False):
    ''' 获取常规的参数 '''
    begin_date = json.get('begin_date')
    end_date = json.get('end_date')
    if not begin_date or not end_date:
        return False, '日期错误'
    if skip_unit:
        unit_id_list_str = '0'
    else:
        unit_id_list_str = json.get('unit_id_list')
    if not unit_id_list_str:
        return True, '资产单元ID为空'
    try:
        unit_id_list = list(map(int, unit_id_list_str.split(',')))
    except Exception as e:
        return False, f'资产单元ID列表错误: {e}'
    if begin_date > end_date:
        return True, '开始日期大于结束日期'
    if len(unit_id_list) <= 0:
        return True, '资产单元ID为空'
    return {'begin_date': begin_date, 'end_date': end_date, 'unit_id_list': unit_id_list}


def check_permit(user_token, args, level: ConfidentialLevel):
    ''' 检查当前用户是否有对应的资产单元ID数据权限 '''
    user_code = get_user_code(user_token)
    if not user_code:
        return False, '用户鉴权失败'
    right_data = get_user_data_right(user_code, user_token)
    if isinstance(right_data, tuple):
        return right_data

    read_right = f'READ{level.value}'  # 样例：READ3
    read_delay_right = f'READ{level.value}_DELAY'  # 样例：READ3_DELAY
    if level == ConfidentialLevel.Public:
        # 非敏感级别，没有延迟选项
        read_delay_right = read_right

    unit_id_set = set(args['unit_id_list'])
    read_set = set()  # 记录所有读权限的unit_id
    read_full_set = set()  # 记录有非延迟读权限的unit_id
    for item in right_data:
        if item['rightType'] != 'SY':
            continue
        right_code = item['rightCode']
        if right_code not in (read_right, read_delay_right):
            continue

        try:
            # 格式：unit-123
            assert item['dataId'].startswith('unit')
            unit_id = int(item['dataId'].split('-')[-1])
        except Exception as e:
            lg.error('find wrong dataId format: %s %s', item, e)
            continue
        if unit_id not in unit_id_set:
            continue
        read_set.add(unit_id)
        if right_code == read_right:
            read_full_set.add(unit_id)
    if read_set < unit_id_set:
        return True, '请求的资产单元部分没有权限'

    begin_date_str = args['begin_date']
    end_date_str = args['end_date']
    if read_full_set == read_set:
        # 如果选中的资产单元，该用户都有非延迟权限
        return {'begin_date': begin_date_str, 'end_date': end_date_str}
    begin_date = datetime.strptime(begin_date_str, '%Y%m%d')
    end_date = datetime.strptime(end_date_str, '%Y%m%d')
    new_end_date = datetime.now() - timedelta(days=DELAY_DAYS)
    if begin_date > new_end_date:
        return False, f'选中的资产单元只有查看{DELAY_DAYS}个自然日前的权限，请重新修改日期范围'
    return {'begin_date': begin_date_str, 'end_date': min(end_date, new_end_date).strftime("%Y%m%d")}


def get_indicator_map(json, indicator_code_map):
    ''' 从参数获取指标列表 '''
    if indicator_code_map is None:
        return
    indicator_code_list = json.get('indicator_code_list')
    if not isinstance(indicator_code_list, str):
        return False, '指标code非指定格式'
    indicator_map = {}
    for indicator_code in indicator_code_list.split(','):
        name = indicator_code_map.get(indicator_code)
        if not name:
            return False, f'指标code不存在: {indicator_code}'
        indicator_map[indicator_code] = name
    return indicator_map


def get_user_code(user_token):
    ''' 获取当前用户code '''
    url = f'{config.gateway_url}/api/ims-acc/web/user/info/current'
    try:
        result = requests.post(url, json={}, headers={'User-Token': user_token}).json()
    except Exception as e:
        lg.error('get_user_code failed: %s', e)
        return False
    if not result['success'] or not isinstance(result.get('data'), dict):
        lg.error('get_user_code failed json: %s', result)
        return False
    user_code = result['data'].get('userId')
    if not user_code:
        lg.error('get user_code failed: %s', result)
    return user_code.lower()


def get_user_data_right(user_code, user_token):
    ''' 获取当前用户数据权限 '''
    url = f'{config.gateway_url}/api/ims-acc/data/dataRight'
    data = {"dataModel": "IMS-PM-MONITOR", "ownerType": "USER", "ownerId": user_code}
    try:
        result = requests.post(url, json=data, headers={'User-Token': user_token}).json()
    except Exception as e:
        lg.error('check_unit_id_list failed: %s', e)
        return False, '查找数据权限接口请求失败：%s' % e
    if not result['success'] or not isinstance(result.get('data'), list):
        lg.error('check_unit_id_list failed json: %s', result)
        return False, '查找数据权限接口请求错误：%s' % result
    return result['data']


def run_task(job_key, func, largs, kwargs):
    ''' 异步跑任务，并发送回调接口 '''
    lg.info('%s异步任务开始, job_key %s', func.__name__, job_key)
    try:
        response_data = func(*largs, **kwargs)
    except Exception:
        message = f'异步任务执行失败：{format_exc()}'
        lg.error(message)
        response_data = {'code': 400, 'success': False, 'message': message, 'data': []}
    lg.info('异步任务完成, job_key %s', job_key)
    if not job_key:
        lg.info('异步任务没有job_key, 无需回调')
        return
    headers = {'Content-Type': 'application/json; charset=UTF-8'}
    job_response_data = {
        'code': '0000' if response_data['code'] == 200 else '0001',
        'success': response_data['success'],
        'message': response_data['message'],
        'type': 'END_JOB',
        'jobKey': job_key,
    }
    url = f'{config.task_response_url}/autoapi/job/response'
    lg.info('task callback request: %s', url)
    res = requests.post(url, json=job_response_data, headers=headers, verify=False)
    lg.info('task callback response: %s', res.text)


def task_response(func):
    ''' 任务调度平台自动异步回调 '''
    pool = ThreadPoolExecutor(max_workers=8)

    @wraps(func)
    def inner(*largs, **kwargs):
        job_key = request.headers.get('djsp-async-http-jobKey', '')
        task_info = func(*largs, **kwargs)
        if 'func' not in task_info:
            # 创建任务失败，可能是参数检查错误，直接返回，不创建异步任务
            task_info['code'] = '0000' if task_info['success'] else '0001'
            return jsonify(task_info)
        pool.submit(run_task, job_key, task_info['func'], task_info.get('largs', ()), task_info.get('kwargs', {}))
        return jsonify({'code': '0000', 'success': True, 'message': '已创建异步任务', 'data': []})

    return inner
