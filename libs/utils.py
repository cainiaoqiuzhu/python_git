# coding: utf-8

from functools import wraps
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from traceback import format_exc

import requests
from flask import jsonify, request

from etc import config
from libs.log import lg

DELAY_DAYS = 7
sensitivity_map = {
    'weight_total': 2,
    'weight_top10': 2,
    'weight_top20': 2,
    'num_ex_new': 2,
    'weight_hk': 2,
    'num_hk': 2,
    'weight_a': 2,
    'num_a': 2,
    'beta': 0,
    'momentum': 0,
    'size': 0,
    'earnyild': 0,
    'resvol': 0,
    'growth': 0,
    'btop': 0,
    'leverage': 0,
    'liquidty': 0,
    'sizenl': 0,
    'mv': 2,
    'mv_log': 2,
    'float_mv': 2,
    'float_mv_log': 2,
    'list_years': 2,
    'list_years_log': 2,
    'mom20': 2,
    'mom60': 2,
    'std20': 2,
    'std60': 2,
    'turn20': 2,
    'turn60': 2,
    'ep_ttm': 2,
    'bp_lf': 2,
    'roe_ttm': 2,
    'roe_fttm': 2,
    'roe_q': 2
}


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


def check_permit(user_token, unit_id_list, begin_date_str, end_date_str, indicator_code_list):
    ''' 检查当前用户是否有对应的资产单元ID数据权限 '''
    user_code = get_user_code(user_token)
    if not user_code:
        return False, '用户鉴权失败'
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
    unit_id_set = set()
    permission_set = {'0': set(), '1READ': set(), '1READ_DELAY': set(), '2READ': set(), '2READ_DELAY': set(), '3READ': set(), '3READ_DELAY': set()}
    for item in result['data']:
        if item['rightType'] != 'SY':
            continue
        if item['rightCode'] not in permission_set.keys():
            continue

        try:
            unit_id = int(item['dataId'].split('-')[-1])
        except Exception as e:
            lg.error('find wrong dataId format: %s\n%s\n%s', item, e, result)
            continue
        unit_id_set.add(unit_id)
        # 得先判断选中的基金是什么权限——只可能是7种的一种
        permission_set[item['rightCode']].add(unit_id)
    if set(unit_id_list) - unit_id_set:
        return True, '请求的资产单元部分没有权限'
    # 在这里要遍历整个字典，看看选中的基金具有什么权限，同时截取所具有权限字符串的第一个值(0,1,2,3)来判断
    # 查看的indicator_code_list敏感度是否符合查看标准，符合就判断是read还是read_delay，执行原来的判断
    # 不符合就返回空数据。这里请帮我写一下

    if set(unit_id_list).issubset(read_set):
    # 如果选中的资产单元，该用户都有read权限
        return {'begin_date': begin_date_str, 'end_date': end_date_str}
    begin_date = datetime.strptime(begin_date_str, '%Y%m%d')
    end_date = datetime.strptime(end_date_str, '%Y%m%d')
    new_end_date = datetime.now() - timedelta(days=DELAY_DAYS)
    if begin_date > new_end_date:
        return False, f'选中的资产单元只有查看{DELAY_DAYS}天前的权限，请重新修改日期范围'
    return {'begin_date': begin_date_str, 'end_date': min(end_date, new_end_date).strftime("%Y%m%d")}


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


def run_task(job_key, func, args, kwargs):
    ''' 异步跑任务，并发送回调接口 '''
    lg.info('%s异步任务开始, job_key %s', func.__name__, job_key)
    try:
        response_data = func(*args, **kwargs)
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
    def inner(*args, **kwargs):
        job_key = request.headers.get('djsp-async-http-jobKey', '')
        task_info = func(*args, **kwargs)
        if 'func' not in task_info:
            # 创建任务失败，可能是参数检查错误，直接返回，不创建异步任务
            task_info['code'] = '0000' if task_info['success'] else '0001'
            return jsonify(task_info)
        pool.submit(run_task, job_key, task_info['func'], task_info.get('args', ()), task_info.get('kwargs', {}))
        return jsonify({'code': '0000', 'success': True, 'message': '已创建异步任务', 'data': []})

    return inner
