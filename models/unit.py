def check_permit(user_token, unit_id_list):
    ''' 检查当前用户是否有对应的资产单元ID数据权限 '''
    user_code = get_user_code(user_token)
    if not user_code:
        return '用户鉴权失败'
    url = f'{config.gateway_url}/api/ims-acc/data/dataRight'
    data = {"dataModel": "IMS-PM-MONITOR", "ownerType": "USER", "ownerId": user_code}
    try:
        result = requests.post(url, json=data, headers={'User-Token': user_token}).json()
    except Exception as e:
        lg.error('check_unit_id_list failed: %s', e)
        return '查找数据权限接口请求失败：%s' % e
    if not result['success'] or not isinstance(result.get('data'), list):
        lg.error('check_unit_id_list failed json: %s', result)
        return '查找数据权限接口请求错误：%s' % result
    unit_id_set = set()
    for item in result['data']:
        if item['rightType'] != 'SY':
            continue
        if item['rightCode'] != 'READ':
            continue
        try:
            unit_id = int(item['dataId'].split('-')[-1])
        except Exception as e:
            lg.error('find wrong dataId format: %s\n%s\n%s', item, e, result)
            continue
        unit_id_set.add(unit_id)
    if set(unit_id_list) - unit_id_set:
        return '请求的资产单元部分没有权限'
    return None
