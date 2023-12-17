# coding: utf-8
import socket
import os

from apollo.main.apollo_client_call import ApolloClientCall

# 实例化apollo类
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
apollos = ApolloClientCall(os.path.join(BASE_DIR, '../config/apollo_config.yaml'))


def get_ip():
 hostname = socket.gethostname()
 ip_list = socket.gethostbyname_ex(hostname)[-1]
 local_ip = next((ip for ip in ip_list if not ip.startswith("127.")), None)
 return local_ip


eureka_instance_host = get_ip()
gateway_url = apollos.get_value('GATEWAY_URL')

# Eureka配置
eureka_conf = {
 "eureka_server": apollos.get_value('eureka_server'), # eureka_server地址
 "app_name": apollos.get_value('eureka_app_name'), # 自定义名称
 "instance_host": eureka_instance_host, # 当前flask web域名或者外部能访问的ip
 "instance_port": int(apollos.get_value('eureka_instance_port')), # 当前flask web的端口
 "health_check_url": '/api/ims-calculatecenter-equity/actuator/health', # 健康检查地址
}

log_yaml_conf = apollos.get_value('log_yaml_file')

# 工具运行环境，可选值为pkl/db
env = apollos.get_value('env')

# 项目数据库
db_process_conf = {
 'host': apollos.get_value('db_factor_host'),
 'port': apollos.get_value('db_factor_port'),
 'db': apollos.get_value('db_factor_db'),
 'user': apollos.get_value('db_factor_user'),
 'passwd': apollos.get_value('db_factor_passwd'),
}

# INFODB数据库
db_infodb_conf = {
 'host': apollos.get_value('db_infodb_host'),
 'port': apollos.get_value('db_infodb_port'),
 'db': apollos.get_value('db_infodb_db'),
 'user': apollos.get_value('db_infodb_user'),
 'passwd': apollos.get_value('db_infodb_passwd'),
}

# IMS数据库
db_ims_conf = {
 'host': apollos.get_value('db_ims_host'),
 'port': apollos.get_value('db_ims_port'),
 'db': apollos.get_value('db_ims_db'),
 'user': apollos.get_value('db_ims_user'),
 'passwd': apollos.get_value('db_ims_passwd'),
}

# 健康检查
healthcheck_tyreal_db = apollos.get_value("DB-ORACLE-TYREAL-NODE1.DATASOURCE.pyurl").format(
 username=apollos.get_value("db_ims_user"), password=apollos.client.get_value("db_ims_passwd"))
healthcheck_info_db = apollos.get_value("IS0038-INFODB-NODE1.pyurl").format(
 username=apollos.get_value("db_infodb_user"), password=apollos.client.get_value("db_infodb_passwd"))
healthcheck_ims_db = apollos.get_value("IS0042-DB-TDMySQL-ims_pm_monitor.pyurl").format(
 username=apollos.get_value("db_factor_user"), password=apollos.client.get_value("db_factor_passwd"))