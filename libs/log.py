import logging
import logging.config
import yaml

from etc import config


def get_logger():
    log_yaml_conf = yaml.full_load(config.log_yaml_conf)
    logging.config.dictConfig(log_yaml_conf)
    return logging.getLogger('main')


lg = get_logger()