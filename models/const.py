from enum import Enum

#  查看延迟的天数
DELAY_DAYS = 20


# 数据敏感级别
class ConfidentialLevel(Enum):
    Public = 0
    Low = 1
    Middle = 2
    High = 3
