from datetime import datetime

date_str = "20220101"
date_obj = datetime.strptime(date_str, "%Y%m%d")
print(date_obj)