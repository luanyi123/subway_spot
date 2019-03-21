# -*- coding:utf-8 -*-
# author: zhushuai
# time: 2019-03-21 14:45


# 第二步
# 判断对应的日期是否是节假日，并给出对应的标记

import pandas as pd
import os

basepath = "./tianchi_metro/train_gen"

train_files = [f for f in sorted(os.listdir(basepath)) if f not in [".DS_Store"]]

# 表示本应该是工作日，却是节假日的情况
w2h = ["01-01", "02-04", "02-05", "02-06", "02-07", "02-08", "02-09", "02-10", "04-05", "05-01", "06-07", "09-13",
      "10-01", "10-02", "10-03", "10-04", "10-05", "10-06", "10-07"]
# 表示本应该是节假日，调休为工作日的情况
h2w = ["02-02", "02-03", "09-29", "10-12"]

for file in train_files:
    train_df = pd.read_csv(os.path.join(basepath, file))

    if file[11:16] in w2h or (file[11:16] not in h2w and train_df.weekday.values[0] in [6, 7]):
        train_df["is_holiday"] = pd.Series([1] * train_df.shape[0])
    else:
        train_df["is_holiday"] = pd.Series([0] * train_df.shape[0])

    train_df.to_csv(os.path.join(basepath, file), index=False)

print("完成！")