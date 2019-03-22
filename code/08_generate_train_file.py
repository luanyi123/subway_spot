# -*- coding:utf-8 -*-
# author: zhushuai
# time: 2019-03-22 20:39


import pandas as pd
import os


basepath = "./train03_22/03"

train_files = [f for f in sorted(os.listdir(basepath)) if f not in [".DS_Store"]]
files_num = len(train_files)

scount, fcount = 0, 0

train_df = pd.DataFrame()

for i in range(files_num - 1):
    print("正在处理...", train_files[i], train_files[i+1])
    try:
        train_fe = pd.read_csv(os.path.join(basepath, train_files[i]))
        train_ta = pd.read_csv(os.path.join(basepath, train_files[i+1]))
        # 只取目标文件中要用的列
        train_ta = train_ta[['stationID', 'now_time_cut', 'weekday', 'is_holiday', 'inNums_now', 'outNums_now']]
        # 对列名进行修改
        train_fe.rename(columns={'weekday': 'yesterday', 'is_holiday': 'yesterday_is_holiday'},
                            inplace=True)
        train_ta.rename(columns={'weekday': 'today', 'is_holiday': 'today_is_holiday',
                                     'inNums_now': 'inNums', 'outNums_now': 'outNums'},
                            inplace=True)

        train_temp = train_fe.merge(train_ta, on=['stationID', 'now_time_cut'], how='left')

        if train_df.empty:
            train_df = train_temp
        else:
            train_df = pd.concat([train_df, train_temp], axis=0, sort=False)

        scount += 1
        print("第", i+1, "次处理成功！")
    except Exception as e:
        fcount += 1
        print(e)
        print(train_files[i], train_files[i+1], "处理失败！")


print("处理完成！成功处理了", scount, "个文件，失败了", fcount, "个！")
train_df.to_csv("./train03_22/metro_train.csv", index=False)