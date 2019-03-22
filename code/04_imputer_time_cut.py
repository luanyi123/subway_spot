# -*- coding:utf-8 -*-
# author: zhushuai
# time: 2019-03-21 19:04

# 第四步
# 补全每天中缺失时段的信息，用0填充缺失值

import pandas as pd
import os

basepath = "./train03_22/01"

train_files = [f for f in sorted(os.listdir(basepath)) if f not in [".DS_Store"]]

temp = [[i, j] for i in range(81) for j in range(144)]
# 生成用于连接缺失时间的DataFrame
pad = pd.DataFrame({"stationID": list(zip(*temp))[0], "time_cut": list(zip(*temp))[1]})

scount = 0
fcount = 0

for file in train_files:
    print("正在处理...", file)
    try:
        train_df = pd.read_csv(os.path.join(basepath, file))
        to_merge = train_df.drop(['time_cut', 'inNums', 'outNums'], axis=1).drop_duplicates().reset_index(drop=True)
        train_df = train_df.drop(['weekday', 'is_holiday', 'station_type', 'neighbor_station'], axis=1)
        train_df = pad.merge(train_df, on=['stationID','time_cut'], how='outer')
        train_df.fillna(0, inplace=True)
        train_df = train_df.merge(to_merge, on=['stationID'], how='left')
        train_df.to_csv("./train03_22/02/" + file, index=False)
        scount += 1
        print(file, "处理成功！")
    except:
        fcount += 1
        print(file, "处理失败！")

print("处理完成，成功处理", scount, "个文件，失败", fcount, "个！")