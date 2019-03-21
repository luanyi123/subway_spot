# -*- coding:utf-8 -*-
# author: zhushuai
# time: 2019-03-21 20:29

# 第五步
# 生成同一站点上下各两个时间段的流量

import numpy as np
import pandas as pd
import os

basepath = "./train03_21/02"

train_files = [f for f in sorted(os.listdir(basepath)) if f not in [".DS_Store"]]

scount, fcount = 0, 0



for file in train_files:
    print("正在处理...", file)
    try:
        train_df = pd.read_csv(os.path.join(basepath, file))
        train_df.rename(columns={'time_cut': 'now_time_cut'}, inplace=True)


        # 生成一个用于中间转换的DataFrame
        train_now = train_df.drop(['weekday', 'is_holiday', 'station_type', 'neighbor_station'], axis=1)

        train_merge = train_now.copy()
        train_merge.rename(columns={'inNums': 'inNums_now', 'outNums': 'outNums_now'}, inplace=True)

        # 考虑前多少个时间段，默认考虑前两个时间段
        for i in range(2, 0, -1):
            train_before = train_now.copy()
            train_before['now_time_cut'] = train_before['now_time_cut'] + i
            train_merge = train_merge.merge(train_before, how='left', on = ["stationID", "now_time_cut"])
            train_merge.rename(columns = {'inNums': f'inNums_before{i}', 'outNums': f'outNums_before{i}'}, inplace=True)
            train_merge.fillna(0, inplace=True)

        # 考虑后多少个时间段，默认考虑前两个时间段
        for j in range(2, 0, -1):
            train_after = train_now.copy()
            train_after['now_time_cut'] = train_after['now_time_cut'] - j
            train_merge = train_merge.merge(train_after, how='left', on = ["stationID", "now_time_cut"])
            train_merge.rename(columns = {'inNums': f'inNums_after{j}', 'outNums': f'outNums_after{j}'}, inplace=True)
            train_merge.fillna(0, inplace=True)

        train_df = train_df.drop(['inNums', 'outNums'], axis=1)
        train_df = train_df.merge(train_merge, how='left', on=['stationID', 'now_time_cut'])

        train_df.to_csv("./train03_21/03/"+file, index=False)
        scount += 1
        print(file, "处理成功！")
    except Exception as e:
        fcount += 1
        print(file, "处理失败！")

print("处理完成，成功处理", scount, "个文件，失败", fcount, "个！")
