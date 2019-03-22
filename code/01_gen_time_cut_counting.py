# -*- coding:utf-8 -*-
# author: zhushuai
# time: 2019-03-20 18:48


# 第一步
# 统计每一天各个时段的流量信息，以及当天的一些时间信息

import pandas as pd
import os
import numpy as np
import warnings

warnings.filterwarnings('ignore')

basepath = "../../Projects/metro/data"

train_files = sorted(os.listdir(os.path.join(basepath, 'Metro_train')))

count = 0

for file in train_files:
    # 导入文件
    train_path = os.path.join(basepath, "Metro_train/"+file)
    print("正在处理...", file)
    train_df = pd.read_csv(train_path)

    # 将时间对应的列转换为pandas可以处理的类型
    train_df['time'] = pd.to_datetime(train_df['time'])
    # 得到对应的周次、小时和分钟
    train_df['weekday'] = train_df['time'].dt.weekday + 1
    train_df['hour'] = train_df['time'].dt.hour
    train_df['minute'] = train_df['time'].dt.minute
    # 丢弃暂时不用的列
    train_df = train_df.drop(['deviceID', 'userID', 'payType'], axis=1)
    # 按照车站、小时和分钟的顺序排序
    train_df = train_df.sort_values(by=['stationID', 'hour', 'minute'])
    # 对分钟进行分箱
    train_df['minute_cut'] = pd.cut(train_df['minute'], bins=list(range(-1,60,10)), labels=list(range(0, 6)))
    # 生成用于转换的临时列
    train_df['temp'] = train_df['hour']*10 + train_df['minute_cut'].astype('int64')
    # 生成用于转换的DataFrame
    temp_array = {'temp': [i*10+j for i in range(24) for j in range(6)], 'time_cut': list(range(0, 144))}
    temp_cut = pd.DataFrame(data=temp_array)
    # 连接转换对应的时间段
    train_merge = train_df.merge(temp_cut, on='temp', how='left')
    # 丢弃接下来不用的列
    train_merge = train_merge.drop(['temp', 'minute', 'minute_cut', 'hour'], axis=1)

    train_final = train_merge.groupby(['stationID', 'time_cut'])['status'].agg(
        {'inNums': np.count_nonzero, 'outNums': lambda x: x.count() - np.count_nonzero(x)}).reset_index()
    
    train_final = train_final.join(train_merge["weekday"], how='left')

    # 生成用于训练的文件n
    train_final.to_csv("./train_gen/"+ 'train' + file[6:], index=False)
    count += 1
    print(file, "处理完成！")

print("生成了", count, '个文件！')