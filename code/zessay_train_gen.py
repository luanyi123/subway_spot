# -*- coding: utf-8 -*-
# @Author: Zessay
# @Date:   2019-03-20 21:43:19
# @Last Modified by:   Zessay
# @Last Modified time: 2019-03-20 21:44:35


# 对原始数据集清洗，生成可以用来训练的数据


import pandas as pd


# 导入20190101的文件
train1_df = pd.read_csv(train_0101_path)

# 将时间对应的列转换为pandas可以处理的类型
train1_df['time'] = pd.to_datetime(train1_df['time'])
# 得到对应的周次、小时和分钟
train1_df['weekday'] = train1_df['time'].dt.weekday
train1_df['hour'] = train1_df['time'].dt.hour
train1_df['minute'] = train1_df['time'].dt.minute
# 丢弃暂时不用的列
train1_df = train1_df.drop(['deviceID', 'userID', 'payType'], axis=1)
# 按照车站、小时和分钟的顺序排序
train1_df = train1_df.sort_values(by=['stationID', 'hour', 'minute'])
# 对分钟进行分箱
train1_df['minute_cut'] = pd.cut(train1_df['minute'], bins=list(range(-1,60,10)), labels=list(range(0, 6)))
# 生成用于转换的临时列
train1_df['temp'] = train1_df['hour']*10 + train1_df['minute_cut'].astype('int64')
# 生成用于转换的DataFrame
temp_array = {'temp': [i*10+j for i in range(24) for j in range(6)], 'time_cut': list(range(0, 144))}
temp_cut = pd.DataFrame(data=temp_array)
# 连接转换对应的时间段
train1_merge = train1_df.merge(temp_cut, on='temp', how='left')
# 丢弃接下来不用的列
train1_merge = train1_merge.drop(['temp', 'minute', 'minute_cut', 'hour'], axis=1)

# 基于之前的文件统计不同站点，不同时间段，不同状态下的人数
train1_gen = train1_merge.groupby(['stationID', 'time_cut', 'status']).size().reset_index(name='count').join(train1_merge['weekday'])

# 生成入站的人数统计DataFrame
train1_inNum = train1_gen[train1_gen['status'] == 1].rename(columns={'count':'inNums'}).drop(['weekday','status'], axis=1)

# 生成出站的人数统计DataFrame
train1_outNum = train1_gen[train1_gen['status'] == 0].rename(columns={'count':'outNums'}).drop(['weekday', 'status'], axis=1)

# 生成左连接后的DataFrame
train1_gen = train1_gen.merge(train1_inNum, on=['stationID', 'time_cut'], how='left').merge(train1_outNum, on=['stationID', 'time_cut'], how='left')

# 用0填充缺失值，丢弃不需要的列
train1_gen = train1_gen.fillna(0.0).drop(['count', 'status'], axis=1)
# 生成用于训练的文件
train1_gen.to_csv("data/train_gen/train_01_01.csv", index=False)