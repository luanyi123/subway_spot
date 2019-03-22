# -*- coding:utf-8 -*-
# author: zhushuai
# time: 2019-03-22 16:27

# 第六步
# 将邻站点信息单独提取出来

import numpy as np
import pandas as pd
import os

basepath = "./train03_21/03"

train_files = [f for f in sorted(os.listdir(basepath)) if f not in [".DS_Store"]]

scount, fcount = 0, 0

for file in train_files:
    print("正在处理...", file)
    try:
        train_df = pd.read_csv(os.path.join(basepath, file))

        # 对第54站的缺失值进行填充
        train_df['neighbor_station'].fillna("[53 55]", inplace=True)
        train_df['station_type'].fillna(2.0, inplace=True)
        train_df['weekday'].fillna(train_df.weekday.values[0], inplace=True)
        train_df['is_holiday'].fillna(train_df.is_holiday.values[0], inplace=True)

        # 将邻站点转换为列表型，便于处理
        train_df['neighbor_station'] = train_df['neighbor_station'].apply(lambda x: x[1:-1].split())

        # 增加4个新列，保存分割后邻站信息
        for i in range(1, 5):
            train_df[f'ns_{i}'] = np.nan

        # 用前面分割好的邻站点填充每一列
        for i in range(1,5):
            for j in range(train_df.shape[0]):
                try:
                    train_df.loc[j, f'ns_{i}'] = int(train_df.loc[j, 'neighbor_station'][i-1])
                except Exception as e1:
                    pass

        train_df.drop(['neighbor_station'], axis=1, inplace=True)

        train_df.to_csv("./train03_22/02/" + file, index=False)

        scount += 1
        print(file, "处理成功！")
    except Exception as e2:
        fcount += 1
        #print(e2)
        print(file, "处理失败！")

print("处理完成，成功处理", scount, "个文件，失败", fcount, "个！")