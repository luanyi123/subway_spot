# -*- coding:utf-8 -*-
# author: zhushuai
# time: 2019-03-22 17:53

# 第七步
# 拼接邻站点的相邻时刻的流量

import pandas as pd
import os


basepath = "./train03_22/02"

train_files = [f for f in sorted(os.listdir(basepath)) if f not in [".DS_Store"]]


scount, fcount = 0, 0

for file in train_files:
    print("正在处理...", file)
    try:
        train_df = pd.read_csv(os.path.join(basepath, file))

        train_temp = train_df.copy()
        train_temp.drop(['weekday', 'is_holiday',
                         'station_type', 'inNums_before2', 'outNums_before2', 'inNums_after2',
                         'outNums_after2', 'ns_1', 'ns_2', 'ns_3', 'ns_4'], axis=1, inplace=True)

        for i in range(1, 5):
            train_li = train_temp.copy()
            train_li.rename(columns={'stationID': f'ns_{i}', 'inNums_now': f'ns_{i}_inNums_now',
                                        'outNums_now': f'ns_{i}_outNums_now', 'inNums_before1': f'ns_{i}_inNums_before1',
                                        'outNums_before1': f'ns_{i}_outNums_before1',
                                        'inNums_after1': f'ns_{i}_inNums_after1',
                                        'outNums_after1': f'ns_{i}_outNums_after1'}, inplace=True)
            train_df = train_df.merge(train_li, on=[f'ns_{i}', 'now_time_cut'], how='left')

        train_df.to_csv("./train03_22/03/" + file, index=False)

        scount += 1
        print(file, "处理成功！")
    except Exception as e:
        fcount += 1
        # print(e)
        print(file, "处理失败！")

print("处理完成，成功处理", scount, "个文件，失败", fcount, "个！")
