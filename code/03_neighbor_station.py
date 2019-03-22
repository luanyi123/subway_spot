# -*- coding:utf-8 -*-
# author: zhushuai
# time: 2019-03-21 16:29


# 第三步
# 生成文件包含邻居站点的信息，包括邻居站点的数量和具体站点

import pandas as pd
import numpy as np
import os

# 上一步生成好的文件
basepath = "./train_gen"
# 地铁网图文件
mappath = "../../Projects/metro/data/Metro_roadMap.csv"
# 对地铁网图进行处理，生成hashmap，键对应站点，值对应邻接点
mapdata = pd.read_csv(mappath)
columns = [mapdata.columns[i] for i in range(1, len(mapdata.columns))]
roadmap = dict()
for index in columns:
    roadmap[int(index)] = np.where(mapdata[index] == 1)[0]

# 生成用于连接的站点DataFrame
stationID = []
neighbor_station = []
for key, value in roadmap.items():
    stationID.append(key)
    neighbor_station.append(value)

station_type = pd.DataFrame(data={"stationID": stationID,
                                  "station_type": [arr.shape[0] for arr in neighbor_station],
                                 "neighbor_station": [arr for arr in neighbor_station]}
                           )

train_files = [f for f in sorted(os.listdir(basepath)) if f not in [".DS_Store"]]
# 记录处理成功和失败的文件数
scount = 0
fcount = 0

for file in train_files:
    print("正在处理...", file)
    try:
        train_df = pd.read_csv(os.path.join(basepath, file))
        train_df = train_df.merge(station_type, on='stationID', how='left')
        train_df.to_csv("./train03_22/01/" + file, index=False)
        scount += 1
        print(file, "处理成功！")
    except:
        fcount += 1
        print(file, "处理失败！")

print("全部处理完成，成功处理了", scount, "个文件，失败了", fcount, "个！")