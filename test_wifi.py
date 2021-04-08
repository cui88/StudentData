# -*- coding:utf-8 -*-
import pandas as pd
import os
import sys
import pdb
import time
import datetime
import logging
import multiprocessing
import csv

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler("log.txt")
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

console = logging.StreamHandler()
console.setLevel(logging.INFO)

logger.addHandler(handler)

dataPath = "/home/guolab/pannian/python/data/xuehao-mac/"
userPath = "/home/guolab/pannian/python/user_file/"
rj_location_file = 'rj.xlsx'
h3_location_file = 'h3c.xls'
user_id_file = 'UserMac20201231.csv'
resultPath = "/home/guolab/pannian/python/school_wifi.csv"
ap_mac_path = "/home/guolab/pannian/python/ap_mac.csv"
device_mac_path = "/home/guolab/pannian/python/device_mac.csv"
day = 366


# 1.遍历user_mac——12-31号文件
# 2.去ap文件寻找对应的device_mac信息（可以考虑线程池）
# 3.根据ap_mac匹配统计位置

# 直接把location位置拼接成一个文件
def GetRJFile():
    path = dataPath + rj_location_file
    writer = pd.ExcelFile(path)
    sheet_len = len(writer.sheet_names)
    rj_location_pd = pd.DataFrame()
    # 指定下标读取
    for i in range(0, sheet_len):
        df = pd.read_excel(writer, sheet_name=i)
        df = df[['楼栋', 'AP_MAC']]
        df = df.rename(columns={'AP_MAC': 'AP_Mac', '楼栋': 'Location'})
        df['AP_Mac'] = df['AP_Mac'].str.lower()
        rj_location_pd = pd.concat([df, rj_location_pd])
    rj_location_pd['AP_Mac'] = rj_location_pd['AP_Mac'].str.lower()
    return rj_location_pd


def Geth3File():
    path = dataPath + h3_location_file
    h3_location_pd = pd.read_excel(path)
    h3_location_pd = h3_location_pd[['序列号', '描述']]  # 只提取这两列
    h3_location_pd = h3_location_pd.rename(columns={'序列号': 'AP_Mac', '描述': 'Location'})
    h3_location_pd['AP_Mac'] = h3_location_pd['AP_Mac'].str.lower()
    return h3_location_pd


def InitLastResultDf():
    df = pd.DataFrame()
    df['UserID'] = ''
    df['Duration'] = ''
    df['Mean_Duration'] = ''
    df['Location'] = ''
    return df


def GetAPMessage():
    local_dict = GetLocationDict()
    user_dict = GetUserDict()
    # 匹配数据
    device_mac_df = pd.DataFrame()
    device_mac_df['Device_Mac'] = ''

    ap_mac_df = pd.DataFrame()
    ap_mac_df['AP_Mac'] = ''

    ap_file_month_list = GetFileDirList(dataPath)
    for cur_file in ap_file_month_list:
        path = os.path.join(dataPath, cur_file)
        if os.path.isfile(path):
            ap_file_month_list.remove(cur_file)
    print(ap_file_month_list)

    # ap_file_month_list = ['2020_03']
    i = 0
    l = 0
    for cur_file in ap_file_month_list:
        path = os.path.join(dataPath, cur_file)
        ap_file_list = GetFileDirList(path)
        # print(ap_file_list)
        for ap_file in ap_file_list:
            ap_file_path = os.path.join(path, ap_file)
            ap_pd = pd.read_csv(ap_file_path, na_values='NAN', encoding='utf_8_sig')
            for row in ap_pd.itertuples():
                l += 1
                file_line = getattr(row, 'Index') + 1
                device_mac = getattr(row, 'Device_Mac')
                if device_mac in user_dict:  # 找不到设备号即找不到对应的学号信息，可忽略
                    # 获取学号和Ap_Mac地址，Duration累加
                    ap_mac = getattr(row, 'AP_Mac').lower()
                    if ap_mac in local_dict:
                        i += 1
                        user_id = user_dict[device_mac]
                        location = local_dict[ap_mac]
                        duration = getattr(row, 'Duration')
                        file_path = userPath + user_id + ".csv"
                        if os.path.exists(file_path):
                            user_df = pd.DataFrame()
                            try:
                                user_df = pd.read_csv(file_path, encoding='utf_8_sig')
                            except Exception as e:
                                print(e.args)
                            if not user_df.empty:
                                location_list = user_df['Location'].values.tolist()
                                if location.strip() in location_list:
                                    index = location_list.index(location)
                                    user_df.at[index, 'Duration'] += duration
                                    user_df.to_csv(file_path, index=False, encoding="utf_8_sig")
                                else:  # 没找到位置直接追加
                                    user_df = pd.DataFrame([[user_id, location, duration]],
                                                           columns=['UserID', 'Location', 'Duration'])
                                    user_df.to_csv(file_path, index=False, header=False, mode='a', encoding="utf_8_sig")
                        else:  # 文件不存在，直接创建写入
                            user_df = pd.DataFrame([[user_id, location, duration]],
                                                   columns=['UserID', 'Location', 'Duration'])
                            user_df.to_csv(file_path, index=False, encoding="utf_8_sig")
                        print("Done")
                    else:
                        PrintLoggerInfo(ap_file_path, file_line, ap_mac)
                        ap_mac_df = StockDf(ap_mac_df, ap_mac)
                else:
                    PrintLoggerInfo(ap_file_path, file_line, device_mac)
                    device_mac_df = StockDf(device_mac_df, device_mac)

    print("l = ", l)
    ap_mac_df.to_csv(ap_mac_path, index=False, encoding="utf_8_sig")
    device_mac_df.to_csv(device_mac_path, index=False, encoding="utf_8_sig")


def GetAPMessage():
    fileName = "/home/guolab/pannian/python/5246.csv"
    resultName = "/home/guolab/pannian/python/test_data.csv"

    user_df = pd.read_csv(fileName, na_values='NAN', encoding='utf_8_sig')
    device_list = ["8c:a9:82:02:98:e8", "fc:ab:90:00:ae:f8", "98:5f:d3:e4:01:69", "e4:a7:a0:20:40:f9",
                   "70:1c:e7:fc:54:cc"]
    user_group_df = user_df.loc[user_df.Device_Mac.isin(device_list), :]
    print(user_group_df)
    print(user_group_df['Duration'].sum())
    user_group_df.to_csv(resultName, index=False, encoding="utf_8_sig")


def StockDf(df, name):
    length = len(df) + 1
    df.loc[length] = [name]
    return df


def FilterData():
    file_list = GetFileDirList(userPath)
    result_df = InitLastResultDf()
    for user_file in file_list:
        path = os.path.join(userPath, user_file)
        df = pd.DataFrame()
        try:
            df = pd.read_csv(path, encoding="utf_8_sig")
        except Exception as e:
            print(e.args)
        if not df.empty:
            user_id = user_file[:-4]
            duration = df['Duration'].sum()
            mean_duration = duration / 366
            index = df['Duration'].argmax()
            location = df.at[index, 'Location']
            length = len(result_df) + 1
            result_df.loc[length] = [user_id, duration, mean_duration, location]
        else:
            logger.warning("路径：" + path + ",文件为空!")
    result_df.to_csv(resultPath, index=False, encoding="utf_8_sig")


def GetFileDirList(path):
    file_list = os.listdir(path)
    return file_list


def GetUserDict():
    user_id_path = dataPath + user_id_file
    user_id_pd = pd.read_csv(user_id_path, na_values='NAN', encoding='utf_8_sig')
    csvDict = GetDict(user_id_pd, key_name='Device_Mac', value_name='UserID')
    return csvDict


def GetLocationDict():
    location_df = pd.concat([GetRJFile(), Geth3File()])
    csvDict = GetDict(location_df, key_name='AP_Mac', value_name='Location')
    return csvDict


def GetDict(df, key_name, value_name):
    csvDict = {}
    for row in df.itertuples():
        csvDict[getattr(row, key_name)] = getattr(row, value_name)
    return csvDict


# if __name__ == '__main__':
# 	start=time.time()
#
# 	# local_dict = GetLocationDict()
# 	# name = "7425-8a88-72c0"
# 	# if name.strip() not in local_dict:
# 	# 	print("local_dict不存在")
# 	path = 'data/xuehao-mac/location.xlsx'
# 	lpd = pd.read_excel(path, keep_default_na=False)
# 	for i in range(len(lpd)):
# 		lpd['lat_lon'][i] = lpd['lat_lon'][i][1:-2]
# 	lpd.to_excel('./location.xlsx', index=False, encoding='utf-8')
# 	end = time.time()
# 	time_m = end-start


def queueuein(queue):
    for x in range(1409):
        queue.put(x)
    print('queueuein 结束')


# if __name__ == '__main__':
#
#     queue = multiprocessing.Queue()
#     process = multiprocessing.Process(target=queueuein, args=(queue,))
#
#     process.start()
#     process.join()
#
#     print('queue.qsinze() >>>', queue.qsize())
#     print('close .....')

from multiprocessing import Process
import os


# 子进程要执行的代码
def run_proc(name):
    print('Run child process %s (%s)...' % (name, os.getpid()))


if __name__ == '__main__':
    print('Parent process %s.' % os.getpid())
    p = Process(target=run_proc, args=('test',))
    print('Child process will start.')
    p.start()
    p.join()
    print('Child process end.')
