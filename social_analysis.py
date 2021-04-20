# -*- coding:utf-8 -*-
from multiprocessing import Queue, JoinableQueue, Process, Manager
import os
import pandas as pd
import time
import datetime
import math
from selenium import webdriver
from PIL import Image

dataPath = "data/xuehao-mac/"
location_file = 'ap_mac_location.xlsx'
user_file_name = 'UserMac20201231.csv'
process_num = 2
latlng_file_name = 'location.xlsx'

# 设定每个用户每天初始活动时间为 10h*60min = 600min
# 按地点访问人数降序排列


def getDict(df, key_name, value_name):
    csvDict = {}
    for row in df.itertuples():
        csvDict[getattr(row, key_name)] = getattr(row, value_name)
    return csvDict


def GetUserDict(path):
    user_id_pd = pd.read_csv(path, na_values='NAN', encoding='utf_8_sig')
    csvDict = getDict(user_id_pd, key_name='Device_Mac', value_name='UserID')
    return csvDict


def getLocationDict():
    path = dataPath + location_file
    location_pd = pd.read_excel(path, keep_default_na=False)
    csvDict = getDict(location_pd, key_name='AP_Mac', value_name='Location')
    return csvDict


def GetTimeData(year_month_dict, up_time, down_time):
    begin = str(up_time)[:10]
    end = str(down_time)[:10]
    begin = datetime.datetime.strptime(begin, '%Y-%m-%d')
    end = datetime.datetime.strptime(end, '%Y-%m-%d')
    for x in range(-1, (end - begin).days + 1):
        day = (begin + datetime.timedelta(days=x)).strftime("%Y_%m%d")
        year_month = (begin + datetime.timedelta(days=x)).strftime("%Y_%m")
        year_month_dict.setdefault(year_month, []).append(day)


def getAddressList():
    path = dataPath + location_file
    location_pd = pd.read_excel(path, keep_default_na=False)
    return location_pd['Location'].unique().tolist()


def account_places_number(startTime, endTime):
    year_month_dict = {}

    user_path = os.path.join(dataPath, user_file_name)

    GetTimeData(year_month_dict, startTime, endTime)

    user_dict = GetUserDict(user_path)
    local_dict = getLocationDict()
    result_dict = {}
    result_list = []
    address_list = getAddressList()
    q = JoinableQueue()
    q2 = Queue()
    # 创建生产者
    p = Process(target=producer, args=(q, year_month_dict))
    p.start()
    # 创建消费者
    c = []
    for i in range(process_num):
        c.append(
            Process(target=consumer, args=(q, user_dict, local_dict, startTime, endTime, q2)))
        c[i].start()

    for i in range(process_num):
        while True:
            if not q2.empty():
                result_list.append(q2.get())
                break
    # 多进程
    print(len(result_list))
    for i in range(process_num):
        print("----%d:准备回收进程" % i)
        c[i].join(timeout=1)
        print("----%d:回收进程已结束" % i)
        if c[i].is_alive():
            c[i].terminate()
            c[i].join()

    # type(i)为datetime
    for address in address_list:
        # type(j)为dict
        for address_dict in result_list:
            if address in address_dict:
                # j[i]为dict
                user_dict = address_dict[address]
                for user_id in user_dict:
                    result_dict.setdefault(address, {}).setdefault(user_id, []).append(user_dict[user_id])
    print('--------------')
    print(len(result_dict))
    print('--------------')
    return result_dict


def consumer(q, user_dict, local_dict, up_time, down_time, q2):
    result_dict = {}
    startDateTime = datetime.datetime.strptime(up_time, '%Y-%m-%d %H:%M:%S')
    endDateTime = datetime.datetime.strptime(down_time, '%Y-%m-%d %H:%M:%S')
    i = 0
    while True:
        if i > 50:
            if q.empty():
                print('end')
                break
        ap_path = q.get()
        ap_file_path = os.path.abspath(os.path.join(dataPath, ap_path))
        if os.path.exists(ap_file_path):
            ap_pd = pd.read_csv(ap_file_path, na_values='NAN', encoding='utf_8_sig')
            if not ap_pd.empty:
                ap_pd['Up_Time'] = pd.to_datetime(ap_pd['Up_Time'])
                ap_pd.sort_values('Up_Time', inplace=True)
                ap_pd = ap_pd.reset_index(drop=True)
                judge_time = ap_pd['Up_Time'][len(ap_pd) - 1]
                if compareDateTime2(judge_time, startDateTime) and compareDateTime2(endDateTime,
                                                                                    judge_time):
                    ap_pd['Down_Time'] = pd.to_datetime(ap_pd['Down_Time'])
                    ap_pd['Location'] = ap_pd['AP_Mac'].map(local_dict)
                    ap_pd = ap_pd[ap_pd['Location'].notna()]
                    # print(ap_pd)
                    if not ap_pd.empty:
                        ap_pd_group = ap_pd.groupby(ap_pd['Location'])
                        for name, group in ap_pd_group:
                            group = group.reset_index(drop=True)
                            for i in range(len(group)):
                                user_up_time = group['Up_Time'][i]
                                if compareDateTime2(user_up_time, startDateTime) and compareDateTime2(endDateTime,
                                                                                              user_up_time):
                                    user_down_time = group['Down_Time'][i]
                                    device_mac = group['Device_Mac'][i]
                                    if device_mac.strip() in user_dict:
                                        user_id = user_dict[device_mac]
                                        result_dict.setdefault(name, {}).setdefault(user_id,[]).append([user_up_time, user_down_time])
        else:
            print("%s文件不存在!" % ap_path)
        # Queue.task_done() 在完成一项工作之后，Queue.task_done()函数向任务已经完成的队列发送一个信号
        q.task_done()
        i += 1
    print('--------')
    print(len(result_dict))
    q2.put(result_dict)


def producer(q, year_month_dict):
    for key in year_month_dict:
        file_list = year_month_dict[key]
        for file in file_list:
            h3_file = os.path.join(key, "h3_log" + file + "_")
            rj_file = os.path.join(key, "rj_log" + file + "_")
            for i in range(0, 23):
                if i < 10:
                    end_num = '0' + str(i)
                else:
                    end_num = str(i)
                h3_file_path = h3_file + end_num + ".csv"
                rj_file_path = rj_file + end_num + ".csv"
                while q.qsize() >= 5:
                    continue;
                q.put(h3_file_path)
                q.put(rj_file_path)
    q.join()


def compareDateTime2(dt1, dt2):
    return True if dt2 <= dt1 else False  # <0则前者较小


if __name__ == '__main__':
    startTime = input("请输入查询的开始时间（格式：2020-11-22 08:00:00）：")
    endTime = input("请输入查询的结束时间（格式：2020-11-23 23:00:00）：")

    start = time.time()
    account_places_number(startTime, endTime)
    end = time.time()
    print('程序执行时间(s)： ', end - start)