# -*- coding:utf-8 -*-
from multiprocessing import Queue, JoinableQueue, Process, Manager
import os
import pandas as pd
import time
import datetime
import math
from selenium import webdriver
from PIL import Image

dataPath = "F:/guolab/python/data/xuehao-mac/"
location_file = 'ap_mac_location.xlsx'
user_file_name = 'UserMac20201231.csv'
process_num = 5
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


def account_places_number(startTime, endTime, address_places_list):
    year_month_dict = {}

    user_path = os.path.join(dataPath, user_file_name)

    GetTimeData(year_month_dict, startTime, endTime)

    user_dict = GetUserDict(user_path)
    local_dict = getLocationDict()
    result_dict = {}
    result_list = []
    address_list = getAddressList()
    # q = JoinableQueue()
    q = Queue()
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
    address_flag = False
    address_number_dict = {}
    for address in address_list:
        # type(j)为dict
        for address_dict in result_list:
            if address in address_dict:
                address_flag = True
                # j[i]为dict
                user_dict = address_dict[address]
                for user_id in user_dict:
                    for time_stamp in user_dict[user_id]:
                        result_dict.setdefault(address, {}).setdefault(user_id, []).append(time_stamp)
        if address_flag:
            # sorted_user_time(result_dict, address)
            address_number_dict[address] = len(result_dict[address])
            address_flag = False
    address_places_list = sorted(address_number_dict.items(), key=lambda x: x[1], reverse=False)
    print(address_places_list)
    print('--------------')
    print(len(result_dict))
    print('--------------')
    return result_dict


def consumer(q, user_dict, local_dict, up_time, down_time, q2):
    result_dict = {}
    startDateTime = datetime.datetime.strptime(up_time, '%Y-%m-%d %H:%M:%S')
    endDateTime = datetime.datetime.strptime(down_time, '%Y-%m-%d %H:%M:%S')
    while True:
        # print("q.size:%d"%q.qsize())
        if q.empty():
            # print("end")
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
        # q.task_done()
    print('--------')
    print("进程结果长度：%d"%len(result_dict))
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
                # while q.qsize() >= 5:
                #     continue;
                q.put(h3_file_path)
                q.put(rj_file_path)
    # q.join()


def sorted_user_time(user_dict):
    for k, v in user_dict.items():
        time_list = sorted(v, key=lambda x: x[0].timestamp(), reverse=False)
        # 合并连续时间
        if len(time_list) > 1:
            first_stamp = True
            overlap = False
            result = []
            t1 = []
            time_list_len = len(time_list) - 1
            for i, time in enumerate(time_list):
                if first_stamp:
                    t1 = time
                    first_stamp = False
                    continue
                # 如果时间戳相差阈值小于30分钟，则也视为时间连续
                if t1[1] >= time[0] or ((time[0] - t1[1]).total_seconds() <= 1800):
                    overlap = True
                    t1 = [min(t1[0], time[0]), max(t1[1], time[1])]
                else:
                    result.append(t1)
                    t1 = time
                if i == time_list_len:
                    result.append(t1)
            if overlap:
                time_list = result
        user_dict[k] = time_list


def compareDateTime2(dt1, dt2):
    return True if dt2 <= dt1 else False  # <0则前者较小


def compare_trajectory(address_places_list, places_dict):
    q = Queue()
    q2 = Queue()
    share_var_user = Manager().dict()
    # 创建生产者
    p = Process(target=trajectory_producer, args=(q, places_dict, address_places_list))
    p.start()
    # 创建消费者
    c = []
    for i in range(process_num):
        c.append(
            Process(target=trajectory_consumer, args=(q, share_var_user)))
        c[i].start()

    # 多进程
    for i in range(process_num):
        print("----%d:准备回收进程" % i)
        c[i].join(timeout=1)
        print("----%d:回收进程已结束" % i)
        if c[i].is_alive():
            c[i].terminate()
            c[i].join()
    return


def trajectory_producer(q, places_dict, address_places_list):
    for addrees in address_places_list:
        q.put(places_dict[addrees])


def trajectory_consumer(q, share_user_pair, share_var_user):
    # 记录每个地点中用户的关联时间(不重复)，key:user  value:(list)[[t1,t2],..]
    # value中的list第一个元素value为剩余可关联时间，默认为600min,
    def compare_user_time(user_name_one, user_time_dict, t):
        if user_name_one in user_time_dict:
            user_time_one = user_time_dict[user_name_one]
            for num in range(1, len(user_time_one)):
                user_time = user_time_one[num]
                if user_time[0] >= t[1]:
                    break
                elif user_time[0] < t[1] <= user_time[1]:
                    if t[0] < user_time[0]:
                        user_time_one[num] = [min(user_time[0], t[0]), user_time[1]]
                        user_time_one[0] += (user_time[0] - t[0]).total_seconds()
                        if user_time_one[0] >= 24000:
                            share_var_user.append(user_name_one)
                    break
                else:
                    if t[0] < user_time[1]:
                        if user_time[0] > t[0]:
                            user_time_one[num] = [t[0], user_time[1]]
                            user_time_one[0] += (user_time[0] - t[0]).total_seconds()
                            if user_time_one[0] >= 24000:
                                share_var_user.append(user_name_one)
                                break
                        t = [user_time[1], t[1]]
            user_time_dict[user_name_one] = user_time_one
        else:
            user_time_one = [time_stamp, t]
            user_time_dict[user_name_one] = user_time_one


    while True:
        if q.empty():
            # print("end")
            break
        # user_dict：key 用户, value 时间段
        user_dict = q.get()
        sorted_user_time(user_dict)
        user_list = list(user_dict)
        user_time_dict = {}
        # 循环比较学生行为轨迹时间段
        for i in range(len(user_list)-1):
            user_name_one = user_list[i]
            if user_name_one in share_var_user:
                break
            for j in range(i+1, len(user_list)):
                associated_time = 0
                user_flag = False
                user_name_two = user_list[j]
                if user_name_two in share_var_user:
                    continue
                user_pair1 = (user_name_one, user_name_two)
                user_pair2 = (user_name_two, user_name_one)
                if user_pair1 in share_user_pair:
                    user_pair = user_pair1
                    # 全局用户关联对的关联时间大于400min时,则忽略不再计算这对用户
                    if share_user_pair[user_pair] > 24000:
                        break
                elif user_pair2 in share_user_pair:
                    user_pair = user_pair2
                    if share_user_pair[user_pair] > 24000:
                        break
                else:
                    user_flag = True
                    user_pair = user_pair1
                time_one_list = user_dict[user_name_one]
                time_two_list = user_dict[user_name_two]
                l = k = 0
                time_one_length = len(time_one_list)
                time_two_length = len(time_two_list)
                while True:
                    if l < time_one_length or k < time_two_length:
                        break
                    time1 = time_one_list[l]
                    time2 = time_two_list[k]
                    if time2[1] <= time1[0]:
                        l += 1
                        continue
                    elif time2[0] >= time1[1]:
                        k += 1
                        continue
                    elif time2[1] >= time1[1]:
                        l += 1
                    else:
                        k += 1
                    time_min = max(time2[0], time1[0])
                    time_max = min(time2[1], time1[1])
                    time_stamp = (time_max - time_min).total_seconds()
                    associated_time += time_stamp
                    t = [time_max, time_min]
                    if time_stamp >= 24000:
                        share_var_user.append(user_name_one)
                        share_var_user.append(user_name_two)
                    else:
                        compare_user_time(user_name_one, user_time_dict, t, time_stamp)
                        compare_user_time(user_name_two, user_time_dict, t, time_stamp)
                if associated_time > 0:
                    if user_flag:
                        share_user_pair[user_pair] = associated_time
                    else:
                        share_user_pair[user_pair] += associated_time


if __name__ == '__main__':
    startTime = input("请输入查询的开始时间（格式：2020-11-22 08:00:00）：")
    endTime = input("请输入查询的结束时间（格式：2020-11-22 11:00:00）：")

    start = time.time()
    address_places_list = []
    places_dict = account_places_number(startTime, endTime, address_places_list)
    # compare_trajectory(address_places_list, places_dict)
    end = time.time()
    print('程序执行时间(s)： ', end - start)