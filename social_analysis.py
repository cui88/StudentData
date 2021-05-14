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
result = 'F:/guolab/python/data/result/'
location_file = 'ap_mac_location.xlsx'
user_file_name = 'UserMac20201231.csv'
process_num = 4
process_num_2 = 4
latlng_file_name = 'location.xlsx'
edge = 'edge.csv'
node = 'node.csv'

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
    # print(id(address_places_list))
    year_month_dict = {}

    user_path = os.path.join(dataPath, user_file_name)

    GetTimeData(year_month_dict, startTime, endTime)
    # print(year_month_dict)
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
    user_test_dict = {}
    for address in address_list:
        # type(j)为dict
        for address_dict in result_list:
            if address in address_dict:
                address_flag = True
                # j[i]为dict
                user_dict = address_dict[address]
                for user_id in user_dict:
                    for time_stamp in user_dict[user_id]:
                        #  result_dict.setdefault(address, {}).setdefault(user_id, []).append(time_stamp)
                        # if user_id == '201710040408' or user_id == '201920030118':
                            # print(user_id)
                        dict = {}
                        if address in result_dict:
                            dict = result_dict[address]
                        dict.setdefault(user_id, []).append(time_stamp)
                        result_dict[address] = dict
        if address_flag:
            # sorted_user_time(result_dict, address)
            address_number_dict[address] = len(result_dict[address])
            address_flag = False
    # for address in result_dict:
    #     address_number_dict[address] = len(result_dict[address])
    address_places_list = sorted(address_number_dict.items(), key=lambda x: x[1], reverse=False).copy()
    print('--------------')
    print(len(result_dict))
    # print(result_dict)
    print('--------------')
    # for user in result_dict:
    #     dict = result_dict[user]
    #     for id in dict:
    #         print(len(dict[id]))
    # if '物理学院A栋-2-228' in result_dict:
    #     dict = result_dict['物理学院A栋-2-228']
    #     for user in dict:
    #         print(user +"---%d"%len(dict[user]))
    #         print(dict[user])
    # print(user_test_dict)
    # for user in user_test_dict:
    #     print(user+"长度：")
    #     print(len(user_test_dict[user]))
    #     print(user_test_dict[user])
    return (result_dict, address_places_list)


def consumer(q, user_dict, local_dict, up_time, down_time, q2):
    result_dict = {}
    startDateTime = datetime.datetime.strptime(up_time, '%Y-%m-%d %H:%M:%S')
    endDateTime = datetime.datetime.strptime(down_time, '%Y-%m-%d %H:%M:%S')
    while True:
        # print("q.size:%d"%q.qsize())
        time.sleep(0.00000000000001)
        if q.qsize() == 0:
            print("end")
            break
        ap_path = q.get()
        # print("%d--取元素！"%os.getpid())
        # print("%d--%s取元素！" % (os.getpid(), ap_path))
        ap_file_path = os.path.abspath(os.path.join(dataPath, ap_path))

        if os.path.exists(ap_file_path):
            ap_pd = pd.read_csv(ap_file_path, na_values='NAN', encoding='utf_8_sig')
            if not ap_pd.empty:
                ap_pd['Up_Time'] = pd.to_datetime(ap_pd['Up_Time'])
                ap_pd.sort_values('Up_Time', inplace=True)
                ap_pd = ap_pd.reset_index(drop=True)
                # 文件最大时间
                judge_time = ap_pd['Up_Time'][len(ap_pd) - 1]
                # 文件最小时间
                judge_time2 = ap_pd['Up_Time'][0]
                if not (compareDateTime2(startDateTime,  judge_time) or compareDateTime2(judge_time2, endDateTime)):
                    # print('%d------%s'%(os.getpid(),ap_path))
                    ap_pd['Down_Time'] = pd.to_datetime(ap_pd['Down_Time'])
                    ap_pd['AP_Mac'] = ap_pd['AP_Mac'].str.lower()
                    ap_pd['Location'] = ap_pd['AP_Mac'].map(local_dict)
                    ap_pd = ap_pd[ap_pd['Location'].notna()]
                    # print(ap_pd)
                    if not (ap_pd.empty or len(ap_pd) == 1):
                        ap_pd_group = ap_pd.groupby(ap_pd['Location'])
                        # print(len(ap_pd_group))
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
                                        user_dict_new = {}
                                        if name in result_dict:
                                            user_dict_new = result_dict[name]
                                        user_dict_new.setdefault(user_id, []).append([user_up_time, user_down_time])
                                        # result_dict.setdefault(name, {}).setdefault(user_id, []).append(
                                            # [user_up_time, user_down_time])
                                        result_dict[name] = user_dict_new
                    else:
                        print("ap_pd为空！")
        else:
            print("%d--%s文件不存在!" %(os.getpid(), ap_path))
        # Queue.task_done() 在完成一项工作之后，Queue.task_done()函数向任务已经完成的队列发送一个信号
        # q.task_done()
    print('--------')
    print("%d进程结果长度：%d"%(os.getpid(), len(result_dict)))
    q2.put(result_dict)
    # print(result_dict)


def producer(q, year_month_dict):
    for key in year_month_dict:
        file_list = year_month_dict[key]
        for file in file_list:
            h3_file = os.path.join(key, "h3_log" + file + "_")
            rj_file = os.path.join(key, "rj_log" + file + "_")
            for i in range(0, 24):
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
                if t1[1] >= time[0] or (0 < (time[0] - t1[1]).total_seconds() <= 1800):
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


#dt1文件最大的上线时间,dt2上线时间
def compareDateTime2(dt1, dt2):
    # return True if dt2 <= dt1 else False  # <0则前者较小
    return True if (dt1 - dt2).total_seconds() >= 0 else False


def compare_trajectory(address_places_list, places_dict):
    q_t = Queue()
    q2_t = Queue()
    share_var_user = Manager().list()
    share_user_pair = Manager().dict()
    # 创建生产者
    p = Process(target=trajectory_producer, args=(q_t, places_dict, address_places_list))
    p.start()
    # 创建消费者
    c = []
    for i in range(process_num_2):
        c.append(
            Process(target=trajectory_consumer, args=(q_t, share_user_pair, share_var_user, q2_t)))
        c[i].start()
    p.join()

    while True:
        if q2_t.qsize() == process_num_2:
            break
    # 多进程
    for i in range(process_num_2):
        print("----%d:准备回收进程" % i)
        c[i].join(timeout=1)
        # c[i].join()
        print("----%d:回收进程已结束" % i)
        if c[i].is_alive():
            c[i].terminate()
            c[i].join()
    print('----share_var_user----')
    print(share_var_user)
    print("share_var_user长度：%d"%len(share_var_user))
    # print(share_user_pair)
    str = ('201710040408','201920030118')
    str2 = ('201920030118','201710040408')
    if str in share_user_pair:
        print(share_user_pair[str])
    else:
        print("str1不存在")
    if str2 in share_user_pair:
        print("2222")
        print(share_user_pair[str2])
    else:
        print("str2不存在")
    if '201710040408' in share_var_user:
        print('201710040408  存在')
    if '201920030118' in share_var_user:
        print('201920030118  存在')
    return share_user_pair


def trajectory_producer(q, places_dict, address_places_list):
    for addrees_pair in address_places_list:
        addrees = addrees_pair[0]
        q.put(places_dict[addrees])
    # test代码
    # def sort_list(list1):
    #     list2 = []
    #     for str in list1:
    #         list2.append([datetime.datetime.strptime(str[0], '%Y-%m-%d %H:%M:%S'),datetime.datetime.strptime(str[1], '%Y-%m-%d %H:%M:%S')])
    #     return list2
    # list1 = [['2020-11-28 09:00:40', '2020-11-28 09:23:40'], ['2020-11-28 10:00:20', '2020-11-28 10:00:40'], ['2020-11-28 10:00:40', '2020-11-28 10:38:40']]
    # list1 = sort_list(list1)
    # list2 = [['2020-11-28 09:00:10', '2020-11-28 09:23:20'], ['2020-11-28 09:23:20', '2020-11-28 10:00:52'], ['2020-11-28 10:00:52', '2020-11-28 10:38:40']]
    # list2 = sort_list(list2)
    # list3 = [['2020-11-28 09:00:15', '2020-11-28 09:22:20'], ['2020-11-28 09:23:20', '2020-11-28 10:00:55'], ['2020-11-28 10:00:58', '2020-11-28 10:39:40']]
    # list3 = sort_list(list3)
    # user_dict1 = {'S200200213': list1, '202093010102': list2, 'S2001W0057': list3}
    # user_dict2 = {'S200200213': list1, '202093010102': list2, 'S2001W0057': list3}
    # user_dict1['201920030118'] = sort_list(user_dict1['201920030118'])
    # user_dict1['201710040408'] = sort_list(user_dict1['201710040408'])
    # q.put(user_dict1)
    # q.put(user_dict2)

    # test代码
    # def sort_list(list1):
    #     list2 = []
    #     for str in list1:
    #         list2.append([datetime.datetime.strptime(str[0], '%Y-%m-%d %H:%M:%S'),datetime.datetime.strptime(str[1], '%Y-%m-%d %H:%M:%S')])
    #     return list2
    # list1 = [['2020-11-28 09:00:40', '2020-11-28 09:23:40'], ['2020-11-28 10:00:20', '2020-11-28 10:00:40'], ['2020-11-28 10:00:40', '2020-11-28 10:38:40']]
    # list1 = sort_list(list1)
    # list2 = [['2020-11-28 09:00:10', '2020-11-28 09:23:20'], ['2020-11-28 09:23:20', '2020-11-28 10:00:52'], ['2020-11-28 10:00:52', '2020-11-28 10:38:40']]
    # list2 = sort_list(list2)
    # list3 = [['2020-11-28 09:00:15', '2020-11-28 09:22:20'], ['2020-11-28 09:23:20', '2020-11-28 10:00:55'], ['2020-11-28 10:00:58', '2020-11-28 10:39:40']]
    # list3 = sort_list(list3)
    # user_dict1 = {'S200200213': list1, '202093010102': list2, 'S2001W0057': list3}
    # user_dict2 = {'S200200213': list1, '202093010102': list2, 'S2001W0057': list3}
    # q.put(user_dict1)
    # q.put(user_dict2)
    # test代码


def trajectory_consumer(q, share_user_pair, share_var_user, q2):
    # 记录每个地点中用户的关联时间(不重复)，key:user  value:(list)[[t1,t2],..]
    # value中的list第一个元素value为剩余可关联时间，默认为600min,
    print("trajectory_consumer进程start!")
    str = ('201710040408','201920030118')
    str2 = ('201920030118','201710040408')
    def compare_user_time(user_name_one, user_time_dict, t, time_stamp):
        if user_name_one in user_time_dict:
            user_time_one = user_time_dict[user_name_one]
            time_end_flag = False
            for num in range(1, len(user_time_one)):
                user_time = user_time_one[num]
                if user_time[0] >= t[1]:
                    user_time_one.insert(num, t)     # 时间元素小于此用户存在的所有已关联的时间序列
                    user_time_one[0] += (t[1] - t[0]).total_seconds()
                    if user_time_one[0] >= 24000:
                        share_var_user.append(user_name_one)
                    break
                elif user_time[0] < t[1] <= user_time[1]:
                    if t[0] < user_time[0]:
                        user_time_one[num] = [min(user_time[0], t[0]), user_time[1]] # 时间元素起始时间小，扩大关联的此时间序列
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
                    if num == (len(user_time_one) - 1):
                        time_end_flag = True  # 最后一个时间段元素
            if time_end_flag:
                user_time_one.append(t)
                user_time_one[0] += (t[1] - t[0]).total_seconds()
                if user_time_one[0] >= 24000:
                    share_var_user.append(user_name_one)
            user_time_dict[user_name_one] = user_time_one
        else:
            user_time_one = [time_stamp, t]
            user_time_dict[user_name_one] = user_time_one
        return user_time_dict

    while True:
        print("队列长度：%d" % q.qsize())
        time.sleep(0.0000000000000000000001)
        if q.qsize() == 0:
            print("end")
            break
        # user_dict：key 用户, value 时间段
        user_dict = q.get()
        sorted_user_time(user_dict)
        user_list = list(user_dict)
        user_time_dict = {}
        # 循环比较学生行为轨迹时间段
        for i in range(len(user_list) - 1):
            user_name_one = user_list[i]
            # print("user_name_one:%s" % user_name_one)
            if user_name_one in share_var_user:
                break
            for j in range(i + 1, len(user_list)):
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
                    if share_user_pair[user_pair] >= 24000:
                        break
                elif user_pair2 in share_user_pair:
                    user_pair = user_pair2
                    if share_user_pair[user_pair] >= 24000:
                        break
                else:
                    user_flag = True
                    user_pair = user_pair1
                time_one_list = user_dict[user_name_one]
                time_two_list = user_dict[user_name_two]
                if user_pair == str or user_pair == str2:
                    print(user_pair)
                    print(time_one_list)
                    print(time_two_list)
                l = k = 0
                time_one_length = len(time_one_list)
                time_two_length = len(time_two_list)
                # 比较同一个地方出现的两个用户之间是否有关联的时间
                while True:
                    if l >= time_one_length or k >= time_two_length:
                        break
                    time1 = time_one_list[l]
                    time2 = time_two_list[k]
                    if time2[1] <= time1[0]:
                        k += 1
                        continue
                    elif time2[0] >= time1[1]:
                        l += 1
                        continue
                    elif time2[1] >= time1[1]:
                        l += 1
                    else:
                        k += 1
                    time_min = max(time2[0], time1[0])
                    time_max = min(time2[1], time1[1])
                    time_stamp = (time_max - time_min).total_seconds()
                    # print('time_stamp:%f'%time_stamp)
                    associated_time += time_stamp
                    t = [time_min, time_max]
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
    q2.put("end")
    # print('-----user_time_dict----')
    # print(user_time_dict)
    # print('-----share_user_pair-----')
    # print(share_user_pair)
    # print('-----share_var_user-----')
    # print(share_var_user)


def save_result(user_dict):
    # df = pd.DataFrame(columns=('Source', 'Target', 'Type', 'Weight'))
    print(len(user_dict))
    # for user in user_dict:
    #     df.loc[len(df)] = [user[0], user[1], '', user_dict[user]]
    df = pd.DataFrame.from_dict(user_dict, orient='index',columns=['Weight'])
    df = df.reset_index().rename(columns={'index': 'Source'})
    df.insert(1, 'Target', df['Source'].str[1])
    df.insert(2, 'Type', '')
    # df['Target'] = df['Source'].str[1]
    df['Source'] = df['Source'].str[0]
    print(df.head())
    filepath = result + edge
    df.to_csv(filepath, index=False, encoding="utf_8_sig")


if __name__ == '__main__':
    startTime = input("请输入查询的开始时间（格式：2020-11-22 00:00:00）：")
    endTime = input("请输入查询的结束时间（格式：2020-11-22 23:59:59）：")

    start = time.time()
    places_pair = account_places_number(startTime, endTime)
    places_dict = places_pair[0]
    address_places_list = places_pair[1]
    user_dict_pair = compare_trajectory(address_places_list, places_dict)
    save_result(user_dict_pair)
    # s = str(user_dict_pair)
    # f = open('user_dict_pair.txt', 'w')
    # f.writelines(s)
    # f.close()
    end = time.time()
    print('程序执行时间(s)： ', end - start)
