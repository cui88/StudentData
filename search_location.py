# -*- coding:utf-8 -*-
# Author:pannian
import pandas
from bs4 import BeautifulSoup
from urllib import request
import re
import os
import sys
import pandas as pd
import numpy as np
import urllib.parse as urp
from xml.etree import ElementTree
import time
import argparse
import datetime
import folium
import pdb
import math
import threading
import queue
from my_ftp import MyFtp
Done = 'done'
Error = 'error'
Empty = 'empty'

dataPath = "/home/guolab/pannian/python/school_wifi/data/xuehao-mac/"
remotePath = "/opt/huda/bak/xuehao-mac/"

# python findLocation.py --ID S181401284 --U "2020-11-22 08:00:00" --D "2020-11-23 00:00:00"

university = '湖南大学'
x_pi = 3.14159265358979324 * 3000.0 / 180.0
thread_num = 5
thread_flag = False


# 创建线程池，一边ftp获取数据，一边处理数据
def GetFileDirList(path):
    file_list = os.listdir(path)
    return file_list


def GetTimeData(year_month_dict, up_time, down_time):
    begin = str(up_time)[:10]
    end = str(down_time)[:10]
    begin = datetime.datetime.strptime(begin, '%Y-%m-%d')
    end = datetime.datetime.strptime(end, '%Y-%m-%d')
    for x in range(-1, (end - begin).days + 1):
        day = (begin + datetime.timedelta(days=x)).strftime("%Y_%m%d")
        year_month = (begin + datetime.timedelta(days=x)).strftime("%Y_%m")
        year_month_dict.setdefault(year_month, []).append(day)


def GetFtpData(year_month_dict, q, user_dict):
    ftp = MyFtp('202.197.98.90', year_month_dict, q, user_dict)

    ftp.Login('logana', '1qaz@WSX')

    ftp.DowloadUserFile(dataPath, remotePath)
    ftp.DownLoadApFile(dataPath, remotePath)

    ftp.close()
    thread_flag = True
    print("over")


def draw_gps(locations, output_path, file_name):
    """
    绘制gps轨迹图
    :param locations: list, 需要绘制轨迹的经纬度信息，格式为[[lat1, lon1], [lat2, lon2], ...]
    :param output_path: str, 轨迹图保存路径
    :param file_name: str, 轨迹图保存文件名
    :return: None
    """
    m = folium.Map(bd09_to_gcj02(112.950693, 28.186051),
                   zoom_start=17,
                   tiles='http://webrd02.is.autonavi.com/appmaptile?lang=zh_cn&size=1&scale=1&style=7&x={x}&y={y}&z={z}',
                   attr='AutoNavi')
    folium.PolyLine(  # polyline方法为将坐标用线段形式连接起来
        locations,  # 将坐标点连接起来
        weight=3,  # 线的大小为3
        color='orange',  # 线的颜色为橙色
        opacity=0.8,  # 线的透明度
        height='50%',
        width='50%'
    ).add_to(m)  # 将这条线添加到刚才的区域m内

    # 起始点，结束点
    for i in range(len(locations)):
        if i == 0:
            print("起点")
            folium.Marker(locations[i], popup='<b>Starting Point</b>').add_to(m)
        elif i == (len(locations) - 1):
            print("终点")
            folium.Marker(locations[i], popup='<b>End Point</b>').add_to(m)
        else:
            print("路途点")
            folium.Marker(locations[i], icon=folium.Icon(color='green')).add_to(m)

    m.save(os.path.join(output_path, file_name))  # 将结果以HTML形式保存到指定路径


def __get_locaton1__(data, city):
    my_ak = 'AhLF63hKP8iXpMvXSiHjGhXsaEByvoKY'  # 需要自己填写自己的AK
    tag = urp.quote('地铁站')  # URL编码
    location = []
    for i in range(len(data)):
        if not pd.isnull(data['Location'][i]):
            name = university + data['Location'][i]
            qurey = urp.quote(name)
            try:
                url = 'http://api.map.baidu.com/place/v2/search?query=' + qurey + '&tag=' + '&region=' + urp.quote(
                    city) + '&output=json&ak=' + my_ak
                req = request.urlopen(url)
                res = req.read().decode()
                lat = pd.to_numeric(re.findall('"lat":(.*)', res)[0].split(',')[0])
                lng = pd.to_numeric(re.findall('"lng":(.*)', res)[0])
                location.append(bd09_to_gcj02(lng, lat))
                print(lat, lng)  # 纬度和经度
            except Exception as e:
                print(e.args)
        else:
            print("Location is empty")
    return location


def bd09_to_gcj02(bd_lon, bd_lat):
    x = bd_lon - 0.0065
    y = bd_lat - 0.006
    z = math.sqrt(x * x + y * y) - 0.00002 * math.sin(y * x_pi)
    theta = math.atan2(y, x) - 0.000003 * math.cos(x * x_pi)
    gg_lng = z * math.cos(theta)
    gg_lat = z * math.sin(theta)
    return [gg_lat, gg_lng]


def GetDeviceList(user_id, device_list):
    for k, v in test_dict.items():
        if v.strip() == user_id:
            device_list.append(k)


def find(user_id, startTime, endTime):
    # 获取所有文件列表
    data_df = pd.DataFrame(columns=['user_id', 'up_time', 'down_time', 'ap_mac', 'location'])
    year_month_dict = {}
    user_dict = {}
    threads = []
    device_list = []
    q = queue.Queue()
    lock = threading.Lock()

    GetTimeData(year_month_dict, startTime, endTime)

    # source_file = pd.read_csv(file_path, na_values='NAN', encoding='utf_8_sig')
    # source_file['Up_Time'] = pd.to_datetime(source_file['Up_Time'])
    # source_file['Down_Time'] = pd.to_datetime(source_file['Down_Time'])
    # 往队列里生成消息
    ftp_thread = threading.Thread(target=GetFtpData, args=(year_month_dict, q, user_dict))
    ftp_thread.start()
    while True:
        if not user_dict:
            continue
        else:
            try:
                GetDeviceList(user_id, device_list)
            except Exception as e:
                print(e.args)
            break
    for i in range(thread_num):
        t = threading.Thread(target=work, args=(q, device_list, local_dict, user_id, startTime, endTime, data_df, lock,))
        # args需要输出的是一个元组，如果只有一个参数，后面加，表示元组，否则会报错
        threads.append(t)

    for i in range(thread_num):
        threads[i].start()
    for i in range(thread_num):
        threads[i].join()

    if df_data.empty:
        print("无法查到您要的数据！")
        return
    df_data = df_data.sort_values('Up_Time').reset_index(drop=True)
    print(df_data)
    return df_data


def work(q, device_list, local_dict, user_id, up_time, down_time, data_df, lock):
    startDateTime = strToDateTime(up_time)
    endDateTime = strToDateTime(down_time)
    while True:
        if q.empty() and thread_flag:
            return
        elif q.empty():
            continue
        else:
            ap_path = q.get()
            ap_pd = pd.read_csv(ap_path, na_values='NAN', encoding='utf_8_sig')
            ap_pd_group = ap_pd.groupby(ap_pd['Device_Mac'])
            for name, group in ap_pd_group:
                if name.strip() in device_list:  # 找不到设备号即找不到对应的学号信息，可忽略
                    # 获取学号和Ap_Mac地址，Duration累加
                    for i in (len(group)):
                        upTime = group['Up_Time'][i]
                        downTime = group['Down_Time'][i]
                        ap_mac = group['AP_Mac'][i].lower()
                        if ap_mac in local_dict:
                            location = local_dict[ap_mac]
                            if compareDateTime(upTime, startDateTime) and compareDateTime(endDateTime, upTime):  # 比较时间大小
                                # 加一把锁
                                lock.acquire()
                                data_df.loc[len(data_df)] = [user_id, upTime, downTime, ap_mac, location]
                                print(data_df)
                                lock.release()


# 日期排序
def sort_behaviors_by_timestamp(behaviors):
    behaviors = sorted(behaviors, key=lambda behavior: behavior['Up_Time'], reverse=False)
    return behaviors


def compareDateTime(dt1, dt2):
    diff = dt2 - dt1
    return True if diff.days < 0 else False  # <0则前者较小


def strToDateTime(str):
    return datetime.datetime.strptime(str, '%Y-%m-%d %H:%M:%S')


if __name__ == '__main__':
    filename = 'personal_line.html'

    parser = argparse.ArgumentParser(description='manual to this script')
    parser.add_argument('--ID', type=str, default=None, help='an id for unique identification user')
    parser.add_argument('--U', type=str, default=None, help='upTime,just like 2020-11-22 07:00:00')
    parser.add_argument('--D', type=str, default=None, help='downTime,just like 2020-11-22 23:00:00')
    args = parser.parse_args()

    start = time.time()

    startTime = args.U
    endTime = args.D
    userID = args.ID
    html_path = "/home/guolab/pannian/python/school_wifi/html/"

    df_data = find(userID, startTime, endTime)

    data_dict = __get_location1__(df_data, '长沙')

    draw_gps(data_dict, html_path, filename)

    end = time.time()
    print('程序执行时间(s)： ', end - start)
