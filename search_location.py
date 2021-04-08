# -*- coding:utf-8 -*-
# Author:pannian
from multiprocessing import Queue, Process
from bs4 import BeautifulSoup
from urllib import request
import re
import os
import sys
import pandas as pd
import numpy as np
import urllib.parse as urp
import time
import datetime
import folium
import pdb
import math

Done = 'done'
Error = 'error'
Empty = 'empty'

dataPath = "data/xuehao-mac/"

# python findLocation.py --ID S181401284 --U "2020-11-22 08:00:00" --D "2020-11-23 00:00:00"

university = '湖南大学'
user_file_name = 'UserMac20201231.csv'
x_pi = 3.14159265358979324 * 3000.0 / 180.0
process_num = 5
rj_location_file = 'rj.xlsx'
h3_location_file = 'h3c.xls'
other_location_file = 'other_place.xls'
latlng_file_name = 'location.xlsx'


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


def parse_zhch(s):
    return str(str(s).encode('ascii', 'xmlcharrefreplace'))[2:-1]


def draw_gps(user_id, locations, output_path, startTime, endTime):
    """
    绘制gps轨迹图
    :param locations: list, 需要绘制轨迹的经纬度信息，格式为[[lat1, lon1], [lat2, lon2], ...]
    :param output_path: str, 轨迹图保存路径
    :param file_name: str, 轨迹图保存文件名
    :return: None
    """
    m = folium.Map(bd09_to_gcj02(112.950221, 28.180776),
                   zoom_start=16,
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
    for i in range(1, len(locations)-1):
        flag = '第' + str(i+1) + '站'
        folium.Marker(locations[i], icon=folium.Icon(color='green'), popup=folium.Popup(html=parse_zhch(flag), max_width=100)).add_to(m)
    folium.Marker(locations[0], popup=folium.Popup(html=parse_zhch('起点'), max_width=100), icon=folium.Icon(color='red')).add_to(m)
    folium.Marker(locations[len(locations) - 1], popup=folium.Popup(html=parse_zhch('终点'), max_width=100), icon=folium.Icon(color='red')).add_to(m)
    title_html = f'''<h3 align="center" style="font-size:20px">
    <b>HNU Trajectory maps for <span style="color:#FF8800 ">{user_id}</span> on <span style="color:#FF8800 ">{str(startTime)}</span> - <span style="color:#FF8800 ">{str(endTime)}</span></b></h3>'''
    m.get_root().html.add_child(folium.Element(title_html))
    file_name = user_id + ".html"
    m.save(os.path.join(output_path, file_name))  # 将结果以HTML形式保存到指定路径


def get_location(data):
    data_list = []
    latlng_path = os.path.join(dataPath, latlng_file_name)
    lat_lng_dict = getLatLngDict(latlng_path)
    if not data.empty:
        for i in range(len(data)):
            address = data['location'][i]
            if address in lat_lng_dict:
                lat = float(lat_lng_dict[address].split(",", 1)[0])
                lng = float(lat_lng_dict[address].split(",", 1)[1])
                data_list.append([lat, lng]) #高德地图API支持[纬度，经度]
    return data_list


def bd09_to_gcj02(bd_lon, bd_lat):
    x = bd_lon - 0.0065
    y = bd_lat - 0.006
    z = math.sqrt(x * x + y * y) - 0.00002 * math.sin(y * x_pi)
    theta = math.atan2(y, x) - 0.000003 * math.cos(x * x_pi)
    gg_lng = z * math.cos(theta)
    gg_lat = z * math.sin(theta)
    return [gg_lat, gg_lng]


def GetDeviceList(user_id, device_list, user_dict):
    for k, v in user_dict.items():
        if v.strip() == user_id:
            device_list.append(k)


def GetUserDict(path):
    user_id_pd = pd.read_csv(path, na_values='NAN', encoding='utf_8_sig')
    csvDict = getDict(user_id_pd, key_name='Device_Mac', value_name='UserID')
    return csvDict


def getLatLngDict(path):
    lat_lng_pd = pd.read_excel(path, keep_default_na=False)
    csvDict = getDict(lat_lng_pd, key_name='location', value_name='lat_lon')
    return csvDict


def find(user_id, startTime, endTime):
    # 获取所有文件列表
    year_month_dict = {}
    device_list = []

    GetTimeData(year_month_dict, startTime, endTime)
    print(year_month_dict)

    user_path = os.path.join(dataPath, user_file_name)

    user_dict = GetUserDict(user_path)
    local_dict = getLocationDict()
    # lat_lng_dict = getLatLngDict(latlng_path)

    # 多进程
    q = Queue()
    q2 = Queue()
    GetDeviceList(user_id, device_list, user_dict)
    # 创建生产者
    p = Process(target=producer, args=(q, year_month_dict))
    p.start()
    # 创建消费者
    c = []

    for i in range(process_num):
        c.append(Process(target=consumer, args=(q, device_list, local_dict, user_id, startTime, endTime, q2)))
        c[i].start()

    result_df = pd.DataFrame()

    for i in range(process_num):
        while True:
            if not q2.empty():
                data_df = pd.DataFrame(q2.get())
                print(data_df)
                result_df = pd.concat([result_df, data_df], axis=0)
                break

    # 多进程
    for i in range(process_num):
        c[i].join(timeout=1)
        if c[i].is_alive():
            c[i].terminate()
            c[i].join()

    if result_df.empty:
        print(f"关于{user_id}:无法查到您要的数据！")
        return result_df
    result_df = result_df.sort_values('up_time').reset_index(drop=True)
    return result_df


# 多进程
def consumer(q, device_list, local_dict, user_id, up_time, down_time, q2):
    result_list = []
    startDateTime = strToDateTime(up_time)
    endDateTime = strToDateTime(down_time)
    while True:
        if q.empty():
            break
        ap_path = q.get()
        ap_file_path = os.path.abspath(os.path.join(dataPath, ap_path))
        if os.path.exists(ap_file_path):
            ap_pd = pd.read_csv(ap_file_path, na_values='NAN', encoding='utf_8_sig')
            if not ap_pd.empty:
                judge_time = strToDateTime(ap_pd['Up_Time'][len(ap_pd)-1])
                if compareDateTime(judge_time, startDateTime) and compareDateTime(endDateTime,
                                                                                              judge_time):
                    ap_pd_group = ap_pd.groupby(ap_pd['Device_Mac'])
                    for name, group in ap_pd_group:
                        if name.strip() in device_list:  # 找不到设备号即找不到对应的学号信息，可忽略
                            # 获取学号和Ap_Mac地址,比较时分秒时间
                            group = group.reset_index(drop=True)
                            for i in range(len(group)):
                                upTime = strToDateTime(group['Up_Time'][i])
                                downTime = strToDateTime(group['Down_Time'][i])
                                ap_mac = group['AP_Mac'][i].lower()
                                if ap_mac.strip() in local_dict:
                                    location = local_dict[ap_mac]
                                    if compareDateTime(upTime, startDateTime) and compareDateTime(endDateTime,
                                                                                                  upTime):  # 比较时间大小
                                        # 加一把锁
                                        dict_iter = {'user_id': user_id, 'up_time': upTime, 'down_time': downTime,
                                                     'ap_mac': ap_mac, 'location': location}
                                        result_list.append(dict_iter)
        else:
            print("%s件不存在!" % ap_path)
    q2.put(result_list)


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
                q.put(h3_file_path)
                q.put(rj_file_path)


# 多进程


# 日期排序
def sort_behaviors_by_timestamp(behaviors):
    behaviors = sorted(behaviors, key=lambda behavior: behavior['Up_Time'], reverse=False)
    return behaviors


def compareDateTime(dt1, dt2):
    return True if dt2 <= dt1 else False  # <0则前者较小


def strToDateTime(str):
    return datetime.datetime.strptime(str, '%Y-%m-%d %H:%M:%S')


def GetFileDirList(path):
    file_list = os.listdir(path)
    return file_list


def Get_RJFile():
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


def Get_h3File():
    path = dataPath + h3_location_file
    h3_location_pd = pd.read_excel(path, keep_default_na=False)
    h3_location_pd = h3_location_pd[['序列号', '描述']]  # 只提取这两列
    h3_location_pd = h3_location_pd.rename(columns={'序列号': 'AP_Mac', '描述': 'Location'})
    h3_location_pd['AP_Mac'] = h3_location_pd['AP_Mac'].str.lower()
    # pdb.set_trace()
    return h3_location_pd


def Get_otherplaceFile():
    path = dataPath + other_location_file
    other_location_pd = pd.read_excel(path, keep_default_na=False)
    other_location_pd = other_location_pd[['AP_Mac', 'Location']]  # 只提取这两列
    other_location_pd['AP_Mac'] = other_location_pd['AP_Mac'].str.lower()
    return other_location_pd


def getLocationDict():
    location_df = pd.concat([Get_RJFile(), Get_h3File(), Get_otherplaceFile()])
    csvDict = getDict(location_df, key_name='AP_Mac', value_name='Location')
    return csvDict


def getDict(df, key_name, value_name):
    csvDict = {}
    for row in df.itertuples():
        csvDict[getattr(row, key_name)] = getattr(row, value_name)
    return csvDict


if __name__ == '__main__':
    filename = 'personal_line.html'

    userID = input("请输入学号（S181401284）：")
    startTime = input("请输入查询的开始时间（格式：2020-11-22 08:00:00）：")
    endTime = input("请输入查询的结束时间（格式：2020-11-23 23:00:00）：")
    html_path = "html/"
    start = time.time()
    df_data = find(userID, startTime, endTime)
    if not df_data.empty:
        data_list = get_location(df_data)
        if data_list:
            draw_gps(userID, data_list, html_path, startTime, endTime)
        else:
            print(f"关于{userID}:由于信息不完善,无法找到对应您轨迹的物理地址!")
    end = time.time()
    print('程序执行时间(s)： ', end - start)
