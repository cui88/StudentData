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
rj_location_file = 'rj.xlsx'
h3_location_file = 'h3c.xls'
user_file_name = 'UserMac20201231.csv'
process_num = 5
latlng_file_name = 'location.xlsx'
other_location_file = 'other_place.xls'

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


def GetTimeData(year_month_dict, up_time, down_time):
    begin = str(up_time)[:10]
    end = str(down_time)[:10]
    begin = datetime.datetime.strptime(begin, '%Y-%m-%d')
    end = datetime.datetime.strptime(end, '%Y-%m-%d')
    for x in range(-1, (end - begin).days + 1):
        day = (begin + datetime.timedelta(days=x)).strftime("%Y_%m%d")
        year_month = (begin + datetime.timedelta(days=x)).strftime("%Y_%m")
        year_month_dict.setdefault(year_month, []).append(day)


def account_places_number(startTime, endTime):
    year_month_dict = {}
    device_list = []
    user_path = os.path.join(dataPath, user_file_name)

    GetTimeData(year_month_dict, startTime, endTime)

    user_dict = GetUserDict(user_path)
    local_dict = getLocationDict()


if __name__ == '__main__':
    startTime = input("请输入查询的开始时间（格式：2020-11-22 08:00:00）：")
    endTime = input("请输入查询的结束时间（格式：2020-11-23 23:00:00）：")

    start = time.time()
    account_places_number(startTime, endTime)
    end = time.time()
    print('程序执行时间(s)： ', end - start)