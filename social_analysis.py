# -*- coding:utf-8 -*-
from multiprocessing import Queue, JoinableQueue, Process, Manager
import os
import pandas as pd
import time
import datetime
import folium
import math
from folium.plugins import HeatMap
from selenium import webdriver
from PIL import Image

# 设定每个用户每天初始活动时间为 10h*60min = 600min
# 按地点访问人数降序排列


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

    GetTimeData(year_month_dict, startTime, endTime)


if __name__ == '__main__':
    startTime = input("请输入查询的开始时间（格式：2020-11-22 08:00:00）：")
    endTime = input("请输入查询的结束时间（格式：2020-11-23 23:00:00）：")

    start = time.time()
    account_places_number(startTime, endTime)
    end = time.time()
    print('程序执行时间(s)： ', end - start)