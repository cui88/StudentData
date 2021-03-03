# -*- coding:utf-8 -*-
import pandas as pd
import os
import sys
import pdb
import time
import datetime
import logging

dataPath = "data/xuehao-mac/"
userPath = "/home/guolab/pannian/python/user_file/"
user_id_file = 'UserMac20201231.csv'
rj_location_file = 'rj.xlsx'
h3_location_file = 'h3c.xls'

university = '湖南大学'
x_pi = 3.14159265358979324 * 3000.0 / 180.0
thread_num = 5
thread_flag = False

def GetUserDict():
    user_id_path = dataPath + user_id_file
    user_id_pd = pd.read_csv(user_id_path, na_values='NAN', encoding='utf_8_sig')
    csvDict = GetDict(user_id_pd, key_name='Device_Mac', value_name='UserID')
    return csvDict


# 直接把location位置拼接成一个文件
def GetRJFile():
    path = dataPath + rj_location_file
    writer = pd.ExcelFile(path)
    sheet_len = len(writer.sheet_names)
    rj_location_pd = pd.DataFrame()
    # 指定下标读取
    for i in range(0, sheet_len):
        df = pd.read_excel(writer, sheet_name=i)
        print(df.head())
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


def GetLocationDict():
    location_df = pd.concat([GetRJFile(), Geth3File()])
    csvDict = GetDict(location_df, key_name='AP_Mac', value_name='Location')
    print(csvDict)
    return csvDict


def GetDict(df, key_name, value_name):
    csvDict = {}
    for row in df.itertuples():
        csvDict[getattr(row, key_name)] = getattr(row, value_name)
    return csvDict

