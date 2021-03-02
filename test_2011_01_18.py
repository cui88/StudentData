# -*- coding:utf-8 -*-
import pandas as pd
import os
import sys
import pdb
import time
import datetime
import logging

logger = logging.getLogger(__name__)
logger.setLevel(level = logging.INFO)
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
#1.遍历user_mac——12-31号文件
#2.去ap文件寻找对应的device_mac信息（可以考虑线程池）
#3.根据ap_mac匹配统计位置

#直接把location位置拼接成一个文件
def GetRJFile():
	path = dataPath + rj_location_file
	writer = pd.ExcelFile(path)
	sheet_len = len(writer.sheet_names)
	rj_location_pd = pd.DataFrame()
	# 指定下标读取
	for i in range(0,sheet_len):
		df = pd.read_excel(writer,sheet_name=i)
		df = df[['楼栋','AP_MAC']]
		df = df.rename(columns={'AP_MAC':'AP_Mac','楼栋':'Location'})
		df['AP_Mac'] = df['AP_Mac'].str.lower()
		rj_location_pd = pd.concat([df,rj_location_pd])
	rj_location_pd['AP_Mac'] = rj_location_pd['AP_Mac'].str.lower()
	return rj_location_pd

def Geth3File():
	path = dataPath + h3_location_file
	h3_location_pd = pd.read_excel(path)
	h3_location_pd = h3_location_pd[['序列号','描述']]	#只提取这两列
	h3_location_pd = h3_location_pd.rename(columns={'序列号':'AP_Mac','描述':'Location'})
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
		path = os.path.join(dataPath,cur_file)
		if os.path.isfile(path):
			ap_file_month_list.remove(cur_file)
	print(ap_file_month_list)

	dataList = []
	# ap_file_month_list = ['2020_03']
	i = 0
	l = 0
	for cur_file in ap_file_month_list:
		path = os.path.join(dataPath,cur_file)
		ap_file_list = GetFileDirList(path)
		# print(ap_file_list)
		for ap_file in ap_file_list:
			ap_file_path = os.path.join(path,ap_file)
			if os.path.isfile(ap_file_path):
				l +=1
				ap_pd = pd.read_csv(ap_file_path,na_values='NAN',encoding='utf_8_sig')
				ap_pd_group = ap_pd.groupby(ap_pd['AP_Mac'])
				for name,group in ap_pd_group:
					ap_mac = name.lower()
					# pdb.set_trace()
					if ap_mac.strip() not in local_dict:#找不到设备号即找不到对应的学号信息，可忽略
						#获取学号和Ap_Mac地址，Duration累加	
						if ap_mac.strip() not in dataList:	
							dataList.append(ap_mac)

		print("进度条：%.4f"%(l/8844))
	print("l = ",l)
	filename = "/home/guolab/pannian/python/ap_mac_02.csv"
	name = ['AP_Mac']
	test=pd.DataFrame(columns=name,data=dataList)#数据有三列，列名分别为one,two,three
	test.to_csv(filename,encoding='utf-8',index=False)

def GetFileDirList(path):
	file_list = os.listdir(path)
	return file_list

def GetUserDict():
	user_id_path = dataPath + user_id_file
	user_id_pd = pd.read_csv(user_id_path,na_values='NAN',encoding='utf_8_sig')
	csvDict = GetDict(user_id_pd,key_name ='Device_Mac',value_name = 'UserID')
	return csvDict

def GetLocationDict():
	location_df = pd.concat([GetRJFile(),Geth3File()])
	csvDict = GetDict(location_df,key_name ='AP_Mac',value_name = 'Location')
	return csvDict

def GetDict(df,key_name,value_name):
	csvDict = {}
	for row in df.itertuples():
		csvDict[getattr(row,key_name)] = getattr(row,value_name)
	return csvDict

if __name__ == '__main__':
	start=time.time()

	GetAPMessage() 
	# FilterData()

	end = time.time()
	time_m = end-start
	print("time: "+str(time_m))