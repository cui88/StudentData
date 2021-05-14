# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import thermodynamic
import get_lat_lng
import time
import datetime
import pandas as pd
import csv
from multiprocessing import Queue, JoinableQueue, Process, Manager
def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


def getDict(df, key_name, value_name1, value_name2):
    csvDict = {}
    for row in df.itertuples():
        csvDict[getattr(row, key_name).strip()] = [getattr(row, value_name1), getattr(row, value_name2)]
    return csvDict


def Get_MessageFile():
    path = 'F:/guolab/python/data/xuehao-mac/message.xls'
    writer = pd.ExcelFile(path)
    sheet_len = len(writer.sheet_names)
    message_pd = pd.DataFrame()
    # 指定下标读取
    for i in range(0, sheet_len):
        df = pd.read_excel(writer, sheet_name=i)
        df.columns = ["UserID", "Name", "XY", "ZY", "IN", "Identity", ]
        df = df[['UserID', 'Name', 'Identity']]
        df['UserID'] = df['UserID'].str.upper()
        message_pd = pd.concat([df, message_pd])
    csvDict = getDict(message_pd, key_name='UserID', value_name1='Name', value_name2='Identity')
    print(len(csvDict))
    return csvDict


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # thermodynamic.GetLocationDict()
    # str2 = '2020-11-22 09:25:01'
    # str1 = '2020-11-23 09:21:04'
    # startDateTime = datetime.datetime.strptime(str1, '%Y-%m-%d %H:%M:%S')
    # endDateTime = datetime.datetime.strptime(str2, '%Y-%m-%d %H:%M:%S')
    # print((endDateTime - startDateTime).total_seconds())
    # path = 'F:/guolab/python/data/result/edge.csv'
    # df = pd.read_csv(path, na_values='NAN', encoding='utf_8_sig')
    # df['new_col'] = list(zip(df.Source, df.Target))
    # dict = df.set_index(['new_col'])['Weight'].to_dict()
    # str = ('201710040408','201920030118')
    # str2 = ('201920030118','201710040408')
    # if str in dict:
    #     print(dict[str])
    # else:
    #     print("str1不存在")
    # if str2 in dict:
    #     print("2222")
    #     print(dict[str2])
    # else:
    #     print("str2不存在")
    # get_lat_lng.getdata()
    # str = 'end'
    # q = Queue()
    # q.put(str)
    # print(q.qsize())
    path = 'F:/guolab/python/data/xuehao-mac/UserMac20201231.csv'
    user_id_pd = pd.read_csv(path, na_values='NAN', encoding='utf_8_sig')
    user_id_pd = user_id_pd[['UserID']].drop_duplicates()
    user_id_pd = user_id_pd.reset_index(drop=True)
    print(user_id_pd.head())
    user_id_pd['UserID'] = user_id_pd['UserID']
    user_id_pd['Name'] = '0'
    user_id_pd['Identity'] = '0'
    message_dict = Get_MessageFile()
    notMessagelist = []
    j = 0
    for i in range(len(user_id_pd)):
        user_id = user_id_pd['UserID'][i].upper().strip()
        if user_id in message_dict:
            # print('---')
            name = message_dict[user_id][0]
            identity = message_dict[user_id][1]
            user_id_pd['Name'][i] = name
            user_id_pd['Identity'][i] = identity
        else:
            j += 1
            print("%s无法找到！" % user_id)
            notMessagelist.append(user_id)
    print(user_id_pd.head())
    print(j)
    user_id_pd.to_csv('node.csv', index=0,encoding="utf_8_sig")
    #
    # with open('notmessage.csv', 'w', newline='') as csvfile:
    #     writer = csv.writer(csvfile)
    #     for row in notMessagelist:
    #         writer.writerow(row)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
