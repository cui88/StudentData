# -*- coding:utf-8 -*-
import os
import pandas as pd


path = 'F:/21年信息化办/数据库用户/'


def GetFileDirList():
    file_list = os.listdir(path)
    return file_list


def GetSystemPd():
    dataPath = 'F:/21年信息化办/信息化办虚拟机总表(1).xlsx'
    writer = pd.ExcelFile(dataPath)
    sheet_len = len(writer.sheet_names)
    system_pd = pd.DataFrame()
    # 指定下标读取
    for i in range(0, sheet_len):
        df = pd.read_excel(writer, sheet_name=i)
        df = df[['IP地址', '虚拟机用途']]
        system_pd = pd.concat([df, system_pd])
    system_pd = system_pd[system_pd['IP地址'].notna()]
    system_pd = system_pd[system_pd['虚拟机用途'].notna()]
    return system_pd


def getSystemDict():
    system_pd = GetSystemPd()
    csvDict = getDict(system_pd, key_name='IP地址', value_name='虚拟机用途')
    return csvDict


def getDict(df, key_name, value_name):
    csvDict = {}
    for row in df.itertuples():
        csvDict[getattr(row, key_name)] = getattr(row, value_name)
    return csvDict


def user_search(result_df):
    file_list = GetFileDirList()
    system_dict = getSystemDict()
    m_judge = 'from'
    for file in file_list:
        user_pd = pd.DataFrame(columns=['用户', '表名', 'IP地址', '系统名'])
        file_path = path + file
        user_name = file[:-4]
        df = pd.read_csv(file_path, na_values='NAN', encoding='gb18030')
        for idx, row in df.iterrows():
            res = row['报文'].lower()
            if not res.endswith(' '):
                res += ' '
            result = res.find(m_judge, 0, len(res)-1)
            if result != -1:
                result2 = res.find(' ', result +5, len(res)-1)
                if result2 != -1:
                    str = res[result+5:result2]
                else:
                    str = res[result+5:len(res)-1]
                str = str.replace(" ", "")
                view_judge = 'view'
                if len(str) != 0:
                    result3 = str.find('.', 0, len(str)-1)
                    str2 = str
                    if result3 != -1:
                        str2 = str[result3+1:len(str)-1]
                    if str2[0] == 'v' or str2[1] == 'v' or view_judge in str2:
                        ip = row['客户端IP']
                        system_name = system_dict[ip]
                        user_pd.loc[len(user_pd)] = [user_name, str2, ip, system_name]
        user_pd.drop_duplicates(subset='表名')
        result_df = pd.concat([user_pd, result_df])
    result_df.to_excel('F:/21年信息化办/用户记录.xlsx', index=False, encoding='utf-8')
    print(result_df.head())


if __name__ == '__main__':
    user_pd = pd.DataFrame(columns = ['用户','表名','IP地址','系统名'])
    user_search(user_pd)