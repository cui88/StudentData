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
from IPython.display import IFrame

dataPath = "data/xuehao-mac/"
picture = "picture/"
rj_location_file = 'rj.xlsx'
h3_location_file = 'h3c.xls'
user_file_name = 'UserMac20201231.csv'
process_num = 5
university = '湖南大学'
x_pi = 3.14159265358979324 * 3000.0 / 180.0
latlng_file_name = 'location.xlsx'
other_location_file = 'other_place.xls'


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


def getDict(df, key_name, value_name):
    csvDict = {}
    for row in df.itertuples():
        csvDict[getattr(row, key_name)] = getattr(row, value_name)
    return csvDict


def bd09_to_gcj02(bd_lon, bd_lat):
    x = bd_lon - 0.0065
    y = bd_lat - 0.006
    z = math.sqrt(x * x + y * y) - 0.00002 * math.sin(y * x_pi)
    theta = math.atan2(y, x) - 0.000003 * math.cos(x * x_pi)
    gg_lng = z * math.cos(theta)
    gg_lat = z * math.sin(theta)
    return [gg_lat, gg_lng]


def draw_gps(name, locations, output_path, png_path):
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

    HeatMap(locations).add_to(m)
    name = str(name)
    title_html = f'''<h3 align="center" style="font-size:20px">
    <b>HNU WIFI-DATA Heatmaps at <span style="color:#FF8800 ">{name}</span></b></h3>'''
    filename = '.html'
    name = name.replace(' ', '').replace(':', '_')
    path = os.path.join(output_path, name + filename)
    m.get_root().html.add_child(folium.Element(title_html))
    m.save(path)  # 将结果以HTML形式保存到指定路径
    # print(path)
    savetopng(path, name, png_path)


def savetopng(path, name, png_path):
    browser = webdriver.Chrome()
    path = os.path.abspath(path)
    browser.get(path)
    time.sleep(1)
    browser.maximize_window()
    filename = os.path.abspath(picture + name + '_heatmap.png')
    browser.save_screenshot(filename)
    browser.quit()
    png_path.append(filename)


def png_to_gif(png_path, duration):
    frames = []
    for i in png_path:
        im = Image.open(i)
        cut_size = [int(x*0.8) for x in im.size]
        im = im.resize(cut_size, Image.ANTIALIAS)
        frames.append(im.copy())
    name = str(datetime.datetime.now()).replace(' ', '').replace(':', '_')
    save_file_path = picture + name + "_heatmap.gif"
    frames[0].save(f'{save_file_path}', format='GIF', append_images=frames[1:], save_all=True, duration=duration,
                   loop=0)


def GetTimeData(year_month_dict, up_time, down_time):
    begin = str(up_time)[:10]
    end = str(down_time)[:10]
    begin = datetime.datetime.strptime(begin, '%Y-%m-%d')
    end = datetime.datetime.strptime(end, '%Y-%m-%d')
    for x in range(-1, (end - begin).days + 2):
        day = (begin + datetime.timedelta(days=x)).strftime("%Y_%m%d")
        year_month = (begin + datetime.timedelta(days=x)).strftime("%Y_%m")
        year_month_dict.setdefault(year_month, []).append(day)


def getLocationDict():
    location_df = pd.concat([Get_RJFile(), Get_h3File(), Get_otherplaceFile()])
    csvDict = getDict(location_df, key_name='AP_Mac', value_name='Location')
    return csvDict


def compareDateTime2(dt1, dt2):
    return True if dt2 <= dt1 else False  # <0则前者较小


def strToDateTime(str):
    return datetime.datetime.strptime(str, '%Y-%m-%d %H:%M:%S')


def GetFileDirList(path):
    file_list = os.listdir(path)
    return file_list


def getLatLngDict(path):
    lat_lng_pd = pd.read_excel(path, keep_default_na=False)
    csvDict = getDict(lat_lng_pd, key_name='location', value_name='lat_lon')
    return csvDict


def GetTimeList(time_list, up_time, down_time):
    t1 = datetime.datetime.strptime(up_time[:13], '%Y-%m-%d %H')
    t2 = datetime.datetime.strptime(down_time[:13], '%Y-%m-%d %H')
    days = ((t2 - t1).days * 86400)
    hours, res = divmod((t2 - t1).seconds + days, 3600)
    for i in range(0, hours + 1):
        result_time = t1 + datetime.timedelta(hours=i)
        time_list.append(result_time)
    # print(time_list)


def work(startTime, endTime):
    # signal.signal(signal.SIGCHLD, wait_child)
    year_month_dict = {}
    time_list = []
    result_list = []
    GetTimeList(time_list, startTime, endTime)
    GetTimeData(year_month_dict, startTime, endTime)
    # print(year_month_dict)
    user_path = os.path.join(dataPath, user_file_name)
    latlng_path = os.path.join(dataPath, latlng_file_name)

    user_dict = GetUserDict(user_path)
    local_dict = getLocationDict()

    lat_lng_dict = getLatLngDict(latlng_path)
    # 多进程
    result_dict = {}
    q = JoinableQueue()
    q2 = Queue()
    # 创建生产者
    p = Process(target=producer, args=(q, year_month_dict))
    p.start()
    # 创建消费者
    c = []
    share_var = Manager().list()
    for i in range(process_num):
        c.append(
            Process(target=consumer, args=(q, user_dict, local_dict, lat_lng_dict, startTime, endTime, q2, share_var)))
        c[i].start()

    for i in range(process_num):
        while True:
            if not q2.empty():
                result_list.append(q2.get())
                break
    # print(result_list)
    # 多进程
    for i in range(process_num):
        print("----%d:准备回收进程" % i)
        c[i].join(timeout=1)
        print("----%d:回收进程已结束" % i)
        if c[i].is_alive():
            c[i].terminate()
            c[i].join()
    print(len(share_var))
    # type(i)为datetime
    for i in time_list:
        # type(j)为dict
        for j in result_list:
            if i in j:
                # j[i]为dict
                result_dict.setdefault(i, []).extend(j[i])
                print("----:" + str(i) + "长度%d" % len(j[i]))
    # print(len(result_dict[datetime.datetime.strptime('2020-11-22 08', '%Y-%m-%d %H')]))
    return result_dict


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
                while q.qsize() >= 5:
                    continue;
                q.put(h3_file_path)
                q.put(rj_file_path)
    q.join()


def consumer(q, user_dict, local_dict, lat_lng_dict, up_time, down_time, q2, share_var):
    result_dict = {}
    startDateTime = datetime.datetime.strptime(up_time[:13], '%Y-%m-%d %H')
    endDateTime = datetime.datetime.strptime(down_time[:13], '%Y-%m-%d %H')
    while True:
        # print(q.qsize())
        if q.empty():
            # print("end")
            break
        ap_path = q.get()
        ap_file_path = os.path.abspath(os.path.join(dataPath, ap_path))
        if os.path.exists(ap_file_path):
            ap_pd = pd.read_csv(ap_file_path, na_values='NAN', encoding='utf_8_sig')
            if not ap_pd.empty:
                ap_pd['time'] = pd.to_datetime(ap_pd['Up_Time'].str[:13])
                ap_pd.sort_values('time', inplace=True)
                ap_pd = ap_pd.reset_index(drop=True)
                # print(ap_pd.head())
                judge_time = ap_pd['time'][len(ap_pd) - 1]
                if compareDateTime2(judge_time, startDateTime) and compareDateTime2(endDateTime,
                                                                                    judge_time):
                    ap_pd_group = ap_pd.groupby(ap_pd['time'])
                    for name, group in ap_pd_group:
                        if compareDateTime2(name, startDateTime) and compareDateTime2(endDateTime,
                                                                                      name):
                            group = group.reset_index(drop=True)
                            for i in range(len(group)):
                                # print(group['Up_Time'][i])
                                device_mac = group['Device_Mac'][i]
                                if device_mac.strip() in user_dict:
                                    user_id = user_dict[device_mac]
                                    user_flag = str(name) + user_id
                                    # share_var  共享的user_id存储为list
                                    if user_flag.strip() not in share_var:
                                        share_var.append(user_flag)
                                    else:
                                        continue
                                    ap_mac = group['AP_Mac'][i].lower()
                                    if ap_mac.strip() in local_dict:
                                        location = lat_lng_dict[local_dict[ap_mac]]
                                        lat = float(location.split(",", 1)[0])
                                        lng = float(location.split(",", 1)[1])
                                        result_dict.setdefault(name, []).append([lat, lng])
                                        # print(result_dict)
                                    # else:
                                    #     # print("%s找不到物理地址" % ap_mac)
        else:
            print("%s文件不存在!" % ap_path)
        q.task_done()
    q2.put(result_dict)
    # print("线程结束")


if __name__ == '__main__':
    filename = 'personal_line.html'
    #
    startTime = input("请输入查询的开始时间（格式：2020-11-22 08:00:00）：")
    endTime = input("请输入查询的结束时间（格式：2020-11-23 23:00:00）：")
    html_path = "html/"
    start = time.time()
    lat_lng_dict = work(startTime, endTime)
    png_path = []
    for i in lat_lng_dict:
        draw_gps(i, lat_lng_dict[i], html_path, png_path)
    png_to_gif(png_path, duration=800)

    end = time.time()
    print('程序执行时间(s)： ', end - start)
