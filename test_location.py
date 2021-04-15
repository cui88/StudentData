import pandas as pd
import json
import requests

dataPath = "data/xuehao-mac/"
rj_location_file = 'rj.xlsx'
h3_location_file = 'h3c.xls'
other_location_file = 'other_place.xls'
university = '长沙市湖南大学'


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
    print(other_location_pd.head())
    other_location_pd = other_location_pd[['AP_Mac', 'Location']]  # 只提取这两列
    other_location_pd['AP_Mac'] = other_location_pd['AP_Mac'].str.lower()
    return other_location_pd

def getLocationDict():
    location_df = pd.concat([Get_RJFile(), Get_h3File(),Get_otherplaceFile()])
    csvDict = getDict(location_df, key_name='AP_Mac', value_name='Location')
    return csvDict


def getDict(df, key_name, value_name):
    csvDict = {}
    for row in df.itertuples():
        csvDict[getattr(row, key_name)] = getattr(row, value_name)
    return csvDict


# def get_location1(data, city):
#     my_ak = 'AhLF63hKP8iXpMvXSiHjGhXsaEByvoKY'  # 需要自己填写自己的AK
#     tag = urp.quote('地铁站')  # URL编码
#     data['lat_lon'] = ''
#     for i in range(len(data)):
#         if not pd.isnull(data['location'][i]):
#             name = university + data['location'][i]
#             query = urp.quote(name)
#             try:
#                 url = 'http://api.map.baidu.com/place/v2/search?query=' + query + '&tag=' + '&region=' + urp.quote(
#                     city) + '&output=json&ak=' + my_ak
#                 req = request.urlopen(url)
#                 res = req.read().decode()
#                 lat = pd.to_numeric(re.findall('"lat":(.*)', res)[0].split(',')[0])
#                 lng = pd.to_numeric(re.findall('"lng":(.*)', res)[0])
#                 print(data['location'][i])
#                 data['lat_lon'][i] = [lat,lng] # 纬度和经度
#                 print(data['lat_lon'][i])
#             except Exception as e:
#                 print(e.args)
#         else:
#             print("Location is empty")
#     df.to_excel('./location.xlsx', index=False, encoding='utf-8')


def get_location_x_y():
    url = 'https://restapi.amap.com/v3/geocode/geo?parameters'
    location_dict = getLocationDict()
    df = pd.DataFrame.from_dict(location_dict, orient='index', columns=['location'])[['location']]
    df = df.drop_duplicates()
    df['lat_lon'] = ''
    for i in range(len(df)):
        parameters = {
            'key': '7965603a8aba9e199eb14d80d5f2073f',
            'address': university + df['location'][i]
        }
        page_resource = requests.get(url, params=parameters, verify=False)
        text = page_resource.text  # 获得数据是json格式
        data = json.loads(text)  # 把数据变成字典格式
        location = data["geocodes"][0]['location']
        print(location)
        df['lat_lon'][i] = location
    df.to_excel('./location.xlsx', index=False, encoding='utf-8')


if __name__ == '__main__':
    # location_dict = getLocationDict()
    # df = pd.DataFrame.from_dict(location_dict, orient='index', columns=['location'])
    # df = df.reset_index().rename(columns={'ap_mac': 'location'})
    # get_location1(df, '长沙')
    get_location_x_y()