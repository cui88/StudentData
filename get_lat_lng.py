import json
import requests


def get_location_x_y():
    headers = {
        'Connection': 'close',
        "user-agent": "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36"
    }
    url = 'https://restapi.amap.com/v3/geocode/geo?parameters'

    parameters = {
        'key': '7965603a8aba9e199eb14d80d5f2073f',
        'address': '长沙市湖南大学天马公寓1区1栋'
    }
    page_resource = requests.get(url, params=parameters,verify=False)
    text = page_resource.text  # 获得数据是json格式
    data = json.loads(text)  # 把数据变成字典格式
    location = data["geocodes"][0]['location']
    print(location)
    return location
    # response = requests.get('https://lbs.amap.com/console/show/picker',headers = headers,timeout=30,verify=False)
    # print(response.text)

if __name__ == '__main__':
    get_location_x_y()


