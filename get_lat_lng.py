import json
from urllib.request import urlopen
from urllib.parse import quote

def getdata():
    url = 'http://api.map.baidu.com/geocoder/v3/'
    output = 'json'
    ak = 'AhLF63hKP8iXpMvXSiHjGhXsaEByvoKY'
    a = ['北京', '天津', '石家庄', '太原', '呼和浩特', '沈阳', '大连', '长春', '哈尔滨', '上海', '南京', '杭州', '宁波', '合肥', '福州', '厦门', '南昌', '济南',
         '青岛', '郑州', '武汉', '长沙', '广州', '深圳', '南宁', '海口', '重庆', '成都', '贵阳', '昆明', '拉萨', '西安', '兰州', '西宁', '银川', '乌鲁木齐']
    for i in a:
        add = quote(i)
        uri = url + '?' + 'address=' + add + '&output=' + output + '&ak=' + ak  # 百度地理编码API
        req = urlopen(uri)
        res = req.read().decode("utf-8")
        print(res)
        if res is None or res == '':
            print('I got a null or empty string value for data in a file')
        else:
            temp = json.loads(res,strict=False)
            print(temp['location']['lng'], temp['location']['lat'])  # 打印出经纬度

    address = '北京市'
    url = 'http://api.map.baidu.com/geocoder?output=json&key=f247cdb592eb43ebac6ccd27f796e2d2&address=' + str(address)
    response = requests.get(url)
    answer = response.json()
    lon = float(answer['result']['location']['lng'])
    lat = float(answer['result']['location']['lat'])