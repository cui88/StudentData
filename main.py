# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import thermodynamic
import get_lat_lng
import time
import datetime
def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # thermodynamic.GetLocationDict()
    str2 = '2020-11-22 09:25:01'
    str1 = '2020-11-22 09:21:04'
    startDateTime = datetime.datetime.strptime(str1, '%Y-%m-%d %H:%M:%S')
    endDateTime = datetime.datetime.strptime(str2, '%Y-%m-%d %H:%M:%S')
    print((endDateTime - startDateTime).total_seconds())
    # get_lat_lng.getdata()
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
