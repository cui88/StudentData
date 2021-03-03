# -*- coding:utf-8 -*-
# Author:pannian
import os
import sys
import pdb
import logging
from ftplib import FTP
import queue

Done = 'done'
Error = 'error'
Empty = 'empty'
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler("log.txt")
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

console = logging.StreamHandler()
console.setLevel(logging.INFO)

logger.addHandler(handler)
logger.addHandler(console)


class MyFtp:
    ftp = FTP()
    ftp.set_debuglevel(2)
    file_flag = '/'
    year_month_dict = {}
    q = queue.Queue()
    user_dict = {}

    def __init__(self, host, year_month_dict, q, user_dict, port=21):
        self.ftp.connect(host, port)
        self.year_month_dict = year_month_dict
        self.q = q
        self.user_dict = user_dict

    # 登录
    def Login(self, user, passwd):
        self.ftp.login(user, passwd)
        print(self.ftp.welcome)

    def DownLoadFile(self, LocalFile, RemoteFile):
        print("远程文件:", RemoteFile)
        if os.path.exists(LocalFile):
            return Done
        file_handler = open(LocalFile, 'wb')
        print(file_handler)
        try:
            self.ftp.retrbinary('RETR ' + RemoteFile, file_handler.write)  # 接收服务器上文件并写入本地文件
        except IOError as e:
            logger.error("Failed file:" + LocalFile + " %s: %s" % (e.errno, e.strerror))
            file_handler.close()
            return Done
        else:
            print("Successful file:" + LocalFile)
            file_handler.close()
            return Error

    def GetFileDirList(self, path):
        file_list = os.listdir(path)
        return file_list

    # 下载整个目录下的文件
    def DownLoadApFile(self, LocalDir, RemoteDir):
        print("远程文件夹:", RemoteDir)
        if not os.path.exists(LocalDir):
            os.makedirs(LocalDir)

        # 打开该远程目录
        # 获取该目录下所有文件名，列表形式
        # print("远程目录")
        # print(RemoteNames)
        # for file in RemoteNames:
        #     Local = os.path.join(LocalDir, file)
        #     Updated = self.ftp.pwd() + self.file_flag + file
        #     print("本地地址：" + Local)
        #     print("file：" + file)
        #     try:
        #         self.ftp.cwd(file)
        #     except:  # 捕捉到异常，为文件
        #         print('当前远程服务器工作目录：' + self.ftp.pwd())
        #         print("DownLoadFile start!")
        #         self.DownLoadFile(Local, file)
        #     else:  # 没有捕捉到异常,确实为文件夹
        #         if not os.path.exists(Local):
        #             os.makedirs(Local)
        #         self.DownLoadFileTree(Local, Updated)
        #         self.ftp.cwd('..')
        self.ftp.cwd(RemoteDir)
        remoteDirNames = self.ftp.nlst()
        localDirNames = GetFileDirList(LocalDir)
        for key in self.year_month_dict:
            local_dir = os.path.join(LocalDir, key)
            remote_dir = os.path.join(RemoteDir, key)
            # 判断是否是压缩包文件目录
            if key in remoteDirNames:  # 文件夹
                # 如果文件在本地不存在，则创建
                file_list = self.year_month_dict[key]
                self.ftp.cwd(key)
                if not os.path.exists(local_dir):
                    os.makedirs(local_dir)
                remoteNames = self.ftp.nlst()
                regex = re.compile(r"\d+\_\d+")
                for file in remoteNames:
                    result = re.findall(regex, file)[0]
                    if result in file_list:  # 文件需要下载
                        remoteFile = os.path.join(remote_dir, file)
                        localFile = os.path.join(local_dir, file)
                        if self.DownLoadFile(localFile, remoteFile).strip() == Done:
                            self.q.put(localFile)
                self.ftp.cwd('..')
            else:  # 压缩包
                print("%s为压缩文件" % key)
                #     #下载压缩包并解压
                # file_zip = key + ".tar"
                # if file_zip not in localDirNames:
                #    remoteFile = os.path.join(RemoteDir,file_zip)
                #    localFile = os.path.join(LocalDir,file_zip)
                #    self.DowLoadFile(LocalDir,remoteFile)
        return

    def DowloadUserFile(self, LocalDir, RemoteDir):
        file = "UserMac" + time.strftime("%Y%m%d") + ".csv"
        localFile = os.path.join(LocalDir, file)
        remoteFile = os.path.join(RemoteDir, file)
        if self.DownLoadFile(localFile, remoteFile).strip() == Done:
            self.user_dict = self.GetUserDict(localFile)

    def GetUserDict(self, path):
        user_id_pd = pd.read_csv(path, na_values='NAN', encoding='utf_8_sig')
        csvDict = self.GetDict(user_id_pd, key_name='Device_Mac', value_name='UserID')
        return csvDict

    def GetDict(self, df, key_name, value_name):
        csvDict = {}
        for row in df.itertuples():
            csvDict[getattr(row, key_name)] = getattr(row, value_name)
        return csvDict
