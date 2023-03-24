# -*- coding: utf-8 -*-
# 引入依赖包
from dataclasses import replace
import cx_Oracle
import os
import datetime
import paramiko
import sys
import time
from datetime import date, timedelta
# 配置属性
config = {
    # 代码根目录
    # 'local_path': '/Users/happyliu/mycode/my_python',
    'local_path': '/u02',
    # 项目文件夹名称
    'project_name': "RMS",
    # 获取近:modify_day 天前变化的数据
    # 'modify_day': 9999,
    'modify_day': 1,
    # 项目地址
    'git_site': 'ssh://git@123.123.123.123:9922/cpgroup/rms.git'
}
##############################################################################################################################
rms_ssh_config = {
    'host': '123.123.123.123',
    'port': 22,
    'username': 'xxxxx',
    'password': 'xxxxx',
    'remote_path': '',
    'local_path': '',
    'remote_ssh_root_path': '/u01/app/abc/oracle/proc/'}

##############################################################################################################################

rms_db_config = {
    'schema_name': "abc",
    'username': 'abc',
    'password': 'abc',
    'encoding': "utf8",
    'host': '123.123.123.123',
    'port': 1521,
    'sid': None,
    'service_name': 'abc'

}


hub_db_config = {
    'schema_name': "HUB",
    'username': 'abc',
    'password': 'abc',
    'encoding': "utf8",
    'host': '123.123.123.123',
    'port': 1521,
    'sid': None,
    'service_name': 'abc'
}
##############################################################################################################################


####
modify_day = config['modify_day']
# 代码根目录
local_path = config['local_path']
# 项目文件夹名称
project_name = config['project_name']
# 项目本地完整路径
code_file_path = os.path.join(local_path, project_name)
git_site = config['git_site']

# 初始化
git_commit_log = []
# extend_list = ["", "ksh", "ctl", "sh", "sql", "lst", "pc", "lis", "mk"]
extend_list = ["ctl", "ksh",  "sh", "sql", "pc", "lis", "mk"]


##############################################################################################################################

def Log(msg, line, name):
    # 文件地址  __file__，可选添加
    date = time.strftime('%Y.%m.%d %H:%M:%S ', time.localtime(time.time()))
    print(date+':' + msg + ', Line '+line+' , in '+name)


class OracleHelper(object):
    conn = None

    def __init__(self, username, password, host, sid=None,  service_name=None, port=1521, encoding='utf8',   region=None, sharding_key=None, super_sharding_key=None):

        self.username = username
        self.password = password
        self.encoding = encoding
        try:
            if sid != None:
                self.dsn = cx_Oracle.makedsn(host=host, port=port, sid=sid)
            elif service_name != None:
                self.dsn = cx_Oracle.makedsn(
                    host, port, service_name=service_name)
            else:
                print("SID 或 service_name 不能都为空 ")
        except Exception as e:
            print(e)
        print('连接数据OK')

    def connect(self):
        self.conn = cx_Oracle.connect(
            user=self.username, password=self.password, dsn=self.dsn,  encoding=self.encoding)
        self.cursor = self.conn.cursor()

    def close(self):
        self.cursor.close()
        self.conn.close()

    def get_one(self, sql, params=()):
        result = None
        try:
            self.connect()
            self.cursor.execute(sql, params)
            result = self.cursor.fetchone()
            self.close()
        except Exception as e:
            print(e)
        return result

    def get_all(self, sql, params=()):
        list_data = ()
        try:
            self.connect()
            self.cursor.execute(sql, params)
            list_data = self.cursor.fetchall()
            self.close()
        except Exception as e:
            print(e)
        return list_data

    def insert(self, sql, params=()):
        return self.__edit(sql, params)

    def update(self, sql, params=()):
        return self.__edit(sql, params)

    def delete(self, sql, params=()):
        return self.__edit(sql, params)

    def __edit(self, sql, params):
        count = 0
        try:
            self.connect()
            count = self.cursor.execute(sql, params)
            self.conn.commit()
            self.close()
        except Exception as e:
            print(e)
        return count


##############################################################################################################################

# 创建文件夹
def mkdir(path):
    # 去除首位空格
    path = path.strip()
    # 去除尾部 \ 符号
    path = path.rstrip("\\")
    # 判断路径是否存在
    # 存在     True
    # 不存在   False
    isExists = os.path.exists(path)
    # 判断结果
    if not isExists:
        # 如果不存在则创建目录
        # 创建目录操作函数
        os.makedirs(path)
        # print(path + ' 创建成功')
        return True
    else:
        # 如果目录存在则不创建，并提示目录已存在
        # print(path + ' 目录已存在')
        return False


##############################################################################################################################
# 获取当前时间
"""
    ds_str = get_datetime_str()
    print(ds_str)
    ds_str = get_datetime_str('time')
    print(ds_str)
"""


def get_datetime_str(style='dt'):
    cur_time = datetime.datetime.now()

    date_str = cur_time.strftime('%Y%m%d')
    time_str = cur_time.strftime('%H%M%S')

    if style == 'date':
        return date_str
    elif style == 'time':
        return time_str
    else:
        return date_str + '_' + time_str


##############################################################################################################################
# 获取Oracle变化的程序 及 生成程序文件

def get_file(db_config):
    # DB信息
    schema_name = db_config['schema_name']
    username = db_config['username']
    password = db_config['password']
    encoding = db_config['encoding']
    host = db_config['host']
    port = db_config['port']
    sid = db_config['sid']
    service_name = db_config['service_name']

    # 扩展名：
    file_extend = {'FUNCTION': 'fnc', 'TRIGGER': 'trg',
                   'PACKAGE': 'pck', "PROCEDURE": 'prc', 'VIEW': 'sql'}

    # 获取 View的配信息
    sql_get_view_ddl = """ 
    SELECT 
      O.OWNER,
       O.OBJECT_NAME,
       CEIL(TEXT_LENGTH / 4000) SEQ,
       O.CREATED,
       O.LAST_DDL_TIME,
       O.STATUS,
       T.TEXT_LENGTH
  FROM ALL_OBJECTS O
  LEFT JOIN DBA_VIEWS T
    ON O.OWNER = T.OWNER
   AND O.OBJECT_NAME = T.VIEW_NAME
 WHERE O.OWNER = :OWNER
   AND O.OBJECT_TYPE = 'VIEW'
   AND O.LAST_DDL_TIME >= TRUNC(SYSDATE) - :day 
  order by O.LAST_DDL_TIME desc 
   
    """

    
    sql_get_view_script = """
    SELECT CMX_LONG_HELP.GET_LONG_STR(:sql,:SEQ) NAME
  FROM DUAL
    
    """

    # 查询 有变动的对象
    sql1 = """SELECT  DECODE(OBJECT_TYPE,
                      'PACKAGE BODY',
                      'PACKAGE',
                      OBJECT_TYPE) OBJECT_TYPE,
              OBJECT_NAME
              ,max(LAST_DDL_TIME) LAST_DDL_TIME
FROM ALL_OBJECTS T
WHERE 1 = 1
  AND OBJECT_TYPE IN ('FUNCTION'
                      -- ,'JAVA SOURCE'
                      --,'LIBRARY'
                    ,
                      'PACKAGE',
                      'PACKAGE BODY',
                      'PROCEDURE',
                      'TRIGGER'
                      --,'TYPE','TYPE BODY'
                      )
  AND LAST_DDL_TIME >= TRUNC(SYSDATE) - :day
 -- and :day is not null 
  AND OWNER = :OWNER
group by 
DECODE(OBJECT_TYPE,
    'PACKAGE BODY',
    'PACKAGE',
    OBJECT_TYPE) ,
     OBJECT_NAME


ORDER BY 1,
        2 """

    sql_code = """
    SELECT TEXT
  FROM (SELECT '1' SEQ,
               LINE AS LINE,
               CASE
                 WHEN LINE = 1 THEN
                  'CREATE OR REPLACE ' || TEXT
                 ELSE
                  TEXT
               END AS TEXT --,ascii(TEXT)
          FROM ALL_SOURCE T
         WHERE NAME = :OBJECT_NAME
           AND TYPE = :OBJECT_TYPE
           AND OWNER = :OWNER
        UNION ALL
        SELECT '2' SEQ,
               0 LINE,
               '/' || CHR(10) TEXT
          FROM DUAL
         WHERE :OBJECT_TYPE IN ('PACKAGE')
        UNION ALL
        SELECT '3' SEQ,
               LINE AS LINE,
               CASE
                 WHEN LINE = 1 THEN
                  'CREATE OR REPLACE ' || TEXT
                 ELSE
                  TEXT
               END AS TEXT
          FROM ALL_SOURCE T
         WHERE NAME = :OBJECT_NAME
           AND TYPE = (DECODE(:OBJECT_TYPE,'PACKAGE','PACKAGE BODY', NULL))
           AND OWNER = :OWNER
        UNION ALL
        SELECT '4' SEQ,
               0 LINE,
               '/' || CHR(10) TEXT
          FROM DUAL
         WHERE 1=1
        -- and :OBJECT_TYPE IN ('PACKAGE')
         ORDER BY SEQ,
                  LINE) T

 
 
 """

    oracledb = OracleHelper(username=username, password=password, host=host,
                            sid=sid, service_name=service_name, port=port,  encoding=encoding)

    list_para = [modify_day, schema_name]
    list_objects = oracledb.get_all(sql1, list_para)
    Log('返回查询结果的数量:'+ str(len(list_objects)), str(sys._getframe().f_lineno), sys._getframe().f_code.co_name)

    # 先将文件夹创建好。
    
    for object_type_key in file_extend:
        file_path = os.path.join(code_file_path, schema_name, object_type_key)
        Log('创建文件夹：'+ file_path, str(sys._getframe().f_lineno), sys._getframe().f_code.co_name)
        mkdir(file_path)

    # 开始循环写入文件
    for rec in list_objects:
        object_type = rec[0]
        object_name = rec[1]
        LAST_DDL_TIME = rec[2]
        list_params = [object_name, object_type, schema_name]

        # 文件名：
        file_path = os.path.join(code_file_path, schema_name, object_type)
        file_name = os.path.join(file_path, str(
            object_name)+'.' + file_extend[object_type])

        git_commit_log.append("对象名：{}.{}    {} 修改时间{}".format(
            schema_name,  object_name, object_type, LAST_DDL_TIME))

        list_code_results = oracledb.get_all(sql_code, list_params)
        Log('开始写入文件：'+ rec[1], str(sys._getframe().f_lineno), sys._getframe().f_code.co_name)

        with open(file_name, encoding="utf-8", mode="w") as f:
            for result in list_code_results:
                f.write(str(result[0]))

    # 获取 视图  # ,modify_day
    list_view_para = [schema_name,modify_day]
    list_view_objects = oracledb.get_all(sql_get_view_ddl, list_view_para)
    try:
        Log('返回修改视图的数量:'+ str(len(list_view_objects)), str(sys._getframe().f_lineno), sys._getframe().f_code.co_name)

        for rec in list_view_objects:
            # print('视图名:' + rec[1] +  "SEQ: "+ str(rec[2]) )
            object_type = "VIEW"
            owner = rec[0]
            object_name = rec[1]
            LAST_DDL_TIME = rec[4]
            seq = rec[2]
            i =1
            while(i<=seq):
                # 文件名：
                file_path = os.path.join(
                    code_file_path, schema_name, object_type)
                file_name = os.path.join(file_path, str(
                    object_name)+'.' + file_extend[object_type])
                print("文件路径："+file_name + "; SEQ: "+ str(seq))
                git_commit_log.append("对象名：{}.{}    {} 修改时间{}".format(
                    schema_name,  object_name, object_type, LAST_DDL_TIME))
                # 查看代码
                v_sql2 = """ SELECT  TEXT
    FROM DBA_VIEWS T
    WHERE T.OWNER = ':OWNER'
    AND VIEW_NAME = ':OBJECT_NAME'
    """
                v_sql2 = v_sql2.replace(':OWNER', schema_name)
                v_sql2 = v_sql2.replace(':OBJECT_NAME', object_name)
                # list_view_params = [schema_name, object_name, i]
                list_view_params = [v_sql2, i]

                list_view_code_results = oracledb.get_all(
                    sql_get_view_script, list_view_params)

                l_str = "CREATE OR REPLACE VIEW "+ object_name +" AS \n"  
                if i==1 :
                    with open(file_name, encoding="utf-8", mode="w") as f:
                        for result in list_view_code_results:
                            f.write(l_str+str(result[0]))
                else:
                    with open(file_name, encoding="utf-8", mode="a") as f:
                        for result in list_view_code_results:
                            f.write(str(result[0]))

                i=i+1

    except IOError:
        print("Error: 没有找到文件或读取文件失败")
    else:
        print(schema_name+"数据库写完成")


def get_db_file():
    # 获取RMS变化文件
    get_file(rms_db_config)
    get_file(hub_db_config)
    # print("\n".join(git_commit_log))
##############################################################################################################################
# 获取Batch文件


def RemoteScp(ssh_confg):
    host_ip = ssh_confg['host']
    host_port = ssh_confg['port']
    host_username = ssh_confg['username']
    host_password = ssh_confg['password']
    remote_path = ssh_confg['remote_path']
    local_path = ssh_confg['local_path']

    # 创建文件夹
    mkdir(local_path)

    print("登陆远程服务器："+remote_path)
    print("local_path:"+local_path)
    scp = paramiko.Transport((host_ip, host_port))
    scp.connect(username=host_username, password=host_password)
    sftp = paramiko.SFTPClient.from_transport(scp)

    try:
        # sftp.listdir_attr
        # remote_files = sftp.listdir(remote_path)
        # print(remote_files)
        # print('remote_files {}  local_path {} '.format(remote_path, local_path))
        files = sftp.listdir_attr(remote_path)
        for f in files:
            t1 = datetime.datetime.fromtimestamp(f.st_mtime)
            t2 = datetime.datetime.now()
            # 修改时间与 当前时间 相差的天数
            # print('文件名 {}  开始时间 {} 结束时间 {}   相差天数 {}'.format(f.filename, t1.strftime('%Y-%m-%d %H:%M:%S'), t2.strftime('%Y-%m-%d %H:%M:%S'), (t2 - t1).days))
            if (t2 - t1).days <= modify_day:
                # print('文件名222 {}  扩展名 {} 开始时间 {} 结束时间 {}   相差天数 {}'.format(f.filename, os.path.splitext(f.filename)[-1][1:]  ,t1.strftime('%Y-%m-%d %H:%M:%S'), t2.strftime('%Y-%m-%d %H:%M:%S'), (t2 - t1).days))
                file_ex = os.path.splitext(f.filename)[-1][1:]
                if file_ex.lower() in extend_list:
                    # print('文件名3333 {}  开始时间 {} 结束时间 {}   相差天数 {}'.format(f.filename, t1.strftime('%Y-%m-%d %H:%M:%S'), t2.strftime('%Y-%m-%d %H:%M:%S'), (t2 - t1).days))
                    local_file = os.path.join(local_path, f.filename)
                    remote_file = os.path.join(remote_path, f.filename)
                    sftp.get(remote_file, local_file)
                    git_commit_log.append("{}: {} {}".format(
                        f.filename, f.st_size, t1.strftime('%Y-%m-%d %H:%M:%S')))
                # if os.path.splitext(f.filename)[-1][1:] in extend_list:
                #     print('文件名3333 {}  开始时间 {} 结束时间 {}   相差天数 {}'.format(f.filename, t1.strftime(
                # '%Y-%m-%d %H:%M:%S'), t2.strftime('%Y-%m-%d %H:%M:%S'), (t2 - t1).days))
                #     git_commit_log.append(
                #         "{}: {} {}".format(f.filename, f.st_size, t1.strftime('%Y-%m-%d %H:%M:%S')))
                #     # if f.filename != "bk":
                #     local_file = local_path + f.filename
                #     remote_file = remote_path + f.filename
                #     print('复制文件： local_file {}  remote_file {} '.format( local_file,remote_file))
                #     sftp.get(remote_file, local_file)
                    # if not os.path.exists(local_file):
                    #     print('复制文件： local_file {}  remote_file {} '.format( local_file,remote_file))
                    #     sftp.get(remote_file, local_file)
    except IOError:  # 如果目录不存在则抛出异常
        return ("remote_path or local_path is not exist")
    scp.close()


def get_batch_file():
    # 本地目录 主目录+系统+  例如：
    # RMS根目录/RMS/BATCH/bin
    # RMS根目录/RMS/BATCH/src
    rms_ssh_config['local_path'] = os.path.join(
        code_file_path, "RMS/BATCH/bin/")
    rms_ssh_config['remote_path'] = os.path.join(
        rms_ssh_config['remote_ssh_root_path'], 'bin')
    RemoteScp(rms_ssh_config)

    rms_ssh_config['local_path'] = os.path.join(
        code_file_path, "RMS/BATCH/src/")
    rms_ssh_config['remote_path'] = os.path.join(
        rms_ssh_config['remote_ssh_root_path'], 'src')
    RemoteScp(rms_ssh_config)

    # Hub代码目录
    hub_ssh_config['local_path'] = os.path.join(
        code_file_path, "HUB/BATCH/shell/")
    hub_ssh_config['remote_path'] = os.path.join(
        hub_ssh_config['remote_ssh_root_path'], 'shell')
    RemoteScp(hub_ssh_config)

    hub_ssh_config['local_path'] = os.path.join(
        code_file_path, "HUB/BATCH/ftp/")
    hub_ssh_config['remote_path'] = os.path.join(
        hub_ssh_config['remote_ssh_root_path'], 'ftp')
    RemoteScp(hub_ssh_config)

    # 远程目录
    # hub_ssh_config['local_path']= os.path.join(code_file_path, "RMS/BATCH/src")
    # hub_ssh_config['remote_path']

    # RemoteScp(hub_ssh_config)


def git_clone():
    cmd = "git clone --depth=1 -b main "+git_site + " "+project_name

    print(cmd)
    result = os.system(cmd)
    print(result)


def git_restore():

    cmd = "git fetch --all"
    print(cmd)
    result = os.system(cmd)
    print(result)

    cmd = "git reset --hard origin/main"
    print(cmd)
    result = os.system(cmd)
    print(result)
    cmd = "git pull"
    print(cmd)
    result = os.system(cmd)
    print(result)

    # cmd = "git reset HEAD"
    # print(cmd)
    # result = os.system(cmd)
    # print(result)

    # # 删除未 add的文件及目录
    # cmd = "git clean -fd"
    # print(cmd)

    # result = os.system(cmd)
    # print(result)

    # # 将修改的文件 进行还原
    # cmd = "git restore ."
    # print(cmd)
    # result = os.system(cmd)
    # print(result)

    # 删除未跟踪的文件及目录


def git_commit():
    # 添加代码
    cmd = "git add ."
    result = os.system(cmd)
    # 提交代码

    cmd = """git commit -m  " """+get_datetime_str('dt')+" 代码自动保存 " + """
    """+"\n".join(git_commit_log) + " \" "

    # print(commit_log_info)
    # cmd = "git commit -m '" + commit_log_info+"'"
    # print(cmd)
    result = os.system(cmd)


def git_push():
    cmd = "git push origin main"
    result = os.system(cmd)
    # print(result)


if __name__ == '__main__':
    print("切换工作目录")
    os.chdir(local_path)

    print("创建目录")
    mkdir(code_file_path)

    print("Clone 代码")
    git_clone()

    Log('切换工作目录  ', str(sys._getframe().f_lineno), sys._getframe().f_code.co_name)

    os.chdir(code_file_path)

    Log('撤销修改  ', str(sys._getframe().f_lineno), sys._getframe().f_code.co_name)
    git_restore()

    # get_db_file()
    Log('1.1 获取 rms_db_config 文件 ', str(sys._getframe().f_lineno),
        sys._getframe().f_code.co_name)
    get_file(rms_db_config)



    Log('1.2 获取 hub_db_config 文件 ', str(sys._getframe().f_lineno),
        sys._getframe().f_code.co_name)
    get_file(hub_db_config)

    # get_batch_file()
    Log('2.1 获取 RMS BATCH  BIN 文件 ', str(sys._getframe().f_lineno),
        sys._getframe().f_code.co_name)

    rms_ssh_config['local_path'] = os.path.join(
        code_file_path, "RMS/BATCH/bin/")
    rms_ssh_config['remote_path'] = os.path.join(
        rms_ssh_config['remote_ssh_root_path'], 'bin')
    RemoteScp(rms_ssh_config)

    Log('2.2 获取 RMS BATCH SRC 文件 ', str(sys._getframe().f_lineno),
        sys._getframe().f_code.co_name)

    print("获取 RMS batch文件")
    rms_ssh_config['local_path'] = os.path.join(
        code_file_path, "RMS/BATCH/src/")
    rms_ssh_config['remote_path'] = os.path.join(
        rms_ssh_config['remote_ssh_root_path'], 'src')
    RemoteScp(rms_ssh_config)

    # Hub代码目录
    Log('2.3 获取 HUB BATCH shell 文件 ', str(sys._getframe().f_lineno),
        sys._getframe().f_code.co_name)
    hub_ssh_config['local_path'] = os.path.join(
        code_file_path, "HUB/BATCH/shell/")
    hub_ssh_config['remote_path'] = os.path.join(
        hub_ssh_config['remote_ssh_root_path'], 'shell')
    RemoteScp(hub_ssh_config)

    Log('2.4 获取 HUB BATCH ftp 文件 ', str(sys._getframe().f_lineno),
        sys._getframe().f_code.co_name)
    hub_ssh_config['local_path'] = os.path.join(
        code_file_path, "HUB/BATCH/ftp/")
    hub_ssh_config['remote_path'] = os.path.join(
        hub_ssh_config['remote_ssh_root_path'], 'ftp')
    RemoteScp(hub_ssh_config)

    Log('2.5 获取 HUB BATCH ctl 文件 ', str(sys._getframe().f_lineno),
        sys._getframe().f_code.co_name)
    hub_ssh_config['local_path'] = os.path.join(
        code_file_path, "HUB/BATCH/ctl/")
    hub_ssh_config['remote_path'] = os.path.join(
        hub_ssh_config['remote_ssh_root_path'], 'inbound')
    hub_ssh_config['remote_path'] = os.path.join(
        hub_ssh_config['remote_path'], 'ctl')
    RemoteScp(hub_ssh_config)

    Log('代码提交  ', str(sys._getframe().f_lineno), sys._getframe().f_code.co_name)
    git_commit()

    Log('代码push  ', str(sys._getframe().f_lineno), sys._getframe().f_code.co_name)
    git_push()
