# 这里的作用通过手机号码获取用户id还有向相应用户发送信息


from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from models import User
import time
import requests
import json
import re


# 获取钉钉的access_token
def get_access_token(appkey,appsecret):
    res=requests.get('https://oapi.dingtalk.com/gettoken?appkey={}&appsecret={}'.format(appkey,appsecret))
    access_token=re.findall('"access_token":"(.*?)"',res.text,re.S)
    return access_token

# 通过access_token,moblie获取用户userid
def get_userid(access_token,moblie):
    res = requests.get('https://oapi.dingtalk.com/user/get_by_mobile?access_token={}&mobile={}'.format(access_token,moblie))
    userid = re.findall('"userid":"(.*?)"',res.text,re.S)
    return userid

# 通过access_token,agent_id,user_list向用户发送msg
def send_msg(access_token,agent_id,user_list,msg):
    print(access_token)
    print(agent_id)
    print(user_list)
    res =requests.post('https://oapi.dingtalk.com/topapi/message/corpconversation/asyncsend_v2?access_token={}'.format(access_token),
                       data={
                           "agent_id":agent_id,
                           "userid_list": user_list,
                           "msg":msg,

                       })


# 监视数据库的脚本(每5秒钟从数据库查询一次)
def send_message():
    while True:
        time.sleep(5)
        cursor = conn.cursor()
        cursor.execute('select phone_code,card_consume,card_balance,dCollectTime,card_oldbalance,MealRecords.card_id from Employee RIGHT JOIN MealRecords on Employee.emp_id = MealRecords.emp_id where MealRecords.is_send is null;')
        res = cursor.fetchall()
        print(res)
        if res:
            for i in res:
                print(i)
                moblie = i[0]
                if moblie:
                    user_id = get_userid(access_token, moblie)[0]
                    msg = json.dumps({"msgtype": "text", "text": {"content": "消费时间:{},\n消费金额:{}元,\n剩余金额:{}元.".format(i[3],i[1],i[2])}})
                    send_msg(access_token,agent_id,user_id,msg)
                cursor.execute('update MealRecords set is_send = 1 where MealRecords.card_id=%s and MealRecords.card_oldbalance=%s',(i[5],i[4]))
        conn.commit()
        cursor.close()

if __name__ == '__main__':
    appkey = ''     # 钉钉的appkey
    appsecret = ''  # 钉钉的appsecret
    agent_id =      # 钉钉的agent_id
    access_token = get_access_token(appkey, appsecret)[0]
    engine = create_engine("mssql+pymssql://sqlserver账号:sqlsever密码/数据库名字", max_overflow=0, pool_size=5) #数据库连接
    conn = engine.raw_connection()
    send_message()