
from __future__ import unicode_literals
from threading import Timer
from wxpy import *
import requests
bot = None
def get_news1():
    #获取金山词霸每日一句，英文和翻译
    url = "http://open.iciba.com/dsapi/"
    r = requests.get(url)
    print(r.json())
    contents = r.json()['content']
    translation = r.json()['translation']
    return contents,translation
def login_wechat():
    
    global bot
    bot = Bot()
    # bot = Bot(console_qr=2,cache_path="botoo.pkl")#Linux专用，像素二维码
 
def send_news():
    if bot == None:
        login_wechat()
    try:
        my_friend = bot.groups().search(["1"],["2"])[0]   #你朋友的微信名称，不是备注，也不是微信帐号。
        # my_friend = bot.groups().search(u'测试')[0]    #你群的微信名称，不是备注，也不是微信帐号。
        my_friend.send(get_news1()[0])
        my_friend.send(get_news1()[1][5:])
        t = Timer(86400, send_news) #每86400秒（1天），发送1次，不用linux的定时任务是因为每次登陆都需要扫描二维码登陆，很麻烦的一件事，就让他一直挂着吧
        t.start()
    except:
        print(u"今天消息发送失败了")
if __name__ == "__main__":
    send_news()
    print(get_news1()[0])
