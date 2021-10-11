#!/usr/bin/python
# -*- coding: UTF-8 -*-
from __future__ import division
from apscheduler.schedulers.blocking import BlockingScheduler
import datetime
import time
import pymysql
import xlwt
import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email.mime.base import MIMEBase
from email import encoders


host = ''
port = 
db = ''
user = ''
password = ''

sender = ''
receivers = []  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱

startTime = ''
endTime = ''    
regions = ["CN","US","ID"]

def start():
    # BlockingScheduler
    scheduler = BlockingScheduler()
    scheduler.add_job(job, 'cron', day_of_week='0-6', hour=1, minute=00)
    scheduler.start()

def job():
    #print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    # 今天日期
    today = datetime.date.today() 
    # 昨天时间
    yesterday = today - datetime.timedelta(days=1)
    print(str(yesterday))
    for region in regions:
        check_it(region)
    SendEmail()
    
    

# ---- 用pymysql 操作数据库
def get_connection():
    conn = pymysql.connect(host=host, port=port, db=db, user=user, password=password)
    return conn
    
    
def check_it(region): 
    # 今天日期
    today = datetime.date.today() 
    # 昨天时间
    yesterday = today - datetime.timedelta(days=1)
    # 昨天开始时间戳
    yesterday_start_time = int(time.mktime(time.strptime(str(yesterday), '%Y-%m-%d')))
    # 昨天结束时间戳
    yesterday_end_time = int(time.mktime(time.strptime(str(today), '%Y-%m-%d'))) - 1
    
    endTime = yesterday_end_time * 1000
    startTime = yesterday_start_time * 1000
    
    conn = get_connection()

    # 使用 cursor() 方法创建一个 dict 格式的游标对象 cursor
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    
    
    workbook = xlwt.Workbook(encoding = 'ascii')
    worksheet = workbook.add_sheet('关卡数据')


    
    #总注册
    cursor.execute("select count(distinct userId) as total from Statistics where ops = 'register' and region = '%s' "%(region))
    data = cursor.fetchone()
    allTotalRegisterNum = data['total']
    print("-- 总注册人数: %s " % (data['total']))
    #当日注册
    cursor.execute("select count(distinct userId) as total from Statistics where ops = 'register' and timestamps > %s and timestamps < %s and region = '%s'"%(startTime, endTime, region))
    data = cursor.fetchone()
    totalRegisterNum = data['total']
    print("-- 当日注册人数: %s " % (data['total']))
    if(totalRegisterNum == 0):
        return
    
    #登录 
    cursor.execute("select count(distinct userId) as total from Statistics where ops = 'level_start' and timestamps > %s and timestamps < %s and region = '%s'"%(startTime, endTime, region))
    data = cursor.fetchone()
    totalLoginNum = data['total']
    print("-- 当日登录人数(开始关卡): %s " % (data['total']))
    

    cursor.execute("select MAX(CAST(param2 as decimal(4,2))) as total from Statistics where ops = 'level_start' and timestamps > %s and timestamps < %s and region = '%s'"%(startTime, endTime, region))
    data = cursor.fetchone()
    maxLevel = data['total']
    print("-- 最远关卡: %s " % (data['total']))
    #worksheet.write(0, 0, maxLevel) # 不带样式的写入
    #workbook.save('formatting.xls') # 保存文件

    
  
    #关卡统计数据
    print("-- 开始关卡数据统计 -- ")
    worksheet.col(0).width = 3000
    worksheet.col(1).width = 3000
    worksheet.col(2).width = 3000
    worksheet.col(3).width = 3000
    worksheet.col(4).width = 3000
    worksheet.col(5).width = 3000
    worksheet.col(6).width = 3000
    worksheet.col(7).width = 3000
    worksheet.col(8).width = 3000
    worksheet.col(9).width = 3000
    worksheet.col(10).width = 3000
    worksheet.col(11).width = 3000
    worksheet.col(12).width = 3000
    worksheet.col(13).width = 3000
    worksheet.write(0, 1, "开始关卡人数") 
    worksheet.write(0, 2, "结束关卡人数") 
    worksheet.write(0, 3, "单关完成率") 
    worksheet.write(0, 4, "留存率（完成人数/注册总人数）") 
    worksheet.write(0, 5, "开始关卡次数") 
    worksheet.write(0, 6, "结束关卡次数") 
    worksheet.write(0, 7, "失败次数") 
    worksheet.write(0, 8, "SingleExplode使用次数") 
    worksheet.write(0, 9, "RegenField使用次数") 
    worksheet.write(0, 10, "FreeMove使用次数") 
    worksheet.write(0, 11, "AreaExpolde使用次数") 
    worksheet.write(0, 12, "看广告买步数次数") 
    worksheet.write(0, 13, "驻停人数") 
    
    cursor.execute("select param2 as level, count(*) as personNum from  (select a.* from Statistics a inner join (select userId,MAX(CAST(param2 as decimal(4,2))) param2 from Statistics where ops = 'level_start' group by userId)b on a.userId=b.userId and a.param2=b.param2 where ops = 'level_start' and region = '%s' ) temp group by temp.param2 order by CAST(param2 as decimal(4,2));"%(region))
    results = cursor.fetchall()
 
    for i in range(1, int(maxLevel)) :
        print("-- 统计第%s关 " % (i))
        cursor.execute("select count(distinct userId) as total from Statistics where ops = 'level_start' and param2 = %s and timestamps > %s and timestamps < %s and region = '%s'"%(i, startTime, endTime, region))
        data = cursor.fetchone()
        startPersonNum = data['total']
        #print("-- 开始第%s关人数: %s " % (i, data['total']))
        
        cursor.execute("select count(distinct userId) as total from Statistics where ops = 'level_end' and param2 = %s and timestamps > %s and timestamps < %s and region = '%s'"%(i, startTime, endTime, region))
        data = cursor.fetchone()
        finishPersonNum = data['total']
        #print("-- 完成第%s关人数: %s " % (i, data['total']))
        
        
        cursor.execute("select count(*) as total from Statistics where ops = 'level_start' and param2 = %s and timestamps > %s and timestamps < %s and region = '%s'"%(i, startTime, endTime, region))
        data = cursor.fetchone()
        startNum = data['total']
        #print("-- 开始第%s关次数: %s " % (i, data['total']))
        
        cursor.execute("select count(*) as total from Statistics where ops = 'level_end' and param2 = %s and timestamps > %s and timestamps < %s and region = '%s'"%(i, startTime, endTime, region))
        data = cursor.fetchone()
        finishNum = data['total']
        #print("-- 完成第%s关次数: %s " % (i, data['total']))
        
        
        cursor.execute("select count(*) as total from Statistics where ops = 'level_end' and param2 = %s and param3 = '%s' and timestamps > %s and timestamps < %s and region = '%s'"%(i,"Fail", startTime, endTime, region))
        data = cursor.fetchone()
        failNum = data['total']
        #print("-- 第%s关失败次数: %s " % (i, data['total']))
        
        cursor.execute("select count(*) as total from Statistics where ops = 'use_booster' and param3 = %s and param1 = '%s' and timestamps > %s and timestamps < %s and region = '%s'"%(i,"SingleExplode", startTime, endTime, region))
        data = cursor.fetchone()
        SingleExplodeNum = data['total']
        
        cursor.execute("select count(*) as total from Statistics where ops = 'use_booster' and param3 = %s and param1 = '%s' and timestamps > %s and timestamps < %s and region = '%s'"%(i,"FreeMove", startTime, endTime, region))
        data = cursor.fetchone()
        FreeMoveNum = data['total']
        
        cursor.execute("select count(*) as total from Statistics where ops = 'use_booster' and param3 = %s and param1 = '%s' and timestamps > %s and timestamps < %s and region = '%s'"%(i,"RegenField", startTime, endTime, region))
        data = cursor.fetchone()
        RegenFieldNum = data['total']
        
        cursor.execute("select count(*) as total from Statistics where ops = 'use_booster' and param3 = %s and param1 = '%s' and timestamps > %s and timestamps < %s and region = '%s'"%(i,"AreaExpolde", startTime, endTime, region))
        data = cursor.fetchone()
        AreaExpoldeNum = data['total']
        
        cursor.execute("select count(*) as total from Statistics where ops = 'ad_impression' and param1 = 'RewardVideoAds' and param2 = '%s' and param5 = %s and timestamps > %s and timestamps < %s and region = '%s'" %("add_step_5", i ,startTime, endTime, region))
        data = cursor.fetchone()
        AddStepNum = data['total']
        
        if(startPersonNum == 0):
            ratio = 0;
        else:
            ratio = finishPersonNum/startPersonNum
        remainRatio = finishPersonNum/totalRegisterNum
        
        personNum = 0;
        for maxLevelInfo in results: 
            #print("maxLevelInfo: %s " % maxLevelInfo['level'])
            if(maxLevelInfo['level'] == str(i)):
                personNum = maxLevelInfo['personNum'];
        
        #print("-- 完成第%s关完成率: %s " % (i, ratio))
        worksheet.write(i, 0, i) 
        worksheet.write(i, 1, startPersonNum) 
        worksheet.write(i, 2, finishPersonNum) 
        worksheet.write(i, 3, ratio) 
        worksheet.write(i, 4, remainRatio) 
        worksheet.write(i, 5, startNum) 
        worksheet.write(i, 6, finishNum) 
        worksheet.write(i, 7, failNum) 
        worksheet.write(i, 8, SingleExplodeNum) 
        worksheet.write(i, 9, RegenFieldNum) 
        worksheet.write(i, 10, FreeMoveNum) 
        worksheet.write(i, 11, AreaExpoldeNum) 
        worksheet.write(i, 12, AddStepNum)
        worksheet.write(i, 13,personNum)
        cursor.close()
        conn.close() 
        conn = get_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
    print("-- 完成关卡数据统计 -- ")
    cursor.close()
    conn.close()
    
 
    
    
    
    #广告统计数据
    print("-- 开始广告数据统计 -- ")
    conn = get_connection()
    # 使用 cursor() 方法创建一个 dict 格式的游标对象 cursor
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    #激励视频
    cursor.execute("select count(*) as total from Statistics where ops = 'ad_query' and param1 = 'RewardVideoAds' and timestamps > %s and timestamps < %s and region = '%s'"%(startTime, endTime, region))
    data = cursor.fetchone()
    RewardAdQueryNum = data['total']
    #print("-- 激励视频广告请求次数: %s " % (data['total']))
        
    cursor.execute("select count(*) as total from Statistics where ops = 'ad_impression' and param1 = 'RewardVideoAds' and timestamps > %s and timestamps < %s and region = '%s'"%(startTime, endTime, region))
    data = cursor.fetchone()
    RewardAdImpressionNum = data['total']
    #print("-- 激励视频广告展示次数: %s " % (data['total']))
    
    cursor.execute("select count(*) as total from Statistics where ops = 'ad_impression_finish' and param1 = 'RewardVideoAds' and param4 = 'Completed' and timestamps > %s and timestamps < %s and region = '%s'"%(startTime, endTime, region))
    data = cursor.fetchone()
    RewardAdFinishNum = data['total']
    #print("-- 激励视频广告完成次数: %s " % (data['total']))
    
    cursor.execute("select count(*) as total from Statistics where ops = 'ad_error' and param1 = 'RewardVideoAds' and timestamps > %s and timestamps < %s and region = '%s'"%(startTime, endTime, region))
    data = cursor.fetchone()
    RewardAdErrorNum = data['total']
    #print("-- 激励视频广告失败次数: %s " % (data['total']))

    #插屏
    cursor.execute("select count(*) as total from Statistics where ops = 'ad_query' and param1 = 'InterstitialAds' and timestamps > %s and timestamps < %s and region = '%s'"%(startTime, endTime, region))
    data = cursor.fetchone()
    InterstitialAdQueryNum = data['total']
    #print("-- 插屏广告请求次数: %s " % (data['total']))
        
    cursor.execute("select count(*) as total from Statistics where ops = 'ad_impression' and param1 = 'InterstitialAds' and timestamps > %s and timestamps < %s and region = '%s'"%(startTime, endTime, region))
    data = cursor.fetchone()
    InterstitialAdImpressionNum = data['total']
    #print("-- 插屏广告展示次数: %s " % (data['total']))
    
    cursor.execute("select count(*) as total from Statistics where ops = 'ad_impression_finish' and param1 = 'InterstitialAds' and timestamps > %s and timestamps < %s and region = '%s'"%(startTime, endTime, region))
    data = cursor.fetchone()
    InterstitialAdFinishNum = data['total']
    #print("-- 插屏广告完成次数: %s " % (data['total']))
    
    cursor.execute("select count(*) as total from Statistics where ops = 'ad_error' and param1 = 'InterstitialAds' and timestamps > %s and timestamps < %s and region = '%s'"%(startTime, endTime, region))
    data = cursor.fetchone()
    InterstitialAdErrorNum = data['total']
    #print("-- 插屏广告失败次数: %s " % (data['total']))

    #开屏
    cursor.execute("select count(*) as total from Statistics where ops = 'ad_query' and param1 = 'SplashAds' and timestamps > %s and timestamps < %s and region = '%s'"%(startTime, endTime, region))
    data = cursor.fetchone()
    SplashAdQueryNum = data['total']
    #print("-- 开屏广告请求次数: %s " % (data['total']))
        
    cursor.execute("select count(*) as total from Statistics where ops = 'ad_impression' and param1 = 'SplashAds' and timestamps > %s and timestamps < %s and region = '%s'"%(startTime, endTime, region))
    data = cursor.fetchone()
    SplashAdImpressionNum = data['total']
    #print("-- 开屏广告展示次数: %s " % (data['total']))
    
    cursor.execute("select count(*) as total from Statistics where ops = 'ad_impression_finish' and param1 = 'SplashAds' and timestamps > %s and timestamps < %s and region = '%s'"%(startTime, endTime, region))
    data = cursor.fetchone()
    SplashAdFinishNum = data['total']
    #print("-- 开屏广告完成次数: %s " % (data['total']))
    
    cursor.execute("select count(*) as total from Statistics where ops = 'ad_error' and param1 = 'SplashAds' and timestamps > %s and timestamps < %s and region = '%s'"%(startTime, endTime, region))
    data = cursor.fetchone()
    SplashAdErrorNum = data['total']
    #print("-- 开屏广告失败次数: %s " % (data['total']))


   #Banner
    cursor.execute("select count(*) as total from Statistics where ops = 'ad_query' and param1 = 'Banner' and timestamps > %s and timestamps < %s and region = '%s'"%(startTime, endTime, region))
    data = cursor.fetchone()
    BannerAdQueryNum = data['total']
    #print("-- Banner广告请求次数: %s " % (data['total']))
        
    cursor.execute("select count(*) as total from Statistics where ops = 'ad_impression' and param1 = 'Banner' and timestamps > %s and timestamps < %s and region = '%s'"%(startTime, endTime, region))
    data = cursor.fetchone()
    BannerAdImpressionNum = data['total']
    #print("-- Banner广告展示次数: %s " % (data['total']))
    
    cursor.execute("select count(*) as total from Statistics where ops = 'ad_impression_finish' and param1 = 'Banner' and timestamps > %s and timestamps < %s and region = '%s'"%(startTime, endTime, region))
    data = cursor.fetchone()
    BannerAdFinishNum = data['total']
    #print("-- Banner广告完成次数: %s " % (data['total']))
    
    cursor.execute("select count(*) as total from Statistics where ops = 'ad_error' and param1 = 'Banner' and timestamps > %s and timestamps < %s and region = '%s'"%(startTime, endTime, region))
    data = cursor.fetchone()
    BannerAdErrorNum = data['total']
    #print("-- Banner广告失败次数: %s " % (data['total']))
  

   #信息流
    cursor.execute("select count(*) as total from Statistics where ops = 'ad_query' and param1 = 'Feed' and timestamps > %s and timestamps < %s and region = '%s'"%(startTime, endTime, region))
    data = cursor.fetchone()
    FeedAdQueryNum = data['total']
    #print("-- 信息流广告请求次数: %s " % (data['total']))
        
    cursor.execute("select count(*) as total from Statistics where ops = 'ad_impression' and param1 = 'Feed' and timestamps > %s and timestamps < %s and region = '%s'"%(startTime, endTime, region))
    data = cursor.fetchone()
    FeedAdImpressionNum = data['total']
    #print("-- 信息流广告展示次数: %s " % (data['total']))
    
    cursor.execute("select count(*) as total from Statistics where ops = 'ad_impression_finish' and param1 = 'Feed' and timestamps > %s and timestamps < %s and region = '%s'"%(startTime, endTime, region))
    data = cursor.fetchone()
    FeedAdFinishNum = data['total']
    #print("-- 信息流广告完成次数: %s " % (data['total']))
    
    cursor.execute("select count(*) as total from Statistics where ops = 'ad_error' and param1 = 'Feed' and timestamps > %s and timestamps < %s and region = '%s'"%(startTime, endTime, region))
    data = cursor.fetchone()
    FeedAdErrorNum = data['total']
    #print("-- 信息流广告失败次数: %s " % (data['total']))

    arv = RewardAdFinishNum/totalLoginNum
    #print("-- 人均激励次数: %s " % arv)

    arv1 = InterstitialAdFinishNum/totalLoginNum
    #print("-- 人均插屏次数: %s " % arv1)
    
    arv2 = SplashAdFinishNum/totalLoginNum
    #print("-- 人均开屏次数: %s " % arv2)
    
    arv3 = FeedAdImpressionNum/totalLoginNum
    #print("-- 人均信息流次数: %s " % arv3)
    
    arv4 = BannerAdImpressionNum/totalLoginNum
    #print("-- 人均Banner次数: %s " % arv4)
    
    
    
    adWorksheet = workbook.add_sheet('广告数据')
    
    
    pattern = xlwt.Pattern() # 创建模式对象Create the Pattern
    pattern.pattern = xlwt.Pattern.SOLID_PATTERN # May be: NO_PATTERN, SOLID_PATTERN, or 0x00 through 0x12
    pattern.pattern_fore_colour = 5 #设置模式颜色 May be: 8 through 63. 0 = Black, 1 = White, 2 = Red, 3 = Green, 4 = Blue, 5 = Yellow, 6 = Magenta, 7 = Cyan, 16 = Maroon, 17 = Dark Green, 18 = Dark Blue, 19 = Dark Yellow , almost brown), 20 = Dark Magenta, 21 = Teal, 22 = Light Gray, 23 = Dark Gray, the list goes on...
    style = xlwt.XFStyle() # 创建样式对象Create the Pattern
    style.pattern = pattern # 将模式加入到样式对象Add Pattern to Style
    
    totalLevelNums = 0
    for maxLevelInfo in results: 
    #print("maxLevelInfo: %s " % maxLevelInfo['level'])
        totalLevelNums =  totalLevelNums + int(maxLevelInfo['personNum']) * int(maxLevelInfo['level']);
    
    adWorksheet.col(12).width = 3000
    adWorksheet.col(13).width = 3000
    adWorksheet.col(14).width = 3000
    adWorksheet.col(15).width = 3000
    adWorksheet.col(16).width = 3000
    adWorksheet.write(0, 12, "总注册人数") 
    adWorksheet.write(0, 13, "当日注册人数") 
    adWorksheet.write(0, 14, "当日活跃人数(开始关卡)") 
    adWorksheet.write(0, 15, "最远关卡") 
    adWorksheet.write(0, 16, "人均关卡") 
    adWorksheet.write(1, 12, allTotalRegisterNum, style) 
    adWorksheet.write(1, 13, totalRegisterNum, style) 
    adWorksheet.write(1, 14, totalLoginNum, style) 
    adWorksheet.write(1, 15, maxLevel, style) 
    adWorksheet.write(1, 16, totalLevelNums/allTotalRegisterNum, style) 
    
    
    
    
    adWorksheet.col(1).width = 3000
    adWorksheet.col(2).width = 3000
    adWorksheet.col(3).width = 3000
    adWorksheet.col(4).width = 3000
    adWorksheet.col(5).width = 3000
    adWorksheet.col(6).width = 5000
    adWorksheet.col(7).width = 5000
    adWorksheet.write(0, 1, "请求次数") 
    adWorksheet.write(0, 2, "展示数") 
    adWorksheet.write(0, 3, "完成次数") 
    adWorksheet.write(0, 4, "失败次数") 
    adWorksheet.write(0, 5, "人均次数") 
    adWorksheet.write(0, 6, "播放率（展示数/请求次数）") 
    adWorksheet.write(0, 7, "完播率（完成次数/展示数）") 
    
    
    #激励视频
    adWorksheet.write(1, 0, "激励视频") 
    adWorksheet.write(1, 1, RewardAdQueryNum) 
    adWorksheet.write(1, 2, RewardAdImpressionNum) 
    adWorksheet.write(1, 3, RewardAdFinishNum) 
    adWorksheet.write(1, 4, RewardAdErrorNum) 
    adWorksheet.write(1, 5, arv) 
    if(RewardAdQueryNum == 0):
        adWorksheet.write(1, 6, 0) 
    else:
        adWorksheet.write(1, 6, RewardAdImpressionNum/RewardAdQueryNum) 
    if(RewardAdImpressionNum == 0):
        adWorksheet.write(1, 7, 0) 
    else:
        adWorksheet.write(1, 7, RewardAdFinishNum/RewardAdImpressionNum) 
    
    #插屏
    adWorksheet.write(2, 0, "插屏") 
    adWorksheet.write(2, 1, InterstitialAdQueryNum) 
    adWorksheet.write(2, 2, InterstitialAdImpressionNum) 
    adWorksheet.write(2, 3, InterstitialAdFinishNum) 
    adWorksheet.write(2, 4, InterstitialAdErrorNum) 
    adWorksheet.write(2, 5, arv1) 
    if(InterstitialAdQueryNum == 0):
        adWorksheet.write(2, 6, 0) 
    else:
        adWorksheet.write(2, 6, InterstitialAdImpressionNum/InterstitialAdQueryNum) 
    if(InterstitialAdImpressionNum == 0):
        adWorksheet.write(2, 7, 0) 
    else:
        adWorksheet.write(2, 7,InterstitialAdFinishNum/InterstitialAdImpressionNum)

    #开屏
    adWorksheet.write(3, 0, "开屏") 
    adWorksheet.write(3, 1, SplashAdQueryNum) 
    adWorksheet.write(3, 2, SplashAdImpressionNum) 
    adWorksheet.write(3, 3, SplashAdFinishNum) 
    adWorksheet.write(3, 4, SplashAdErrorNum) 
    adWorksheet.write(3, 5, arv2) 
    if(SplashAdQueryNum == 0):
        adWorksheet.write(3, 6, 0) 
    else:
        adWorksheet.write(3, 6, SplashAdImpressionNum/SplashAdQueryNum) 
    if(SplashAdImpressionNum == 0):
        adWorksheet.write(3, 7, 0) 
    else:
        adWorksheet.write(3, 7,SplashAdFinishNum/SplashAdImpressionNum)

    #Banner
    adWorksheet.write(4, 0, "Banner") 
    adWorksheet.write(4, 1, BannerAdQueryNum) 
    adWorksheet.write(4, 2, BannerAdImpressionNum) 
    adWorksheet.write(4, 3, BannerAdFinishNum) 
    adWorksheet.write(4, 4, BannerAdErrorNum) 
    adWorksheet.write(4, 5, arv4) 
    if(BannerAdQueryNum == 0):
        adWorksheet.write(4, 6, 0) 
    else:
        adWorksheet.write(4, 6, BannerAdImpressionNum/BannerAdQueryNum) 
    if(BannerAdImpressionNum == 0):
        adWorksheet.write(4, 7, 0) 
    else:
        adWorksheet.write(4, 7,BannerAdFinishNum/BannerAdImpressionNum)

    #信息流
    adWorksheet.write(5, 0, "信息流") 
    adWorksheet.write(5, 1, FeedAdQueryNum) 
    adWorksheet.write(5, 2, FeedAdImpressionNum) 
    adWorksheet.write(5, 3, FeedAdFinishNum) 
    adWorksheet.write(5, 4, FeedAdErrorNum) 
    adWorksheet.write(5, 5, arv3) 
    if(FeedAdQueryNum == 0):
        adWorksheet.write(5, 6, 0) 
    else:
        adWorksheet.write(5, 6, FeedAdImpressionNum/FeedAdQueryNum) 
    if(FeedAdImpressionNum == 0):
        adWorksheet.write(5, 7, 0) 
    else:
        adWorksheet.write(5, 7,FeedAdFinishNum/FeedAdImpressionNum)
    

 
    adWorksheet.write(9, 1, "激励视频展示位") 
    adWorksheet.write(9, 2, "激励视频展示次数") 
    adWorksheet.write(9, 3, "激励视频人均次数") 
    index = 9;
    positions = ["preplay_add_step","get_star_gift","get_coin","cash_reward", "add_step_5", "newuser_reward", "spin", "daily", "ad_booster", "ad_combinebubble", "ad_lucky", "ad_limitreward", "myrewardrank_reward", "myreward_redenvelop_reward", "limit_task_reward", "withdraw_outside_reward", "withdraw_inside_reward", "newuser_1_reward", "newuser_2_reward", "limit_redenvelop_reward", "online_reward", "permanent_reward", "close_window_lucky_reward"]
    for position in positions: 
        cursor.execute("select count(*) as total from Statistics where ops = 'ad_impression' and param1 = 'RewardVideoAds' and param2 = '%s' and timestamps > %s and timestamps < %s and region = '%s'"%(position, startTime, endTime, region))
        data = cursor.fetchone()
        RewardAdImpressionNum = data['total']
        index = index + 1
        adWorksheet.write(index, 1, position) 
        adWorksheet.write(index, 2, data['total']) 
        adWorksheet.write(index, 3, data['total']/totalLoginNum) 
        #print("-- %s激励视频广告展示次数: %s " % (position, data['total']))
        
    worksheet.col(7).width = 3000
    worksheet.col(8).width = 3000
    worksheet.col(9).width = 3000
    adWorksheet.write(9, 7, "插屏展示位") 
    adWorksheet.write(9, 8, "插屏展示次数") 
    adWorksheet.write(9, 9, "插屏人均次数") 
    index = 9;
    positions = ["fail_interstitial","spin_interstitial","cashstore_interstitial","setting_interstitial", "daily_interstitial", "close_redenvelop_interstitial", "cash_interstitial", "dailytask_interstitial", "main_interstitial",  "close_unreceived_interstitial", "newuser_interstitial"]
    for position in positions: 
        cursor.execute("select count(*) as total from Statistics where ops = 'ad_impression' and param1 = 'InterstitialAds' and param2 = '%s' and timestamps > %s and timestamps < %s and region = '%s'"%(position, startTime, endTime, region))
        data = cursor.fetchone()
        InterstitialAdImpressionNum = data['total']
        index = index + 1
        adWorksheet.write(index, 7, position) 
        adWorksheet.write(index, 8, data['total']) 
        adWorksheet.write(index, 9, data['total']/totalLoginNum) 
        #print("-- %s插屏广告展示次数: %s " % (position, data['total']))
        
    worksheet.col(13).width = 3000
    worksheet.col(14).width = 3000
    worksheet.col(15).width = 3000
    adWorksheet.write(9, 13, "信息流展示位") 
    adWorksheet.write(9, 14, "信息流展示次数") 
    adWorksheet.write(9, 15, "信息流人均次数") 
    index = 9;
    positions = ["cash_feed","setting_feed","daily_feed","text_feed", "fail_feed", "unreceived_feed", "newuser_1_feed", "newuser_2_feed"]
    for position in positions: 
        cursor.execute("select count(*) as total from Statistics where ops = 'ad_impression' and param1 = 'Feed' and param2 = '%s' and timestamps > %s and timestamps < %s and region = '%s'"%(position, startTime, endTime, region))
        data = cursor.fetchone()
        FeedAdImpressionNum = data['total']
        index = index + 1
        adWorksheet.write(index, 13, position) 
        adWorksheet.write(index, 14, data['total']) 
        adWorksheet.write(index, 15, data['total']/totalLoginNum) 
        #print("-- %s信息流广告展示次数: %s " % (position, data['total']))   
        
        
    print("-- 完成广告数据统计 -- ")
    

    workbook.save("%s_数据统计_%s.xls" % (region, str(yesterday))) # 保存文件

    # 关闭数据库连接
    cursor.close()
    conn.close()
    
    print("-- 结束该日数据统计 -- ")





def SendEmail(): 
    # 今天日期
    today = datetime.date.today() 
    # 昨天时间
    yesterday = today - datetime.timedelta(days=1)
    for receiver in receivers:
        #创建一个带附件的实例
        message = MIMEMultipart()
        message['From'] = sender
        message["To"] = receiver
        subject = '%s数据统计'%(str(yesterday))
        message['Subject'] = '%s数据统计'%(str(yesterday))
         
        #邮件正文内容
        message.attach(MIMEText('%s数据统计'%(str(yesterday)), 'plain', 'utf-8'))
         
         
        for region in regions:
            if(os.path.exists("%s_数据统计_%s.xls"%(region, str(yesterday)))):        
                att1 = MIMEBase('application', "octet-stream")
                att1.set_payload(open("%s_数据统计_%s.xls"%(region, str(yesterday)),'rb').read())
                encoders.encode_base64(att1)
                att1.add_header('Content-Disposition', 'attachment; filename="%s-%s.xls"'%(region, str(yesterday)))
                message.attach(att1)

        try:
            # 第三方 SMTP 服务
            mail_host=""  #设置服务器
            mail_user=""    #用户名
            mail_pass=""   #口令 
            smtpObj = smtplib.SMTP()
            smtpObj.connect(mail_host,25)    # 25 为 SMTP 端口号
            smtpObj.login(mail_user, mail_pass)  
            smtpObj.sendmail(sender, [receiver], message.as_string())
            print("邮件发送成功")
        except smtplib.SMTPException as e:
            print("Error: 无法发送邮件%s"% e)
   








if __name__ == '__main__':
    start()    