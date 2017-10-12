import os
import requests
from bs4 import BeautifulSoup as bf
import time
from multiprocessing import Pool
from openpyxl import Workbook
import datetime
import pandas as pd
import math

# http://sou.zhaopin.com/jobs/searchresult.ashx?jl=%e9%83%91%e5%b7%9e&kw=python&isadv=0&sg=5768330709424ce580218125482810e8&p=1
MYCOUNT=0
def getZhilianInfo(city, kw,p=1,isGetPage=False,RequestsUrl='http://sou.zhaopin.com/jobs/searchresult.ashx',Mcount=MYCOUNT):
    global MYCOUNT
    try:
        query = {'isadv':0, 'jl':city,
                 'kw': kw,
                 'sg': '5768330709424ce580218125482810e8',
                 'p': p,
                 }
        headers = {'user-agent': "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
                   'Host':'sou.zhaopin.com',
                   'Upgrade-Insecure-Requests':'1',
                   'Connection':'keep-alive',
                   'Cache-Control': 'max-age=0',
                   'Accept-Encoding':'gzip, deflate',
                   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                   }
        r = requests.get(RequestsUrl, params=query, headers=headers)
        print(r.status_code, r.url)
        if r.status_code == 200:
            print(UA)
            # print(r.text)
            soup=bf(r.text,'lxml')
            contentList=soup.select('#newlist_list_content_table  .newlist')[1:]
            if len(contentList)==0:
                raise ValueError('get a empty list ,try again')
            # print(len(contentList),contentList)
            totalRecord=soup.select('.seach_yx .search_yx_tj em')[0].text
            if isGetPage:
                return totalRecord
            everyPageList=[]
            for ls in contentList:
                try:
                    offer=ls.select('.zwmc a')[0].text
                    offerLink=ls.select('.zwmc a')[0]['href']
                    company = ls.select('.gsmc a')[0].text
                    companyLink = ls.select('.gsmc a')[0]['href']
                    salary = ls.select('.zwyx')[0].text
                    location = ls.select('.gzdd')[0].text
                    try:
                        detailList=getDetailPageInfo(offerLink) # 获取详情页面具体的招聘信息和介绍
                    except Exception as e:
                        #这里以后可以写入日志 建立一个日志函数 处理所有出错信息 ,写上出错的地方 和具体内容
                        print(e,'获取某个列表详情信息出错，做为空值处理')
                        detailList =[]
                    # 列表页 信息加上详情页信息 作为一条大记录
                    everyList=[p,offer,offerLink,salary,location,company,companyLink]+detailList
                    everyPageList.append(everyList)
                except Exception as e:
                    print(e,'循环列表是 ，某一条记录出错,跳过')
                    continue
            return everyPageList
        else:
            print('服务器拒绝访问。。。。')
            exit()

    except Exception as e:
        MYCOUNT+=1
        time.sleep(1)
        print(e,'第\t%d页\t%d次\t请求链接出现问题，正在请求下一次链接......'%(p,MYCOUNT))
        #对于一些无法预料的错误的处理 比如淘宝不让搜索 sisy私服 这个，如果不加判断条件，程序就会一直请求，死循环了
        if MYCOUNT<3:
            getZhilianInfo(city, kw, p=1,isGetPage=False, RequestsUrl='http://sou.zhaopin.com/jobs/searchresult.ashx', Mcount=MYCOUNT)
        else:
            MYCOUNT = 0
            print('error stop')
            exit()

# 得到具体的招聘信息
def getDetailPageInfo(offerLink):
    headers = {'user-agent': UA,
               # 'Host':'sou.zhaopin.com',
               # 'Upgrade-Insecure-Requests': '1',
               }
    r = requests.get(offerLink, headers=headers)
    if r.status_code == 200:
        # print(r.status_code, r.url)
        soup = bf(r.text, 'lxml')
        # 智联招聘的 详情也网址 分两种  分别处理这两种详情
        # 一种是  http://jobs.zhaopin.com/CZ730878880J00074933601.htm
        # 一种是 http://xiaoyuan.zhaopin.com/job/CC000116133J90002078000
        if 'xiaoyuan' in offerLink:
            try:
                upInfo = soup.select('.cJobDetailInforWrap ul.cJobDetailInforTopWrap')[0]
                allCompany=upInfo.select('#jobCompany a')[0].text
                # businessLine = upInfo.select('.cJobDetailInforWd2')[0].text
                businessLine ='全职'
                # companySize = upInfo.select('.cJobDetailInforWd2')[1].text
                companySize='无经验'
                # companyType = upInfo.select('li')[-1].text
                companyType='不限'
                # print(allCompany,businessLine,companySize,companyType)
                downInfo = soup.select('.cJobDetailInforWrap ul.cJobDetailInforBotWrap')[0]
                # print('*#'*100)
                workingPlace = downInfo.select('li')[1].text.strip()
                jobCategory= downInfo.select('li')[3].text
                recruitCount = downInfo.select('li')[5].text
                publishDate = downInfo.select('li')[-1].text
                # print(allCompany,businessLine,companySize,companyType,workingPlace,jobCategory,recruitCount,publishDate)
                decribeJob=soup.select('.cJobDetail_tabSwitch_content .cJob_Detail p')[0].text.strip()
                # print(decribeJob)
                subtotalInfoList=[workingPlace,publishDate,businessLine,companySize,companyType,recruitCount,jobCategory,decribeJob]
            except:
                subtotalInfoList =['error']*8

            # print(len(subtotalInfoList), subtotalInfoList)

        else:
            # 这里判断一个 返回无线端链接的
            upInfo=soup.select('.terminalpage-left ul.terminal-ul li strong')
            subtotalInfoList=[]

            for smallInfo in upInfo:
                tempText=smallInfo.text.strip()
                if not tempText:
                    tempText="error"
                subtotalInfoList.append(tempText)
            # 特殊页面的处理
            if len(subtotalInfoList)==0:
                subtotalInfoList=['error']*8
            # print(len(subtotalInfoList), subtotalInfoList)
            # 奇特的情况，网页结构混乱等，遇到过这种情况，有十几个数据
            if len(subtotalInfoList)>8:
                subtotalInfoList=subtotalInfoList[0:8]
            # print(len(subtotalInfoList),subtotalInfoList)
            # 读取具体的要求
            try:
                content = soup.select('.tab-cont-box .tab-inner-cont')[0].select('p')
                allContent=' '

                for everyItem in content:
                    everyItem=str(everyItem.text.strip())
                    if not everyItem:continue # 过滤空行文字
                    allContent+=everyItem+"----"

            # except IndexError:
            except:
                allContent='error'
            # print(allContent)
            subtotalInfoList.append(allContent)
            subtotalInfoList.pop(0)

        # print(len(subtotalInfoList),subtotalInfoList)
        return subtotalInfoList

#===========================店铺信息存到excel文件 这个用在excel 2007及以上的版本=====================
def EveryPageWriteExcel2016(bigDataList,page=1,sheetName='data'):
    if len(bigDataList)==0:
        print('*******************空列表************************')
        exit()
    try:
        AllExcelHead = ['页码','0职位名称', '1职位链接', '2薪水','6地区', '7公司名称', '8公司链接','工作地点','发布日期','是否全职','工作经验','学历','招聘人数','职位','职责要求' ]

        fetchDate = str(time.strftime("%Y-%m-%d", time.localtime()))
        doc = r'{}\{}_{}_{}_{}.{}'.format(filePath, str(page),fetchDate,city,kw, 'xlsx')
        print(doc)
        # 在内存创建一个工作簿obj
        wb = Workbook()
        ws=wb.active
        #给sheet明个名
        ws.title = sheetName
        # 向第一个sheet页写数据吧 格式 ws2['B1'] = 4
        ws.append(AllExcelHead)
        k = 0
        for line in bigDataList:
            try:
                # print(line)
                if type(line)!=list:
                    line=[line]
                ws.append(line)
                k += 1
                # print('写入第%d条记录完毕' % (k))
            except Exception as e:
                print(e,'第%d条记录有问题，已经忽略' % k)
                continue
        else:
            print('###############恭喜你，第\t%d页\t写入完毕#####################'%page)
        wb.save(doc)
        print('数据保存完毕,文件路径是\t{}'.format(doc))
    except Exception as e:
        print(e ,'函数 \tEveryPageWriteExcel2016\t出现问题了')
        return
def IsSubString(SubStrList, Str):
    '''''
    #判断字符串Str是否包含序列SubStrList中的每一个子字符串
    #>>>SubStrList=['F','EMS','txt']
    #>>>Str='F06925EMS91.txt'
    #>>>IsSubString(SubStrList,Str)#return True (or False)
    '''
    flag = True
    for substr in SubStrList:
        if not (substr in Str):
            flag = False
    return flag

def GetALLFileListFromDir(FindPath, FlagStr=[]):
    #获取目录中指定的文件名
    FileList = []
    FileNames = os.listdir(FindPath)
    if (len(FileNames) > 0):
        for fn in FileNames:
            if (len(FlagStr) > 0):
                # 返回指定类型的文件名
                if (IsSubString(FlagStr, fn)):
                    fullfilename = os.path.join(FindPath, fn)
                print(fullfilename)
                FileList.append(fullfilename)
            else:
                # 默认直接返回所有文件名
                fullfilename = os.path.join(FindPath, fn)
                FileList.append(fullfilename)
    # print(FileList)
    return FileList

#获取失败的页码
def GetFailPage(FindPath, FlagStr=[]):
    pageList=[]
    FileNames = os.listdir(FindPath)
    if (len(FileNames) > 0):
        for fn in FileNames:
            if (len(FlagStr) > 0):
                # 返回指定类型的文件名
                if (IsSubString(FlagStr, fn)):
                    fullfilename = os.path.join(FindPath, fn)
                    basename = os.path.basename(fullfilename)
                    page=basename.split('_')[0]#获取最前面的页码
                    # print(page,basename)
                    pageList.append(int(page))
    return pageList
#  合并所有页码到一个文件 
def combineEveryPageInfoToOneV2(FindPath,outPath):
    try:
        FlagStr = ['xlsx']
        readDirFile = GetALLFileListFromDir(FindPath, FlagStr=FlagStr)
        dataList=[]
        for doc in readDirFile:
            try:
                if os.path.isfile(doc):
                    data = pd.read_excel(doc, sheetname='data')
                    dataList.append(data)
            except Exception as e:
                print(e,'合并\t%s\t文档出错,已经跳过'%doc)
                continue
        dataAll = pd.concat(dataList)
        dataAll.to_excel(outPath, index=False, sheet_name='data')
        print('合并完成')
    except Exception as e:
        print(e,'合并数据出了问题,检查 函数 combineEveryPageInfoToOneV2')
        return


FailCount=0 #这个全局的变量必须写到这里 ，不知道为啥 就是要写到函数前面
# 循环处理，如果发现第一次 一些页面没有处理成功，递归调用，在处理，直到所有页面处理完成为止
def cicleGetFailPage(FindPath,curFailCount=FailCount):
    global FailCount
    FlagStr=['xlsx']
    # 从本地读取处理失败的页码，当然，第一次读取的值是空值，不能说是失败，因为还没有处理，这个不影响，应为下面的
    # 差集还是总页码
    MyPageList = GetFailPage(FindPath, FlagStr=FlagStr)
    totalPageList = [p for p in range(1, totalPage + 1)]
    # 这里用差集，来判断是否有处理失败，总页码和已经处理成功的页码做差集，差集就是上次处理失败的页码，再次处理
    differencSetList = list(set(totalPageList) ^ set(MyPageList))

    if len(differencSetList) == 0:
        print('---------检查完毕，没有出错的页码了，已经把全部页码数据写入成功----------')
        return None
    if FailCount>0:
        print('第%d次写入失败了，这里获取页码，下面是失败的页码.....'%FailCount)
        print(differencSetList)
    myp = Pool()
    myp.map(main, differencSetList)
    print('等待5秒钟进行，下一步检查。。。。')
    time.sleep(5)
    curFailCount +=1
    cicleGetFailPage(FindPath,curFailCount=FailCount)

def main(page):
    print('正在处理第%d页' % page)
    print('☺' * 200)
    everyPageList = getZhilianInfo(city, kw, p=page)
    EveryPageWriteExcel2016(everyPageList,page=page)


start = datetime.datetime.now()
city='' # 郑州
if city=='':city='选择地区'
kw='天猫运营'
SavePath='{}:\综合信息\招聘'.format(ROOT_DIR)
# keyWord=kw
assignTotalPage= -1 # 手动指定 得到的页码
maxPage=40
isExecuate=1
isMerge=1
# offerLink='http://jobs.zhaopin.com/CZ408806830J00001457813.htm'
# getDetailPageInfo(offerLink)
# exit()
filePath = r'{}\{}'.format(SavePath, kw)
if not os.path.exists(filePath):
    os.makedirs(filePath)
if isExecuate:
    if assignTotalPage > 0:
        totalPage = assignTotalPage
    else:
        totalRecord = getZhilianInfo(city, kw, p=1, isGetPage=True)
        totalPage = math.ceil(int(totalRecord) / 60)
        if totalPage > maxPage: totalPage = maxPage
    print('共%d页' % totalPage)

if __name__ == '__main__':
    if isExecuate:
        cicleGetFailPage(filePath,curFailCount=FailCount)
    if isMerge:
        print('开始合并文件......')
        fetchDate = str(time.strftime("%Y-%m-%d", time.localtime()))
        outPathFile = r'{a}\{b}_{c}_{g}页_{d}.{f}'.format(a=str(SavePath), b=fetchDate, c=city, g=str(totalPage),d=str(kw), f='xlsx')
        combineEveryPageInfoToOneV2(filePath, outPathFile)
    end = datetime.datetime.now()
    print('☺☺☺☺☺☺恭喜你，全部信息保存完毕用时 %s ☺☺☺☺☺☺' % (end - start))


















