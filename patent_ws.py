import json
import random
import re
import threading
import time
import pandas
from lxml import etree
from multiprocessing.dummy import Pool
import requests

## 创建线程池
pool = Pool(5)
# 将代理接口设置为自己的，记得选代理格式为https、数据格式为json、每次提取个数为1

# 创建随机ua列表，防采集
USER_AGENTS = [
    "Mozilla/5.0 (Linux; U; Android 2.3.6; en-us; Nexus S Build/GRK39F) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
    "Avant Browser/1.2.789rel1 (http://www.avantbrowser.com)",
    "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/532.5 (KHTML, like Gecko) Chrome/4.0.249.0 Safari/532.5",
    "Mozilla/5.0 (Windows; U; Windows NT 5.2; en-US) AppleWebKit/532.9 (KHTML, like Gecko) Chrome/5.0.310.0 Safari/532.9",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/534.7 (KHTML, like Gecko) Chrome/7.0.514.0 Safari/534.7",
    "Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) AppleWebKit/534.14 (KHTML, like Gecko) Chrome/9.0.601.0 Safari/534.14",
    "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.14 (KHTML, like Gecko) Chrome/10.0.601.0 Safari/534.14",
    "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.20 (KHTML, like Gecko) Chrome/11.0.672.2 Safari/534.20",
]

##定义json请求请求头
headers = {
    "User-Agent": random.choice(USER_AGENTS),
    "Referer": "https://kns.cnki.net/kns8/defaultresult/index",
    "Origin": "https://kns.cnki.net",
    "Host": "kns.cnki.net",
    "Connection": "close",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Accept": "*/*; q=0.01",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "sec-ch-ua": '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
}

## 获取详情页，摘要信息的函数
def getdetaildata(param,iname):

    ##构造html详情页请求的请求头
    headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Referer":"https://www.cnki.net/",
                "Origin": "https://kns.cnki.net",
                "Host": "kns.cnki.net",
                "Connection": "close",
                "Content-Type":"application/x-www-form-urlencoded; charset=UTF-8",
                "Accept":"*/*; q=0.01",
                "Accept-Encoding":"gzip, deflate, br",
                "Accept-Language":"zh-CN,zh;q=0.9",
                "sec-ch-ua":'"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"'
            }
    ## 发起请求，捕获异常、
    while True:
        try:
            ##正常请求退出循环
            res = requests.get(param, headers=headers, verify=False,
                               timeout=100,
                               # proxies=golols["dl"]
                               ).text  # ,proxies=golols["dl"]
            break
        except:

            # 异常请求重发
            print("里面请求异常")

    # 解析网页源码为xml对象
    htmldom = etree.HTML(res)

    ##构造需要解析的字段
    data = {
        "ChDivSummary":None,
        "claim_text":None,
    }
    ##解析内容
    data["ChDiv_summary"] = ''.join(htmldom.xpath(".//div[@id='ChDivSummary']//text()"))
    data["claim_text"] = ''.join(htmldom.xpath(".//div[@class='claim-text']//text()"))

    ##返回内容
    return data


def datasql(newitem):
    ##将采集的item保存到temp临时文件中
    with open("temp.txt",'a',encoding='utf-8') as f:
        f.write(json.dumps(newitem))
        f.write('\n')

#该函数用来格式化详情页url的
#通过一级页面采集的url并不能直接访问，需要解析出dbcode、dbname、filename访问
def change_href(firsturl):
    
    ##将url拼接的参数转字典
    params = dict(
        [
            (i.split("=")[0].strip(),i.split("=")[-1].strip()) for i in
            firsturl.split('?')[-1].split('&')
        ]
    )
    ##拼接详情页url
    secondurl = f'https://kns.cnki.net/kcms/detail/detail.aspx?' \
                f'dbcode={params.get("DBCode")}&' \
                f'dbname={params.get("dbname")}&' \
                f'filename={params.get("filename")}'
    # 返回url
    return secondurl

# 获取查询的sql参数
def get_SearchSql(iyear,iname):
    ##定义json请求请求体
    data = {
        "IsSearch": "true",
        "QueryJson": '{"Platform":"","DBCode":"SCOD","KuaKuCode":"","QNode":{"QGroup":[{"Key":"Subject","Title":"","Logic":1,"Items":[{"Title":"主题","Name":"SU","Value":"'+iname+'","Operate":"%=","BlurType":""}],"ChildItems":[]},{"Key":"MutiGroup","Title":"","Logic":1,"Items":[],"ChildItems":[{"Key":"3","Title":"","Logic":1,"Items":[{"Key":"'+iyear+'","Title":"'+iyear+'","Logic":2,"Name":"年","Operate":"","Value":"'+iyear+'","ExtendType":0,"ExtendValue":"","Value2":"","BlurType":""}],"ChildItems":[]}]}]}}',
        "PageName": 'DefaultResult',
        "DBCode": "SCOD",
        "KuaKuCodes": "CJFQ,CDMD,CIPD,CCND,CISD,SNAD,BDZK,CCJD,CCVD,CJFN",
        "CurPage": 1,
        "RecordsCntPerPage": "50",
        "CurDisplayMode": "listmode",
        "CurrSortFieldType": "desc",
        "IsSentenceSearch": "false",
        "CurrSortField":'',
        'Subject': "",
    }
    ##json请求url
    base_post_api = 'https://kns.cnki.net/KNS8/Brief/GetGridTableHtml'

    ##发送请求，获取翻页内容
    while True:
        try:
            resc = requests.post(base_post_api, data=data, headers=headers, timeout=10,
                                 # proxies=golols["dl"],
                                 verify=False)
            ## 正确响应获取sqlval字段值
            lxml_dom = etree.HTML(resc.text)
            sql_id = lxml_dom.xpath(".//input[@id='sqlVal']/@value")[0]
            break
        except Exception as e:
            ##异常请求重连
            print(f"里面请求异常 {e}")
            time.sleep(2)
    return sql_id

##该函数用来翻页获取内容
def getout(iyear,iname):
            ##定义页码计数器
            config = {
                "currentpage": 1,
                "totpage": 1,
            }
            ##获取检索的sql内容
            iname_search_id = get_SearchSql(iyear,iname)

            ##遍历页码、
            while config["currentpage"] <= config["totpage"]:

                ##定义当前页码的请求参数
                data = {
                    "IsSearch": "false",
                    "QueryJson": '{"Platform":"","DBCode":"SCOD","KuaKuCode":"","QNode":{"QGroup":[{"Key":"Subject","Title":"","Logic":1,"Items":[{"Title":"主题","Name":"SU","Value":"' + iname + '","Operate":"%=","BlurType":""}],"ChildItems":[]},{"Key":"MutiGroup","Title":"","Logic":1,"Items":[],"ChildItems":[{"Key":"3","Title":"","Logic":1,"Items":[{"Key":"' + iyear + '","Title":"' + iyear + '","Logic":2,"Name":"年","Operate":"","Value":"' + iyear + '","ExtendType":0,"ExtendValue":"","Value2":"","BlurType":""}],"ChildItems":[]}]}]}}',
                    "PageName": 'DefaultResult',
                    "HandlerId": "2",
                    "DBCode":"SCOD",
                    "KuaKuCodes": "",
                    "CurPage": str(config["currentpage"]),
                    "RecordsCntPerPage":"50",
                    "CurDisplayMode": "listmode",
                    "CurrSortField":'',
                    "CurrSortFieldType": "desc",
                    "IsSentenceSearch": "false",
                    "IsSortSearch":'false',
                    'Subject': "",
                    "SearchSql":iname_search_id
                }

                ##发起网络请求、并捕获异常
                while True:
                    try:
                        try:
                            resc = requests.post('https://kns.cnki.net/KNS8/Brief/GetGridTableHtml', data=data,
                                                 timeout=10,
                                                 # proxies=golols["dl"],
                                                 headers=headers, verify=False).text  #
                            break
                        except requests.exceptions.ConnectionError:
                            print("请求异常")
                    except Exception as e:
                        print(e, "请求外面页面异常")
                        time.sleep(3)
                ##解析页码内容
                resdom = etree.HTML(resc)

                ##如果是第一页，将后续页码检索方式改为fasle
                ##并计算totpage，以便退出
                if config["currentpage"] == 1:
                    data["IsSearch"]= "false"
                    data["SearchSql"] = resdom.xpath(".//input[@id='sqlVal']/@value")[0] if len(resdom.xpath(".//input[@id='sqlVal']/@value"))>0 else ''
                    config['totpage'] = int(
                        resdom.xpath(".//span[@class='countPageMark']/text()")[0].split("/")[-1].strip()) if len(resdom.xpath(".//span[@class='countPageMark']/text()")) else 1
                    tot_count = re.findall(r'共找到<em>(.*?)</em>条结果',resc)[0].replace(",",'') if len(re.findall(r'共找到<em>(.*?)</em>条结果',resc)) !=0 else 0
                    with open("total_count.txt",'a',encoding='utf-8') as f:
                        f.write('%s\t%s'%(iname,str(tot_count)))
                        f.write('\n')
                    print("当其主题下%s有：%d页数据" % (iname, config['totpage']))
                ##解析提取页面中的内容
                handleoutdom(resdom, iname, config,iyear)
                ##页码+1
                config["currentpage"] += 1

#该函数从temp中读取临时存储的文件、转json
def load_data():
    with open('temp.txt','r',encoding='utf-8') as f:
        lines = [json.loads(i.strip()) for i in f.readlines()]
        df = pandas.DataFrame(lines)
        df.to_excel("metalcore.xlsx",index=False)
## 该函数，解析提取字段
def handleoutdom(resdom,iname,config,iyear):

    ##获取表格列表内容
    alllist = resdom.xpath(".//table[@class='result-table-list']/tr")
    for ili in alllist:
        newitem = {}
        ##提取其中的字段
        newitem["title"] = ''.join(ili.xpath("./td[2]")[0].xpath("string(.)").strip().split(' ')).replace("\t",'').replace("\n",'').replace("\r",'')
        newitem["author"] = ili.xpath("./td[3]")[0].xpath("string(.)").strip().replace("\t",'').replace("\n",'').replace("\r",'')
        newitem["com"] = ili.xpath("./td[4]")[0].xpath("string(.)").strip().replace("\t",'').replace("\n",'').replace("\r",'')
        newitem["public"] = ili.xpath("./td[5]")[0].xpath("string(.)").strip().replace("\t",'').replace("\n",'').replace("\r",'')
        newitem["sql"] = ili.xpath("./td[6]")[0].xpath("string(.)").strip().replace("\t",'').replace("\n",'').replace("\r",'')
        newitem["quote"] = ili.xpath("./td[7]")[0].xpath("string(.)").strip().replace("\t",'').replace("\n",'').replace("\r",'')
        newitem["download"] = ili.xpath("./td[8]")[0].xpath("string(.)").strip().replace("\t",'').replace("\n",'').replace("\r",'')
        newitem["href"]  = change_href("https://kns.cnki.net" + ili.xpath("./td[2]/a/@href")[0])
        try:
            returndata = getdetaildata(newitem["href"],iname)
        except Exception as e:
            print(e, "443页面异常")
            with open("error.txt", 'a', encoding='utf-8') as f:
                f.write(time.ctime() + str(iname))
            time.sleep(2)
            continue
        newitem["ChDiv_summary"] = returndata.get("ChDiv_summary")
        newitem['claim_text'] = returndata.get("claim_text")

        ##打印日志
        print(
                "%s 年份 %s 当前主题：%s\t第%d页共%d页第%d条数据："% (
                    str(threading.currentThread().getName()),iyear,iname,config["currentpage"],config["totpage"],alllist.index(ili) +1)
              ,newitem
              )
        ##数据保存
        datasql(newitem)


if __name__ == "__main__":


    """
    @知网验证码属于验证码类型、sql不变即可
    @代码默认单线程，可开启多线程，最高不要超过五个线程
    @代码仅供学习参考
    """

    for iyear in range(1987,2023)[::-1]:
        getout(str(iyear),'金属核')
    load_data()


