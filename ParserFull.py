import grequests
from bs4 import BeautifulSoup
import re
import pymysql


connection = pymysql.connect(host="localhost",user="al",passwd="gansior",db="parser1",charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)


urls = []
with connection:

    cursor = connection.cursor()
    cursor.execute("SELECT DISTINCT link FROM `links`  ") 
    lincks = cursor.fetchall()

    for linck in lincks:
        urls.append(linck["link"])


def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]
     

chunks_list = list(chunks(urls,100)) 

# если произойдет ошибка то откладываем урл на потом
def exception_handler(request, exception):
      pass 

    # print(request.url,exception)


headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36'}

nf=int(0)
indic=int(0)
for urls in chunks_list:  

    rs = (grequests.get(u,headers=headers,timeout=10) for u in urls)
    responses = grequests.map(rs,exception_handler=exception_handler)
    if indic==int(10):
        nf+=1
        indic=int(0)
        print("Creat file "+ str(nf))
    indic+=1
    for response in  responses:
        try:
            soup = BeautifulSoup(response.text,'html.parser')
            for par in soup.find_all("p"):
                if par.text:
                    loverstr = par.text.lower()
                    words_list = re.findall(r'[а-я]+',loverstr)
                    if len(words_list) > 100:
                        result = " ".join(words_list)
                        with open("worlds"+str(nf)+".txt", "a") as myfile:
                            myfile.write(result) 
        except Exception as e:
          pass

 
