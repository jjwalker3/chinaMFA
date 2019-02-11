
# coding: utf-8

# In[39]:


import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
# from selenium.webdriver.firefox.options import Options
# from selenium.webdriver.chrome.options import Options
import re

# %matplotlib inline
import re
import sys
import time
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import os
import psycopg2
from tqdm import tqdm


# %matplotlib inline


# In[46]:


start = str(datetime.today())


# In[2]:


try:
    topLevel_orig = pd.read_json('topLevel.json')
except:
    topLevel_orig = pd.DataFrame(columns=['country','page','url','lang','more'])


# In[3]:


country_root = "https://www.fmprc.gov.cn/web/gjhdq_676201/"


# In[4]:


# def invokeDriver():
#     from selenium.webdriver.firefox.options import Options
#     options = Options()
#     options.add_argument("--headless")
#     driver = webdriver.Firefox(firefox_options=options, executable_path="geckodriver.exe")
#     driver.implicitly_wait(15)
#     print("Firefox Headless Browser Invoked")
#     return(driver)
    


# In[5]:


def invokeDriver():
    from selenium.webdriver.chrome.options import Options
    options = Options()
    options.add_argument("--headless")
    options.add_argument('log-level=1')     
    driver = webdriver.Chrome(options=options, executable_path="chromedriver.exe")
    driver.implicitly_wait(15)
    print("Chrome Headless Browser Invoked")
    return(driver)


# In[6]:


country_top = []
driver = invokeDriver()
driver.get(country_root)
soup=BeautifulSoup(driver.page_source, 'lxml')

for i in soup.find(class_='gubox_class').find_all('a'):
    country_top.append(country_root + i['href'][2:])
    
country_orgs = []    
for i in country_top:
    print(i)
    driver.get(i)
    soup=BeautifulSoup(driver.page_source, 'lxml')
    for k in soup.find(class_='gubox_link').find_all('a'):
        country_orgs.append(i+k['href'][2:])


# In[7]:


subpages = [
    '发言人有关谈话',
    '发言人谈话',
    '外交掠影',
    '相关新闻',
    '重要文件',
    '重要讲话',
    '驻外报道',]


# In[8]:


topLevel = pd.DataFrame(columns=['country','page','url','lang','more','complete'])


# In[9]:


# counter = 0
# for i in country_orgs:
#     driver.get(i)
#     soup = BeautifulSoup(driver.page_source, 'lxml')
#     for k in subpages:
#         print('\n')
#         print(i)
#         print(k)
#         try:
#             href = soup.find(class_="rebox_l fl").find("a", text=k)['href']
#             if re.search("https://www.fmprc.gov.cn",href):
#                 href = href
#                 print('CHECK')
#             else:
#                 href = i + re.split('/',href)[-2] + '/'
#             print(href)
            
#             pagename = k
#             countryname = soup.find(class_='nav').find_all('a')[-2].text
#             lang = 'zh'
#             topLevel = topLevel.append({'url':href,
#                                         'page':pagename,
#                                         'country':countryname,
#                                         'lang':lang},
#                                        ignore_index=True)            
#             counter += 1
#             print(counter)
            
#         except Exception as err:
#             print('NOPE',k)
#             print(err)


# In[10]:


topLevel['url'].head()


# In[11]:


topLevel.shape


# In[12]:


topLevel.head()


# In[13]:


topLevel_orig.shape


# In[14]:


topLevel = pd.concat([topLevel_orig,topLevel],sort=False).drop_duplicates(subset='url').reset_index(drop=True)
topLevel['complete'] = 'n'
topLevel.shape


# In[15]:


documents = pd.DataFrame(columns=list(topLevel.columns) + ['nav','title','date'])


# In[16]:


def scrapeDirectories(topleveldf,documentsdf,review=False):
    topleveldf = topleveldf.copy()
    documentsdf = documentsdf.copy()
    
    driver = invokeDriver()
    print(review)
    if review:
        ind = list(topleveldf[topleveldf['complete']=='n'].index.copy())
    else:
        ind = list(topleveldf.index.copy())
    
    print("INDEX",len(list(ind)))
    for i in tqdm(topleveldf.loc[ind,].index):
        try:
            url_root = topleveldf.loc[i,'url']
            print(url_root)
            country = topleveldf.loc[i,'country']
            page = topleveldf.loc[i,'page']
            lang = topleveldf.loc[i,'lang']

            driver.get(url_root)
            soup=BeautifulSoup(driver.page_source, 'lxml')

            nav = str()
            for k in soup.find(class_='nav').find_all('a'):
                nav = nav + k.text + '_'

            try:
                more = soup.find("a", string='尾页')['href']
                print(more)
                topleveldf.loc[i,'more'] = more
            except:
                print('NO MORE')

            try:
                for j in soup.find(class_='rebox_news').find_all('li'):
                    title = j.text
                    print(title)
                    url_new = url_root+j.find('a')['href'][2:]
                    print(url_new)

                    try:
                        date = re.findall(r"\d\d\d\d\-\d\d\-\d\d",j.text)[0]
                    except:
                        date = '9999-99-99'
                    print(date)

                    documentsdf = documentsdf.append({'country':country,
                                                  'page':page,
                                                  'url':url_new,
                                                  'lang':lang,
                                                  'nav':nav,
                                                  'title':title,
                                                  'date':date,
                                                 },
                                                 ignore_index=True)
            except:
                print('No listed items')
            topleveldf.loc[i,'complete'] = 'y'
            print('YES IT WORKED')
        except:
            print('skipped')
            topleveldf.loc[i,'complete'] = 'n'
            driver.quit()
            time.sleep(5)
            driver = invokeDriver()
            continue
        print(topleveldf[topleveldf['complete']=='n'].shape)
    return(topleveldf,documentsdf)


# In[17]:


topLevel,documents = scrapeDirectories(topleveldf=topLevel,documentsdf=documents)
time.sleep(10)
topLevel,documents = scrapeDirectories(topleveldf=topLevel,documentsdf=documents,review=True)
time.sleep(20)
topLevel,documents = scrapeDirectories(topleveldf=topLevel,documentsdf=documents,review=True)
time.sleep(40)
topLevel,documents = scrapeDirectories(topleveldf=topLevel,documentsdf=documents,review=True)


# In[18]:


documents.head()


# In[19]:


topLevel['complete'].value_counts()


# In[20]:


documents.shape


# In[21]:


for i in topLevel.loc[topLevel['complete']=='n','url']:
    print(i)


# In[22]:


topLevel.to_json('topLevel.json')


# In[23]:


conn = psycopg2.connect(host="chinaquant.cofdbub8go3o.eu-west-1.rds.amazonaws.com",
                    database="postgres",
                    user="tokyo3",
                    password="evangelion")

cur = conn.cursor()

command = """
SELECT TO_CHAR(doc_date, 'yyyy-mm-dd')
FROM documents 
WHERE doc_date IS NOT NULL
ORDER BY doc_date DESC
LIMIT 1
"""

cur.execute(command)
existing = cur.fetchall()
cur.close()
conn.close()


# In[24]:


existing[0][0]


# In[25]:


timecutoff = datetime.strftime(datetime.strptime(existing[0][0], '%Y-%m-%d') - timedelta(days=1), "%Y-%m-%d")


# In[26]:


documents = documents[documents['date'] >= timecutoff].copy()
documents.shape


# In[27]:


scrapedList = [x for x in documents['url']]


# In[28]:


conn = psycopg2.connect(host="chinaquant.cofdbub8go3o.eu-west-1.rds.amazonaws.com",
                    database="postgres",
                    user="tokyo3",
                    password="evangelion")

cur = conn.cursor()

command = """
SELECT doc_url
FROM documents 
WHERE doc_url IN {}
""".format(tuple(scrapedList))

cur.execute(command)
existing = cur.fetchall()
cur.close()
conn.close()


# In[29]:


extant = [x[0] for x in existing]


# In[30]:


extant


# In[31]:


exclusions = []
for i in documents.index:
    if documents.loc[i,'url'] in extant:
        exclusions.append(i)


# In[32]:


exclusions


# In[33]:


documents = documents.loc[~documents.index.isin(exclusions),].copy()


# In[34]:


documents.shape


# In[35]:


documents.to_json('documentsUpdate.json')


# In[36]:


try:
    newAdditions = []
    for i in documents[documents['date'] != '9999-99-99'].index:
        country = documents.loc[i,'country']
        lang = documents.loc[i,'lang']
        page = documents.loc[i,'page']
        url = documents.loc[i,'url']
        nav = documents.loc[i,'nav']
        title = documents.loc[i,'title']
        date = documents.loc[i,'date']
        add = tuple([country,lang,page,url,nav,title,date])
        newAdditions.append(add)
except:
    newAdditions = []

conn = psycopg2.connect(host="chinaquant.cofdbub8go3o.eu-west-1.rds.amazonaws.com",
                    database="postgres",
                    user="tokyo3",
                    password="evangelion")
cur = conn.cursor()

command = """
    INSERT INTO documents(doc_country,doc_language,doc_page_name,doc_url,doc_nav,doc_title,doc_date)
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    """

if len(newAdditions) == 1:
    cur.execute(command,newAdditions[0])
    conn.commit()
    cur.close()
    conn.close()
elif len(newAdditions) > 1:
    cur.executemany(command,newAdditions)
    conn.commit()
    cur.close()
    conn.close()
else:
    cur.close()
    conn.close()


# In[37]:


limit = 10

def html(url):
    try:
        driver.get(url)
        html = driver.page_source
        if type(html) == str:
            return(html)
        else:
            return(None)
    except:
        return(None)


def buildEntry(index_value,url):
    t_start = time.time()    
    index = index_value
    pageSource = html(url)
    pageText = str()
    if pageSource == None:
        pageText = None
    else:
        try:
            soup=BeautifulSoup(pageSource, 'lxml')
            pageText = soup.find(class_='content').text
            if pageText == '':
                pageText = None
        except:
            pageText = None

    entry = (pageSource,
             pageText,
             int(0),
             index)

    t_end = time.time()
    duration = t_end - t_start
    if duration >= 10:
        time.sleep(np.random.randint(5,15))          
    return(entry)


def fullHarvest(limit=limit):
    t = time.time()
   
    limit = limit

    conn = psycopg2.connect(host="chinaquant.cofdbub8go3o.eu-west-1.rds.amazonaws.com",
                        database="postgres",
                        user="tokyo3",
                        password="evangelion")

    cur = conn.cursor()

    command = """
    SELECT doc_id,doc_url
    FROM documents
    WHERE doc_content IS NULL AND (doc_skip = 0 OR doc_skip IS NULL)
    ORDER BY doc_id DESC
    LIMIT {}
    """.format(limit)

    cur.execute(command)
    existing = cur.fetchall()
    cur.close()
    conn.close()

    submission = [buildEntry(int(x[0]),str(x[1])) for x in existing]    
    skiplist = []
    newSubmission = []

    for i in submission:
        if i[1] == 'ERROR':
            skiplist.append(i[3])
        elif i[1] == None:
            skiplist.append(i[3])
        elif i[0] == 'ERROR':
            skiplist.append(i[3])
        elif i[0] == None:
            skiplist.append(i[3])
        else:
            newSubmission.append(i)

    skiplist = list(np.unique(skiplist))    

    sub_len = len(newSubmission)


    conn = psycopg2.connect(host="chinaquant.cofdbub8go3o.eu-west-1.rds.amazonaws.com",
                        database="postgres",
                        user="tokyo3",
                        password="evangelion")

    cur = conn.cursor()

    if len(skiplist) > 1:
        sql = """
        UPDATE documents
        SET doc_skip = 1
        WHERE doc_id IN {};
        """.format(tuple(skiplist))
        cur.execute(sql)
        conn.commit()
    if len(skiplist) == 1:
        sql = """
        UPDATE documents
        SET doc_skip = 1
        WHERE doc_id = {};
        """.format(skiplist[0])
        cur.execute(sql)
        conn.commit()


    sql = """
        UPDATE documents
        SET doc_html = %s,doc_content = %s, doc_skip = %s
        WHERE doc_id = %s;
        """

    cur.executemany(sql,newSubmission)
    conn.commit()


    command = """
    SELECT COUNT(doc_id)
    FROM documents
    WHERE doc_content IS NULL AND (doc_skip = 0 OR doc_skip IS NULL)
    """
    #.format(doclist)
    cur.execute(command)
    count = cur.fetchall()
    cur.close()
    conn.close()

    nt = time.time()
    dt = nt - t   

    return(len(skiplist),count,dt,sub_len,existing)


counter = 0
driver = invokeDriver()

for i in tqdm(range(100)):    
    s,r,t,l,e = fullHarvest(limit=limit)
    print("{} : {:.4f}s : {} skips : {} remaining : {} counter".format(i,t,s,r,counter))
    if s > 0:
        #print(e)
        counter += 1
        if counter > 1000:
            break
        else:
            driver.quit()
            time.sleep(np.random.randint(5,10))
            driver = invokeDriver()


# In[ ]:


end = str(datetime.today())


# In[38]:


with open('logs.txt','a') as f:
        f.write(str(datetime.now()) +" " + start + " " + end +"\n")

