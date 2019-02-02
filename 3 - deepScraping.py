
# coding: utf-8

# In[2]:


import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
# from selenium.webdriver.firefox.options import Options
from selenium.webdriver.chrome.options import Options
import re

# %matplotlib inline


# In[3]:


directory = pd.read_json('directory.json').sort_index()
documents = pd.read_json('documents.json')


# In[4]:


directory.head()


# In[5]:


documents.head()


# In[6]:


directory.shape


# In[7]:


documents.shape


# In[8]:


# options = Options()
# options.add_argument("--headless")
# driver = webdriver.Firefox(firefox_options=options, executable_path="geckodriver.exe")
# print("Firefox Headless Browser Invoked")


# In[9]:


options = Options()
options.add_argument("--headless")
options.add_argument('log-level=1')     
driver = webdriver.Chrome(options=options, executable_path="chromedriver.exe")
print("Chrome Headless Browser Invoked")


# In[10]:


try:
    subdirectory_progress = pd.read_json('subdirectory_progress.json')
except:
    subdirectory_progress = pd.DataFrame(columns=['sub_url'])
    
try:
    directory_progress = pd.read_json('directory_progress.json')
except:
    directory_progress = pd.DataFrame(columns=['dir_url'])


# In[11]:


subdirectory_progress.head()


# In[12]:


subdirectory_progress.shape


# In[13]:


directory_progress.shape


# In[14]:


directory_progress.head()


# In[28]:


directory.loc[directory['more'].notnull(),].shape


# In[29]:


directory.head()


# In[16]:


counter = 0
dir_counter = 0
problems = []
for i in directory.loc[directory['more'].notnull(),'more'].index:
    try:
        url_root = directory.loc[i,'url']
        if url_root in list(directory_progress['dir_url']):
            print('SKIP DIR')
            continue
        else:
            print('\n','ROOT')
            print(url_root)
            print('\n','SUBS')
            country = directory.loc[i,'country']
            page = directory.loc[i,'page']
            lang = directory.loc[i,'lang']

            subpage_count = int(re.split(r'_|\.',directory.loc[i,'more'])[1])
            subpages = np.arange(subpage_count)+1
            subpages_urls = [(url_root+'default_'+str(m)+'.shtml') for m in subpages]
            
            dir_counter += 1

            for h in subpages_urls:
                if h in list(subdirectory_progress['sub_url']):
                    print('SKIP SUB')
                    continue
                else:                
                    print(h,'\n')
                    driver.get(h)
                    soup=BeautifulSoup(driver.page_source, 'lxml')

                    nav = str()
                    for k in soup.find(class_='nav').find_all('a'):
                        nav = nav + k.text + '_'

            #         try:
            #             more = soup.find("a", string='尾页')['href']
            #             print(more)
            #             directory.loc[i,'more'] = more
            #         except:
            #             print('NO MORE')

                    for j in soup.find(class_='rebox_news').find_all('li'):
                        title = j.text
                        url_new = url_root+j.find('a')['href'][2:]
                        print(url_new)

                        try:
                            date = re.findall(r"\d\d\d\d\-\d\d\-\d\d",j.text)[0]
                        except:
                            date = '9999-99-99'
                        print(date)

                        documents = documents.append({'country':country,
                                                      'page':page,
                                                      'url':url_new,
                                                      'lang':lang,
                                                      'nav':nav,
                                                      'title':title,
                                                      'date':date,
                                                     },
                                                     ignore_index=True)
                        subdirectory_progress = subdirectory_progress.append({'sub_url':h},ignore_index=True)
                        counter += 1
                        print(dir_counter)
                        print(counter,'\n')
                        
                directory_progress = directory_progress.append({'dir_url':url_root},ignore_index=True)
    except:
        problems.append(i)


# In[17]:


documents.shape


# In[18]:


documents.tail()


# In[19]:


documents.shape


# In[20]:


documents = documents.drop_duplicates()
subdirectory_progress = subdirectory_progress.drop_duplicates()
directory_progress = directory_progress.drop_duplicates()


# In[21]:


documents.shape


# In[30]:


subdirectory_progress = pd.DataFrame(columns=['sub_url'])
directory_progress = pd.DataFrame(columns=['dir_url'])


# In[22]:


documents.to_json('documents.json')


# In[23]:


subdirectory_progress.to_json('subdirectory_progress.json')


# In[24]:


directory_progress.to_json('directory_progress.json')


# In[25]:


pd.Series(problems).to_json('problems.json')


# In[26]:


problems


# In[27]:


type(documents.loc[1,'url'])

