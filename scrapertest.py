from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import os
import psycopg2

def main():
    options = Options()
    options.add_argument("--headless")
    print('Getting directory')
    scriptPath = os.getcwd()
    driverPath = scriptPath + '\\' + 'geckodriver.exe'

    print('Starting driver')
    driver = webdriver.Firefox(options=options, executable_path=driverPath)
    driver.get("https://www.bbc.com/news")
    html = driver.page_source
    soup=BeautifulSoup(html, 'lxml')
    pageText = soup.find(class_='gs-c-promo-heading gs-o-faux-block-link__overlay-link gel-paragon-bold nw-o-link-split__anchor').text
    
    conn = psycopg2.connect(host="dbtest123.cofdbub8go3o.eu-west-1.rds.amazonaws.com",
                        database="dbtestname",
                        user="tokyo6",
                        password="shinjiikari")

    cur = conn.cursor()

    print('PageText: '.format(pageText))

    command = """
    INSERT INTO testtable(content,datetime)
    VALUES(%s,current_timestamp);
    """
    

    cur.execute(command,(pageText,))
    print('Commiting to DB')
    conn.commit()

    cur.close()
    conn.close()
    print('Finished')


if __name__ == '__main__':
    main()