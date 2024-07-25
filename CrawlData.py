import pandas as pd
import numpy as np
import json
from datetime import date, datetime, timedelta

import re
import os
import time
import logging
## warning
import warnings
warnings.filterwarnings('ignore')

from selenium.webdriver import Chrome
from selenium.common.exceptions import NoSuchElementException,StaleElementReferenceException, TimeoutException,NoSuchFrameException, WebDriverException, UnexpectedAlertPresentException, ElementClickInterceptedException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


#### 네이버 블로그 페이지 URL 접속
def naver_blog_get_url(page_num, start_date, end_date, query, driver):

    basic_url = 'https://section.blog.naver.com/Search/Post.naver?pageNo={}&rangeType=PERIOD&orderBy=recentdate&'.format(
        page_num)
    custom_url = 'startDate={}&endDate={}&keyword={}'.format(start_date, end_date, query)
    url = basic_url + custom_url;
    driver.get(url)


#### 포스팅 스크래핑
def naver_blog_scraping(start_date, end_date, query, keyword, driver):
    global li
    global total_li
    page_num = 1;
    last_element = '';
    data_len = 0  ## 초기값 세팅
    while True:
        naver_blog_get_url(page_num, start_date, end_date, query, driver)
        try:
            element_css = '#content > section > div.area_list_search > div > div > div.info_post'
            element_box = WebDriverWait(driver, 5).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, element_css)))
            li = [t.text.split('\n') for t in element_box]
            url_li = [t.find_element(By.CSS_SELECTOR, 'div.desc > a.desc_inner').get_attribute('href') for t in
                      element_box]
            df = pd.DataFrame(li, columns=['SJ', 'CN', 'CRTR_ID', '작성자블로그', 'NTCE_YMD'])
            df.insert(0, 'MEDIA', '블로그');
            df.insert(0, 'LINK_URL', url_li);
            df.insert(0, 'PRDLST', keyword)
            total_li.append(df);
            data_len += len(df);
            time.sleep(1)
            #print('progress:      ', round(data_len / blog_buzz * 100, 1), '%      page:      ', page_num,
            #      '      length:      ', data_len, '      ', end='\r')
            progress = round(data_len / blog_buzz * 100, 1)
            logging.info('progress: %s%%  page: %s  length: %s', progress, page_num, data_len)
            if last_element == li[-1]:
                break
            else:
                last_element = li[-1]
                page_num += 1
        except:
            break


#### 자동화 함수
def naver_blog_crawling(keyword_df, startDate, endDate, driver):
    global total_blog
    global total_li
    global blog_buzz
    total_li = []
    #for ind in tqdm(range(0, len(keyword_df))):
    for ind in range(0, len(keyword_df)):    
        keyword = keyword_df['PRDLST'][ind]
        query = keyword_df['쿼리'][ind].replace('+', '%2B')  ## 쿼리 문자 대체

        ## 입력한 기간에 대해서 블로그 토탈 버즈량 체크
        naver_blog_get_url(1, startDate, endDate, query, driver)
        element_css = 'em.search_number'
        blog_buzz = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, element_css))).text
        blog_buzz = int(blog_buzz.replace(',', '').replace('건', ''))

        ## (1). 4천건 이상인 경우: 블로그페이지에서 하루씩 크롤링 진행
        if blog_buzz > 4000:
            for one_day in pd.date_range(start=startDate, end=endDate):
                one_day = str(one_day).split(' ')[0]
                naver_blog_scraping(one_day, one_day, query, keyword, driver)

        ## (2). 4천건 미만인 경우: 총기간 필터조건에 넣고 일괄적으로 크롤링 진행
        else:
            naver_blog_scraping(startDate, endDate, query, keyword, driver)
    ## 전처리
    total_df = pd.concat(total_li)

    total_df1 = total_df[~total_df['NTCE_YMD'].isna()]
    total_df2 = total_df[total_df['NTCE_YMD'].isna()]
    total_df2 = total_df2.rename({'CN': 'CRTR_ID', 'CRTR_ID': '작성자블로그2', '작성자블로그': 'NTCE_YMD2', 'NTCE_YMD': 'CN2'},
                                 axis=1)
    total_df2 = total_df2.rename({'CRTR_ID2': 'CRTR_ID', '작성자블로그2': '작성자블로그', 'NTCE_YMD2': 'NTCE_YMD', 'CN2': 'CN'},
                                 axis=1)
    total_df = pd.concat([total_df1, total_df2], axis=0)

    total_df['NTCE_YMD'] = total_df['NTCE_YMD'].apply(
        lambda x: datetime.now().strftime(format='%Y. %m. %d.') if '전' in x else x)
    total_df['NTCE_YMD'] = pd.to_datetime(total_df['NTCE_YMD'], format='%Y. %m. %d.')

    total_df = total_df[total_df['NTCE_YMD'] <= datetime.strptime(endDate, '%Y-%m-%d')]
    total_blog = total_df.drop_duplicates().reset_index(drop=True)


date_re = re.compile('[0-9]{4}.[0-9]{2}.[0-9]{2}.')


#### 포스팅 스크래핑
def naver_cafe_scraping(one_day, query, keyword, driver):
    global df
    global total_li
    global first_element
    page_num = 1;
    first_element = '';
    data_len = 0  ## 초기값 세팅
    while True:
        basic_url = 'https://search.naver.com/search.naver?cafe_where=articleg&query={}'.format(query)
        url = basic_url + '&sm=mtb_opt&ssc=tab.cafe.all&nso=so:dd,p:from' + one_day + 'to' + one_day + '&start=' + str(
            page_num)
        driver.get(url)
        try:
            element_css = '#main_pack > section > div.api_subject_bx > ul > li'
            element_box = WebDriverWait(driver, 5).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, element_css)))
            user_li = [
                t.find_element(By.CSS_SELECTOR, 'div > div.user_box').text.replace('문서 저장하기\n', '').replace('대표\n',
                                                                                                            '').split(
                    '\n') for t in element_box]
            title_li = [t.find_element(By.CSS_SELECTOR, 'div > div.detail_box > div.title_area').text for t in
                        element_box]
            mention_li = [t.find_element(By.CSS_SELECTOR, 'div > div.detail_box > div.dsc_area').text for t in
                          element_box]
            url_li = [t.find_element(By.CSS_SELECTOR, 'div > div.detail_box > div.title_area > a').get_attribute('href')
                      for t in element_box]

            df1 = pd.DataFrame(user_li, columns=['CRTR_ID', 'NTCE_YMD'])
            df1['NTCE_YMD'] = one_day
            df2 = pd.DataFrame({'LINK_URL': url_li, 'SJ': title_li, 'CN': mention_li})
            df = pd.concat([df2, df1], axis=1)

            df.insert(0, 'MEDIA', '카페');
            df.insert(0, 'PRDLST', keyword)

            total_li.append(df);
            data_len += len(df);
            time.sleep(1)
            #print('length:      ', data_len, '      ', end='\r')
            logging.info('length:      %s', data_len)
            if first_element == df.iloc[0, 2:].to_list():
                break
            elif page_num == 990:
                page_num = 1000
            elif page_num >= 1000:
                break
            else:
                first_element = df.iloc[0, 2:].to_list()
                page_num += 30
        except:
            break


#### 자동화 함수
def naver_cafe_crawling(keyword_df, startDate, endDate, driver):
    global total_cafe
    global total_li
    total_li = []
    #for ind in tqdm(range(0, len(keyword_df))):
    for ind in range(0, len(keyword_df)):
        keyword = keyword_df['PRDLST'][ind]
        query = keyword_df['쿼리'][ind].replace('+', '%2B')  ## 쿼리 문자 대체

        for one_day in pd.date_range(start=startDate, end=endDate):
            one_day = str(one_day).split(' ')[0].replace('-', '')
            naver_cafe_scraping(one_day, query, keyword, driver)

    ## 전처리
    total_df = pd.concat(total_li)

    total_df1 = total_df[~total_df['NTCE_YMD'].isna()]
    total_df2 = total_df[total_df['NTCE_YMD'].isna()]
    total_df2['CRTR_ID'] = total_df2['CRTR_ID'].apply(lambda x: date_re.sub('', str(x), count=1))
    total_df2['NTCE_YMD'] = total_df2['CRTR_ID'].apply(lambda x: date_re.findall(str(x))[0])
    total_df = pd.concat([total_df1, total_df2], axis=0)

    total_df['NTCE_YMD'] = pd.to_datetime(total_df['NTCE_YMD'], format='%Y%m%d')

    total_cafe = total_df.drop_duplicates().reset_index(drop=True)

#### 포스팅 스크래핑
def naver_news_scraping(one_day, query, keyword, driver):
    global df
    global total_li
    global first_element
    page_num = 1
    first_element = ''
    data_len = 0  ## 초기값 세팅
    while True:
        basic_url = 'https://search.naver.com/search.naver?ssc=tab.news.all&where=news&query={}'.format(query)
        url = basic_url + '&sm=tab_jum&nso=so:dd,p:from' + one_day + 'to' + one_day + '&start=' + str(page_num)
        driver.get(url)
        try:
            element_css = 'div.news_area > div.news_info > div.info_group > a.info.press'
            author_box = WebDriverWait(driver, 3).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, element_css)))
            author_name_li = [t.text.replace('언론사 선정', '') for t in author_box]
            #             author_url_li = [t.get_attribute('href') for t in author_box]

            element_css = 'div.news_area > div.news_contents'
            text_box = WebDriverWait(driver, 3).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, element_css)))
            text_li = [t.text.replace('동영상\n', '').split('\n') for t in text_box]
            url_li = [t.find_element(By.CSS_SELECTOR, 'a').get_attribute('href') for t in text_box]

            df1 = pd.DataFrame({'LINK_URL': url_li, 'CRTR_ID': author_name_li})
            df2 = pd.DataFrame(text_li, columns=['SJ', 'CN'])
            df = pd.concat([df1, df2], axis=1)
            df['NTCE_YMD'] = one_day
            df.insert(0, 'MEDIA', '뉴스')
            df.insert(0, 'PRDLST', keyword)

            total_li.append(df)
            data_len += len(df)
            time.sleep(1)
            #print('length:      ', data_len, '      ', end='\r')
            logging.info('length:      %s', data_len)
            if first_element == df.iloc[0, 1:].to_list():
                break
            elif page_num >= 10000:
                break
            else:
                first_element = df.iloc[0, 1:].to_list()
                page_num += 10
        except:
            break


#### 카페 자동화 함수
def naver_news_crawling(keyword_df, startDate, endDate, driver):
    global total_news
    global total_li
    total_li = []
    #for ind in tqdm(range(0, len(keyword_df))):
    for ind in range(0, len(keyword_df)):
        keyword = keyword_df['PRDLST'][ind]
        query = keyword_df['쿼리'][ind].replace('+', '%2B')  ## 쿼리 문자 대체

        for one_day in pd.date_range(start=startDate, end=endDate):
            one_day = str(one_day).split(' ')[0].replace('-', '')
            naver_news_scraping(one_day, query, keyword, driver)

    ## 전처리
    total_df = pd.concat(total_li)
    total_df['NTCE_YMD'] = total_df['NTCE_YMD'].apply(
        lambda x: datetime.now().strftime(format='%Y%m%d') if '전' in x else x)
    total_df['NTCE_YMD'] = pd.to_datetime(total_df['NTCE_YMD'], format='%Y%m%d')

    total_news = total_df.drop_duplicates().reset_index(drop=True)

    #### 포스팅 스크래핑
def naver_news_scraping(one_day, query, keyword, driver):
    global df
    global total_li
    global first_element
    page_num = 1
    first_element = ''
    data_len = 0  ## 초기값 세팅
    while True:
        basic_url = 'https://search.naver.com/search.naver?ssc=tab.news.all&where=news&query={}'.format(query)
        url = basic_url + '&sm=tab_jum&nso=so:dd,p:from' + one_day + 'to' + one_day + '&start=' + str(page_num)
        driver.get(url)
        try:
            element_css = 'div.news_area > div.news_info > div.info_group > a.info.press'
            author_box = WebDriverWait(driver, 3).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, element_css)))
            author_name_li = [t.text.replace('언론사 선정', '') for t in author_box]
            #             author_url_li = [t.get_attribute('href') for t in author_box]

            element_css = 'div.news_area > div.news_contents'
            text_box = WebDriverWait(driver, 3).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, element_css)))
            text_li = [t.text.replace('동영상\n', '').split('\n') for t in text_box]
            url_li = [t.find_element(By.CSS_SELECTOR, 'a').get_attribute('href') for t in text_box]

            df1 = pd.DataFrame({'LINK_URL': url_li, 'CRTR_ID': author_name_li})
            df2 = pd.DataFrame(text_li, columns=['SJ', 'CN'])
            df = pd.concat([df1, df2], axis=1)
            df['NTCE_YMD'] = one_day;
            df.insert(0, 'MEDIA', '뉴스')
            df.insert(0, 'PRDLST', keyword)

            total_li.append(df)
            data_len += len(df)
            time.sleep(1)
            #print('length:      ', data_len, '      ', end='\r')
            logging.info('length:      %s', data_len)
            if first_element == df.iloc[0, 1:].to_list():
                break
            elif page_num >= 10000:
                break
            else:
                first_element = df.iloc[0, 1:].to_list()
                page_num += 10
        except:
            break

#### 자동화 함수
def naver_news_crawling(keyword_df, startDate, endDate, driver):
    global total_news
    global total_li
    total_li = []
    #for ind in tqdm(range(0, len(keyword_df))):
    for ind in range(0, len(keyword_df)):
        keyword = keyword_df['PRDLST'][ind]
        query = keyword_df['쿼리'][ind].replace('+', '%2B')  ## 쿼리 문자 대체

        for one_day in pd.date_range(start=startDate, end=endDate):
            one_day = str(one_day).split(' ')[0].replace('-', '')
            naver_news_scraping(one_day, query, keyword, driver)

    ## 전처리
    total_df = pd.concat(total_li)
    total_df['NTCE_YMD'] = total_df['NTCE_YMD'].apply(
        lambda x: datetime.now().strftime(format='%Y%m%d') if '전' in x else x)
    total_df['NTCE_YMD'] = pd.to_datetime(total_df['NTCE_YMD'], format='%Y%m%d')

    total_news = total_df.drop_duplicates().reset_index(drop=True)

    ## 최종 저장
def save_data(save_names, startDate, endDate):
    global fin_df
    fin_df = pd.concat([total_blog, total_cafe], axis=0)
    fin_df = pd.concat([fin_df, total_news], axis=0)

    ## 날짜 어긋나게 수집된 데이터 삭제
    fin_df = fin_df[(fin_df['NTCE_YMD'] >= datetime.strptime(startDate, '%Y-%m-%d')) | (
                fin_df['NTCE_YMD'] <= datetime.strptime(endDate, '%Y-%m-%d'))]

    ## 중복 제거 및 필요없는 컬럼 삭제
    fin_df = fin_df.drop_duplicates(['PRDLST', 'SJ', 'CN', 'CRTR_ID', 'NTCE_YMD'])
    fin_df.drop(['작성자블로그'], axis=1, inplace=True)

    fin_df.to_csv('{}.csv'.format(save_names), index=False, encoding= 'utf-8-sig')
