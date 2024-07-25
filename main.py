from typing import Any
import CrawlData
import numpy as np
import pandas as pd
import json
from datetime import date, datetime, timedelta
import re
import os
import warnings
warnings.filterwarnings('ignore')
import logging
from selenium.webdriver import Chrome

from selenium.common.exceptions import NoSuchElementException,StaleElementReferenceException, TimeoutException,NoSuchFrameException, WebDriverException, UnexpectedAlertPresentException, ElementClickInterceptedException
from selenium.webdriver.common.keys import Keys

import subprocess
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions


def check_installed(package):
    try:
        subprocess.check_call(['dpkg', '-l', package])
        return True
    except subprocess.CalledProcessError:
        return False

def to_do_logic(
    param_json: dict = None,
    ex_model_param: dict = None,
    in_dir: str = None,
    out_dir: str = None,
    ex_out_dir: str = None,
    is_training: bool = False,
    training_model_path: str = None):

    accuracy_json: dict = {}
    loss_json: dict = {}
    output_params: dict = {}
    files: list = list()
    
    # 생략가능 ----------------------------- (1)
    # logging.basicConfig(filename=f'{out_dir}{os.sep}log_file.txt', level=logging.DEBUG,
    #                 format="[ %(asctime)s | %(levelname)s ] %(message)s", force=True,
    #                 datefmt="%Y-%m-%d %H:%M:%S")
    # files.append("log_file.txt")
    # -------------------------------------- (1)

    logging.info("=================================================2024074_Test===================================================")

    logging.info("=================================================== READY ===================================================")
    logging.info(f"param_json => {param_json}")    
    logging.info(f"ex_model_param => {ex_model_param}")    
    logging.info(f"in_dir => {in_dir}")    
    logging.info(f"out_dir => {out_dir}")    
    logging.info(f"ex_out_dir => {ex_out_dir}")    
    logging.info(f"is_training => {is_training}")    
    logging.info(f"training_model_path => {training_model_path}")    
    logging.info("==============================================================================================================")

    logging.info("Start: apt update")
    update_process = subprocess.Popen(['apt', 'update'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    update_output, update_error = update_process.communicate()
    logging.info("[update output] ---- ")
    logging.info(update_output)
    logging.info("[update error] --- ")
    logging.info(update_error)
    logging.info("Finish : apt update")

    # wget 설치안되어 있는 경우, 설치
    if not check_installed('wget'):
        logging.info("Start : Install wget")
        wget_process = subprocess.Popen(['apt', 'install', 'wget'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        wget_output, wget_error = wget_process.communicate()
        logging.info("[wget output] ---- ")
        logging.info(wget_output)
        logging.info("[wget error] --- ")
        logging.info(wget_error)
        logging.info("Finish : Install wget")
    else:
        logging.info("wget is already installed")

    # google-chrome-stable 설치안되어 있는 경우, 설치
    if not check_installed('google-chrome-stable'):
        logging.info("Start : Download chrome")
        download_process = subprocess.Popen(['wget', 'https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        download_output, download_error = download_process.communicate()
        logging.info("[download_output] ---- ")
        logging.info(download_output)
        logging.info("[download_error] --- ")
        logging.info(download_error)
        logging.info("Finish : Download chrome")
    else:
        logging.info("chrome is already installed")
    
    logging.info("Start : Install chrome")
    install_process = subprocess.Popen(['apt', '-y', 'install', './google-chrome-stable_current_amd64.deb'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    install_output, install_error = install_process.communicate()
    logging.info("[install_output] ---- ")
    logging.info(install_output)
    logging.info("[install_error] --- ")
    logging.info(install_error)
    logging.info("Finish : Install chrome")

    logging.info("Start : Install chrome driver")
    service = ChromeService(executable_path=ChromeDriverManager().install()) # 최신드라이버 설치
    logging.info("Finish : Install chrome driver")

    options = ChromeOptions()
    options.add_argument("lang=ko_KR")
    options.add_argument('headless') # headless 모드(실제 브라우저 창이 표시되지 않고 백그라운드에서 실행)
    options.add_argument("--no-sandbox") # Chrome 브라우저의 보안 기능 중 하나인 "Sandbox"를 비활성화
    options.add_argument('--disable-dev-shm-usage') # Chrome 브라우저 공유 메모리 파일 시스템 사용안함 

    driver = webdriver.Chrome(service=service, options=options)

    #Open the Chromedriver
    #driver = Chrome()
    #driver.maximize_window()
    logging.info("Start Chrome")

    #Read the Keyword List
    file_name = 'DX_KATRI_크롤링대상품목키워드리스트_240401_ver1.5'

    keyword_df = pd.read_excel('{}.xlsx'.format(file_name), sheet_name='키워드')
    keyword_df = keyword_df[:1] #추후 삭제, 테스트용 데이터 인덱싱
    logging.info("Read the keyword list")

    startDate = '2024-07-01'  ## 시작날짜
    endDate = '2024-07-09'  ## 마지막날짜
    save_names = f'{out_dir}{os.sep}crawl_test_20240724v1'  ## 저장경로_명

    CrawlData.naver_blog_crawling(keyword_df, startDate, endDate, driver = driver)
    logging.info("Finish CrawldData1")
    CrawlData.naver_cafe_crawling(keyword_df, startDate, endDate, driver = driver)
    logging.info("Finish CrawldData2")
    CrawlData.naver_news_crawling(keyword_df, startDate, endDate,  driver = driver)
    logging.info("Finish CrawldData3")
    CrawlData.save_data(save_names, startDate, endDate)
    files.append("crawl_test_20240724v1.csv")
    logging.info("Finish CrawldData4")

    return accuracy_json, loss_json, output_params, files
