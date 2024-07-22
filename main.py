import numpy as np
import pandas as pd
import logging
import json
from datetime import date, datetime, timedelta
import os
import warnings
warnings.filterwarnings('ignore')



# Press the green button in the gutter to run the script.
def to_do_logic(
    param_json: dict = None,
    ex_model_param: dict = None,
    in_dir: str = None,
    out_dir: str = None,
    ex_out_dir: str = None,
    is_training: bool = False,
    training_model_path: str = None,
) -> Any:
    
    
    accuracy_json: dict = {}
    loss_json: dict = {}
    output_params: dict = {}
    files: list = list()

    # in_dir = "/data/jupyter_config/KATRI_2024/01_Data/01_Original/SFR_011" # 추후 삭제
    # out_dir = "/data/jupyter_config/KATRI_2024/01_Data/02_Proprecessing/SFR_011" # 추후 삭제

    # 로고 파일 생성
    file_nm = f'log_file_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
    logging.basicConfig(filename=f'{out_dir}{os.sep}{file_nm}.txt', level=logging.INFO,
                    format="[ %(asctime)s | %(levelname)s ] %(message)s", force=True,
                    datefmt="%Y-%m-%d %H:%M:%S")

    files.append(f'{file_nm}.txt')
    logger = logging.getLogger()

    logger.info("=================================================== READY ===================================================")
    logger.info(f"param_json => {param_json}")    
    logger.info(f"ex_model_param => {ex_model_param}")    
    logger.info(f"in_dir => {in_dir}")    
    logger.info(f"out_dir => {out_dir}")    
    logger.info(f"ex_out_dir => {ex_out_dir}")    
    logger.info(f"is_training => {is_training}")    
    logger.info(f"training_model_path => {training_model_path}")    
    logger.info("==============================================================================================================")



    #Read the Keyword List
    file_name = 'test_sample_data'

    keyword_df = pd.read_csv('{}.csv'.format(file_name))
    logger.info(keyword_df.head)
    logger.info ("---------------Process is completed---------------")

    return accuracy_json, loss_json, output_params, files
