import os
import time
import argparse
import multiprocessing as mp
from copy import deepcopy
from typing import List
import numpy as np

import tarfile
import zipfile
import json
import uuid
import pandas as pd

from tqdm import tqdm
from tools.config_utils import ConfigUtils
import requests
import io
from zip_type.extract_file import zip_ex_out,extract_tar_folder
# 设置最大文件大小（单位：字节），如果超过这个大小就分包
MAX_SIZE = 10*1024*1024*1024 # 一个包10G


def extract_archive(local_in_zip_path,out_zip_path,archive,config):

    """根据文件类型解压文件"""
    ext = os.path.splitext(local_in_zip_path)[1].lower()
    if ext == '.zip':

        # 判断是否存在和压缩包在同一级的 meta文件
        json_file = archive.replace('.zip', '.json')
        txt_file = archive.replace('.zip', '.txt')
        if os.path.exists(json_file):
            json_file=json_file

        elif os.path.exists(txt_file):
            json_file=txt_file
        else:
            json_file=None

        print(f'[INFO]: Started extract archieve {local_in_zip_path}')
        # 判断是否需要重打包
        txt_dict_list = zip_ex_out(local_in_zip_path,out_zip_path,archive,config,json_file)
        return txt_dict_list


    elif ext in ['.tar', '.tar.gz', '.tar.bz2']:
        json_file = archive.replace('.tar', '.json')
        if os.path.exists(json_file):
            json_file=json_file
        else:
            json_file=None
        txt_dict_list = extract_tar_folder(local_in_zip_path,out_zip_path,archive,config,json_file)
        return txt_dict_list

    # elif ext == '.rar':
    #     with rarfile.RarFile(local_in_zip_path) as rar:
    #         # rar.extractall(extract_dir)
    #         pass

    else:
        raise ValueError(f"Unsupported file format: {ext}")


def process_archive_files(config,archive_files,rank):
    rank_num = rank # 节点号
    idx_num = config.local_rank
    num = 0
    for archive in archive_files:
        M_split = str(num).zfill(5)
        ext = os.path.splitext(archive)[1].lower()
        repack_local_zip = os.path.join(config.tmp_tar_loacl_path,str(rank_num)+str(idx_num)+'_'+M_split + ext)

        archive_name = str(rank_num) + str(idx_num) + '_' + M_split + ext
























