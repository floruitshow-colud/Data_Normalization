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

# 设置最大文件大小（单位：字节），如果超过这个大小就分包
MAX_SIZE = 10*1024*1024*1024 # 一个包10G


def process_image_files(config,image_files,rank):
    pass


def process_archive_files(config,archive_files,rank):
    rank_num = rank # 节点号
    idx_num = config.local_rank
    num = 0
    for archive in archive_files:
        M_split = str(num).zfill(5)
        ext = os.path.splitext(archive)[1].lower()
        repack_local_zip = os.path.join(config.tmp_tar_loacl_path,str(rank_num)+str(idx_num)+'_'+M_split + ext)

        archive_name = str(rank_num) + str(idx_num) + '_' + M_split + ext
























