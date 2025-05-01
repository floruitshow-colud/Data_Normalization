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
from zip_type.process_image_files import process_image_files
from zip_type.process_archive_files import process_archive_files

IMAGE_EXTENSIONS = ['.jpg','.jpeg','.png','.gif']
ARCHIVE_EXTENSIONS = ['.zip','.tar','.tar.gz']


def categorize_files_by_type(file_path_list,recursive=True,skip_dir=False):
    image_files = []
    archive_files = []
    other_files = []
    for file in file_path_list:
        file_extension = os.path.splitext(file)[1].lower()
        if file_extension in IMAGE_EXTENSIONS:
            image_files.append(file)
        elif file_extension in ARCHIVE_EXTENSIONS:
            archive_files.append(file)
        else:
            other_files.append(file)
    return image_files,archive_files,other_files


def init_main(config,rank,file_format=('.zip','.tar'),image_format = ()):

    file_path_list = config.index_list.split(",")
    image_files,archive_files,other_files = categorize_files_by_type(file_path_list)

    if image_files:
        print(f'[INFO]: {image_files[:10]} is image')
        process_image_files(config,image_files,rank)
    elif archive_files:
        print(f'[INFO]: {archive_files[:10]} is archive')
        process_archive_files(config,archive_files,rank)




















