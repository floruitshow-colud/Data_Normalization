# -*- coding: utf-8 -*-
import os
import time
import argparse
from copy import deepcopy
import multiprocessing as mp
from typing import List
from pathlib import Path

import moxing as mox
import numpy as np
from tools.config_utils import ConfigUtils
import requests
mox.file.shiftl('os', 'mox')
import io
from zip_type.classify_and_process_files i
import init_main


def run_task(config,rank):
    # 设置每个进程内可见devices

    if os.path.exists("/usr/local/Ascend/"):
        print("[INFO] Using NPU")
        os.environ['ASCEND_RT_VISIBLE_DEVICES'] = ",".join(map(str,config.device_list))
    else:
        os.environ["CUDA_VISIBLE_DEVICES"] = ','.join(map(str,config.device_list))

    # 注意一定要先设置CUDA_VISIBLE_DEVICES境変量，再导包
    init_main(config,rank)
def split_parquet_by_count(config, parquet_list, node_id, num_proc_per_node, num_total_proc):
    """将parquet数据根据其中的样本数目分给各个进程"""

    if config.full:
        config.start = 0
        config.end = len(parquet_list)
    elif config.end > len(parquet_list):
        print("[INFO] using config.end=={}".format(len(parquet_list)))
        config.end = len(parquet_list)
    range_start = config.start
    range_end = config.end
    if range_start >= range_end:
        print(f"[ERROR] invalid start(frange_start}) and end(frange_end}).")
        return []

    num_parquet = range_end - range_start
    assert len (parquet_list) >= num_parquet
    parquet_list = parquet_list[range_start: range_end ]
    if num_parquet < num_total_proc:
        print(f"[WARNI] parquet(‹num_parquet)) is less than num_total proc({num_total_proc}),some proc tasks will skip.")
    range_total = [[] for _ in range(num_total_proc)]
    range_count = np.zeros(num_total_proc)

    for par in parquet_list:
        num = 1
        curr_min_idx = range_count.argmin()
        range_total[curr_min_idx].append(par) # 贪心策略 每次将新parquet 填入当前样本负载最小的进程
        range_count[curr_min_idx] += num
    print(f'[INFO] range_count:{range_count}')
    range_node = range_total[node_id * num_proc_per_node:(node_id + 1) * num_proc_per_node]
    return range_node


def wait_complete(process_list: List [mp.Process], rank: int):
    complete_flag = False
    while not complete_flag:
        time.sleep (5)
        for p in process_list:
            if not p.is_alive() and p.exitcode!= 0:
                print('[WARNING] process failed')

        complete_flag = any([p.is_alive() for p in process_list]) is False
    print(f"rank:{rank} task complete.")
def generate_files(image_dir,recursive=False):
    glob_pattern = '**/*' if recursive else '*'
    return [
        str(p.absolute())
        for p in Path(image_dir).glob(glob_pattern)
        if not (p.name.startswith('.') or p.is_dir())
    ]
def get_parquet(path_list, file_type):
    parquet_list = []
    for path in path_list:

        parquet_list += [os.path.join(path,file) for file in mox.file.list_directory(path,recursive=True, skip_dir=False) if not file.endswith('.xlsx) and not file.endswith.csv) ]
    # parquet _list +=  [os.path.join(path, file) for file in os.listdir(path) if file.endswith(file_type)]
    print(f"(path} parquet _list is {len(parquet_list)}")
    return parquet_list
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config_file, type=str, default="config_SJZG.yaml"')
    args, unparsed = parser.parse_known_args()
    config = ConfigUtils.parse_config(args)

    os.makedirs(config.tmp_tar_local_path, exist_ok= True)
    os.makedirs(config.ex_local_zip_path, exist_ok= True)
    os.makedirs(config.ex_out_zip_path, exist_ok= True)
    os.makedirs(config.file_no_jpg, exist_ok= True)
    os.makedirs(config.tmp_img_local_path, exist_ok= True)
    os.makedirs(config.init_parquet_local, exist_ok= True)
    os.makedirs('/cache/txt', exist_ok= True)


    mp.set_start_method (spawn")
    world_size =
    intlos.environ.get("MA_NUM_HOSTS",
    "1"））
    rank =
    intlos.environ.get("VC_TASK_INDEX",

    num_devices =
    intlos.environ.get("MA_NUM_GPUS",
    "1"））
    # num_proc_per_node =
    num_devices
    num proc_per_node = config.num_proc_per_node
    num_total_proc =
    num_proc_per_node * world_size
    num_device_per_task = 1
    tar_list =
    get_parquet([config.image_root_path
    ],")
    600359
    print(f'[INFO] len of parquet_list:{len(tar_list)})
    tar_list = sorted (tar _list)
    range_list = split_parquet_by_count/config, tar_list, rank, num_proc_per_node, num_total_proc)
    process _list: ListImp.Process] = 0]
    for ida, r in enumerate(range_list):

    tar_list = sorted(tar_list)
    range_list =
    split_parquet_by_count/config, tar_list, rank, num_proc_per_node, num_total_proc)
    process_list: Listlmp.Process」 =1
    for id, r in enumerate(range_list): print(f"[INFO] node_idx:trank),
    proc:fidx), split range _list:(len(r)},
    {r [:101}... )
    cfg_ = deepcopy(config)
    cfg_local_rank = idx
    cfg_.device_list = list(li + idx *
    num_device_per_task for i in
    range(num_device_per_task)])
    cfg_index_list = , join(r)
    p = mp.Process(target=run_task,
    args=(cfg_, rank), daemon=False)
    process_list.append(p)
    for p in process_list:
    p.start)
    wait_complete(process_list, rank)