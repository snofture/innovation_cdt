#-*- coding:utf-8 -*-
'''
__author__ = 'Men Li'
'''

import os
import sys
import yaml
import datetime
import time
#import opencs
import subprocess
import item_fourth_cate as ifc
from attributes_cleaner.attrs_cleaner import *
import multiprocessing
import logging

def get_logger(run_dt):
    # current dir path
    cwd = os.getcwd()
    auto_log_path = cwd+"/scanlogs"
    if os.path.exists(auto_log_path) is False:
        os.makedirs(auto_log_path)
    log_file = auto_log_path + "/%s.log" % run_dt
    # new logger
    logger = logging.getLogger("%s" % run_dt)
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(log_file, mode="a", encoding='utf-8')
    fh.setLevel(logging.INFO)
    fmt = '%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s'
    datefmt = '%a, %d %b %Y %H:%M:%S'
    formatter = logging.Formatter(fmt, datefmt)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger

def scan_table():
    try:
        # scan app.app_ai_wric_data_load_done
        dt_cid3_list = []
        query = "select * from app.app_ai_wric_data_load_done where status='0'"
        cmd = '''hive -e "%s" '''%(query)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status == 0:
            logger.info("app.app_ai_wric_data_load_done table download successful")
            output = output.strip('\r\t\n').split('\n')
            for out in output:
                out = out.strip('\r\t\n').split('\t')
                if len(out) == 3:
                    dt = out[1]
                    cid = out[2]
                    dt_cid3_list.append([dt,cid])
        else:
            logger.error("app.app_ai_wric_data_load_done table download failed")
        # multi_load
        logger.info("update data_load_done table")
        multi_insert(dt_cid3_list)
        # multi cal
        logger.info("cal cdt and switch start")
        calc_start(dt_cid3_list)
    except Exception as e:
        logger.error(e)

def multi_insert(dt_cids_list):
    try:
        insert_result = []
        pool = multiprocessing.Pool(processes=20)
        for dt,cid in dt_cids_list:
            insert_result.append(pool.apply_async(insert,(dt,cid)))
        pool.close()
        pool.join()
        for insert_r in insert_result:
            logger.info(insert_r.get())
    except Exception as e:
        logger.error(e)


def insert(dt,cid):
    try:
        query = "insert overwrite table app.app_ai_wric_data_load_done partition(dt='%s',cid3='%s') values ('1') "%(dt,cid)
        cmd = '''hive -e "%s" '''%(query)
        status = subprocess.call(cmd,shell=True)
        if status == 0:
            return "%s %s app_ai_wric_data_load_done insert successful"%(dt,cid)
        else:
            return "ERROR: %s %s status: %s app_ai_wric_data_load_done insert failed"%(dt,cid,status)
    except Exception as e:
        return "ERROR: %s %s %s app_ai_wric_data_load_done insert failed"%(dt,cid,e)

# calculation starts
def calc_start(dt_cid3_list):
    try:
        pool = multiprocessing.Pool(processes=20)
        for dt_cid in dt_cid3_list:
            cid3 = dt_cid[1]
            pool.apply_async(run,(cid3,))
        pool.close()
        pool.join()
    except Exception as e:
        logger.error(e)


def run(cid3):
    #param_file_brand = 'params/brand_%s.yaml' % (cid3,)
    param_file_sku = 'params/sku_%s.yaml' % (cid3,)
    params = yaml.load(open(param_file_sku,'r'))
    log_file = get_log_file(param_file_sku)
    open(log_file, 'w').close()
    lvl = params['scope_type']
    pnames_sku = []
    try:
        if lvl =='lvl4':
            code = clean_attributes(params)
            f = open(log_file, 'a')
            if code != 'success':
                f.write('attributes cleaner failed!\n')
                sys.exit()
            else:
                f.write('attributes cleaner success!\n')
            f.close()
            pnames_sku = ifc.main(params)
        else:
            pnames_sku = [param_file_sku]
    except Exception as e:
        print(e)
    # 计算替代性和cdt
    for pname in pnames_sku:
        try:
            # cal switch, ord or log
            if params['ord_log'] == 'ord':
                run_command(["../../chenzhiquan/anaconda3/bin/python", "switch.py", pname], log_file)
                print('switch.py calculation done')
            elif params['ord_log'] == 'log':
                run_command(['spark-submit','--master yarn','--deploy-mode','client','--driver-memory','4G',
                            '--executor-cores','5','--num-executors',' 200',' --executor-memory','10G', '--conf',
                            'spark.buffer.pageSize=16m','--conf','spark.default.parallelism=200','switch_log_spark.py',pname],log_file)
                print('switch_log_spark.py calculation done')
            # cal cdt
            run_command(["../../chenzhiquan/anaconda3/bin/python","cdt.py",pname],log_file)
            print('cdt calculation done')
        except Exception as e:
            fw = open(log_file,"a")
            fw.write(pname + ": Error "+e)
            fw.close()
    run_command(["../../chenzhiquan/anaconda3/bin/python", "check_and_to_table.new.py", param_file_sku], log_file)


def run_command(args, log_file=None):
    if log_file==None:
        log_file = get_log_file(args[-1])
    f = open(log_file, 'a')
    status = subprocess.call(args, stdout=f, stderr=f)
    f.close()
    return status

def get_log_file(param_file):
    params = yaml.load( open(param_file, 'r') )
    return params['log_file']

if __name__ == '__main__':
    # read command line arguments
    #while True:
        #try:
    run_dt = time.strftime("%Y-%m-%d", time.localtime())
    logger = get_logger(run_dt)
    scan_table()
            #time.sleep(3600)
        #except:
            #continue
'''
    n = len(sys.argv) - 1
    if n < 1:
        print('Usage: \n    python computation_pipe.py cid3 [lvl4]\n')
        sys.exit()
    else:
        cid3 = sys.argv[1]
        lvl = n==2 and sys.argv[2] or 'lvl3'
    run(cid3,lvl)
'''