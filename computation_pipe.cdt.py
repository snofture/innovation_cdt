#-*- coding:utf-8 -*-
__author__ = 'xiajiguang1'

import os
import sys
import yaml
import opencs
import subprocess
import item_fourth_cate as ifc
from attributes_cleaner.attrs_cleaner import *
from multiprocessing import Pool

def scan_table():
    pre_dt_cid3={}
    pre_query = 'select * from app.app_ai_wric_pre_data_load_done'
    pre_cmd = '''hive -e "%s"''' %(pre_query)
    (status,output) = subprocess.getstatusoutput(pre_cmd)
    if status == 0:
        print("app.app_ai_wric_pre_data_load_done table download successful")
        pre_output = output.strip('\r\t\n').split('\n')
        for pre_out in pre_output:
            pre_out = pre_out.strip('\r\t\n').split('\t')
            pre_cid_status = pre_out[0]
            pre_dt = pre_out[1]
            pre_cid3 = pre_out[2]
            if pre_cid_status == '1':
                if pre_dt_cid3.has_key(pre_dt):
                    pre_dt_cid3[pre_dt].append(pre_cid3)
                else:
                    pre_dt_cid3[pre_dt] = []
                    pre_dt_cid3[pre_dt].append(pre_cid3)
    else:
        print("app.app_ai_wric_pre_data_load_done table download failed")

    dt_cid3 = {}
    query = 'select * from app.app_ai_wric_data_load_done'
    cmd = '''hive -e "%s"''' % (query)
    (status, output) = subprocess.getstatusoutput(cmd)
    if status == 0:
        print("app.app_ai_wric_data_load_done table download successful")
        output = output.strip('\r\t\n').split('\n')
        for out in output:
            out = out.strip('\r\t\n').split('\t')
            cid_status = out[0]
            dt = out[1]
            cid = out[2]
            if cid_status == '1':
                if dt_cid3.has_key(dt):
                    dt_cid3[dt].append(cid)
                else:
                    dt_cid3[dt] = []
                    dt_cid3[dt].append(cid)
    else:
        print("app.app_ai_wric_data_load_done table download failed")

    to_dt_cid3 = {}
    for dt in dt_cid3:
        cid3_all_list = dt_cid3[dt]
        if pre_dt_cid3.has_key[dt]:
            cid3_fsh_list = pre_dt_cid3[dt]
            cid3_run_list = [cid for cid in cid3_all_list if cid not in cid3_fsh_list]
            to_dt_cid3[dt] = cid3_run_list
        else:
            to_dt_cid3[dt] = cid3_all_list

    for dt in to_dt_cid3:
        cid3_to_run = to_dt_cid3[dt]
        result = []
        for cid3 in cid3_to_run:
            result.append(pool.apply_async(run,(cid3,)))
        pool.close()
        pool.join()
        with opencs('compute.log','a') as fp:
            for res in result:
                fp.write(res.get()+'\n')
            print('subprocess done!')


def run(cid3):
    #param_file_brand = 'params/brand_%s.yaml' % (cid3,)
    param_file_sku = 'params/sku_%s.yaml' % (cid3,)
    params = yaml.open(load(param_file_sku,'r'))
    log_file = get_log_file(param_file_sku)
    open(log_file, 'w').close()
    lvl = params['scope_type']

    if lvl =='lvl4':
        params_sku = yaml.load(open(param_file_sku, 'r') )
        code = clean_attributes(params_sku)
        f = open(log_file, 'a')
        if code != 'success':
            f.write('attributes cleaner failed!\n')
            sys.exit()
        else:
            f.write('attributes cleaner success!\n')
        f.close()
        pnames_sku = ifc.main(params_sku)
        #params_brand = yaml.load( open(param_file_brand, 'r') )
        #pnames_brand = ifc.main(params_brand)
    else:
        pnames_sku = [param_file_sku]
        #pnames_brand = [param_file_brand]

    params = yaml.load(open(param_file_sku, 'r'))

    # 计算替代性和cdt
    for pname in pnames_sku:
        try:
            # cal switch, ord or log
            if params['ord_log'] == 'ord':
                run_command(["../../chenzhiquan/anaconda3/bin/python", "switch.py", pname], log_file)
            elif params['ord_log'] == 'log':
                run_command(['spark-submit','--master yarn','--deploy-mode','client','--driver-memory','4G',
                            '--executor-cores','5','--num-executors',' 200',' --executor-memory','10G', '--conf',
                            'spark.buffer.pageSize=16m','--conf','spark.default.parallelism=200','switch_log_spark.py',pname],log_file)
            # cal cdt
            run_command(["../../chenzhiquan/anaconda3/bin/python","cdt.py",pname],log_file)
        except Exception as e:
            fw = open(log_file,"a")
            fw.write(pname + ": Error "+e.message)
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


def remove_switching_file(pnames, log_file):
    p = yaml.load( open(pnames[0], 'r') )
    if p['scope_type']=='lvl4':
        dst_dir = 'output/%s/%s' % (p['EndDate'], p['scope_id'])
    else:
        dst_dir = 'output/%s/%s' % (p['EndDate'], p['scope_id'])
    dst_file = dst_dir + '/switching_prob.txt'

    if os.path.exists(dst_file):
        os.remove(dst_file)
        f = open(log_file, 'a')
        f.write('%s removed\n' % (dst_file,) )
        f.close()


if __name__ == '__main__':
    # read command line arguments
    pool = multiprocessing.Pool(processes=5)
    while True:
        scan_table()
        time.sleep(3600)
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