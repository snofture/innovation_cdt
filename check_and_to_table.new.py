# -*- coding: utf-8 -*-
'''

@Author: Zhang Zhan

@License: (C) Copyright 2013-2017, Revenue Management R & D Department, JD.COM.

@Email: zhangzhan19901015@gmail.com

@Software: PyCharm Community Edition

@File: check_and_to_table.py

@Time: 2017/9/28 10:39

@Desc: check cdt result; load to data to pre table

'''
import os
import sys
import yaml
from datetime import datetime,timedelta
import re
import pandas as pd
import numpy as np
import subprocess
import item_fourth_cate as ifc


n = len(sys.argv) - 1
if n < 1:
    print ('Usage: \n    python check_and_to_table.new.py param_file\n')
    sys.exit()
else:
    param_file = sys.argv[1]
    print ('[INFO] check_and_to_table.new started')


cdt_name="cdt_cn.txt"
# read config
param_file = "params/sku_878.yaml"
params = yaml.load(open(param_file,"r"))
cid3 = params["item_third_cate_cd"]
dtt = params['EndDate']
dt = (datetime.strptime(dtt, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

# output path
cate3_path = './output/' + params['EndDate'] + '/' + params['super_scope_id']
if os.path.exists(cate3_path) is False:
    os.mkdir(cate3_path)

f = open(cate3_path + '/check.log', 'w')
sys.stdout = f

if params["scope_type"] == 'lvl4':
    params_list = ifc.main(params)
else:
    params_list = [param_file]

# check cdt error
cdt_all = None
for param_f in params_list:
    print(param_f)
    params_sub = yaml.load(open(param_f,"r"))
    sub_path = "output/" + params_sub["EndDate"] +"/" + params_sub["scope_id"]+"/cdt_cn.txt"
    if os.path.exists(sub_path):
        err = 0
        cdt = pd.read_table(sub_path, quotechar='\0', header=None, sep='\t',encoding="utf-8")
        cdt.columns = ['scope_id', 'attr', 'coef', 'model']
        cdt = cdt[cdt.model == 'Random Forest Regression']
        scope_id = cdt['scope_id'].unique().tolist()
        max_coef = cdt['coef'].max()
        min_coef = cdt['coef'].min()
        #  是否含有null
        if len(scope_id) > 1:
            print ('error: there exist more than one scope_id in cdt.txt of %s' % (params_sub['scope_id']))
            err = 1
        if scope_id[0] != params_sub['scope_id']:
            print ('error: the scope_id of %s is not right' % params_sub['scope_id'])
            err = 1
        if max_coef != 1.0:
            print ('error: the max coef in cdt of %s is not right' % params_sub['scope_id'])
            err = 1
        if min_coef == -1 * np.inf:
            print ('error: the min coef in cdt of %s is not right' % params_sub['scope_id'])
            err = 1
        if len(cdt[cdt.coef.isnull()] > 0):
            print ('error: there exists null value in cdt coef of %s' % params_sub['scope_id'])
            err = 1
        if err == 0:
            cdt_all = pd.concat([cdt_all, cdt], axis=0)
    else:
        print ('warning: there is no cdt_cn.txt in %s' % (sub_path))

# load cdt results from tmp to pre
if len(cdt_all) > 0:
    if params["scope_type"] == 'lvl4':
        cdt_all.to_csv(cate3_path+"/cdt_cn.txt",index=False,header=False,sep="\t",encoding="utf-8")

    assemble_file = cate3_path + "/cdt_cn.txt"
    if os.path.exists(assemble_file):
        tmp_table = 'app.app_cis_wric_auth_attr_cdt_tmp'
        pre_table = 'app.app_cis_wric_auth_attr_cdt_pre'
        query = '''load data local inpath '%s' overwrite into table %s partition(dt='%s',cid3='%s');
                           set hive.exec.dynamic.partition = true;
                           set hive.exec.dynamic.partition.mode=nonstrict;
                           insert overwrite table %s partition(dt,cid3) select * from %s where dt='%s' and cid3 = '%s';
                        ''' % (assemble_file, tmp_table, dt, cid3, pre_table, tmp_table, dt, cid3)
        query = query.replace('\n', '')
        cmd = 'hive -e "%s" ' % (query)
        status = subprocess.call(cmd, shell=True)
        if status == 0:
            print ('load %s of %s into pre table done' % (cdt_name, cid3))
        else:
            print ('load %s of %s into pre table failed' % (cdt_name, cid3))

# check switching_prob error
switch_prob_all = None
for param_ff in params_list:
    print(param_ff)
    params_subb = yaml.load(open(param_ff,"r"))
    sub_pathh = "output/" + params_subb["EndDate"] +"/" + params_subb["scope_id"]+"/switching_prob.txt"
    if os.path.exists(sub_pathh):
        switch_error = 0
        switch = pd.read_table(cate3_path + '/' + 'switching_prob.txt', quotechar='\0', header=None, sep='\t')
        switch.columns = ['scope_id', 'src_item_id', 'dst_item_id', 'switching_prob', 'model']
        #   check scope_id
        tmp_scope = switch.loc[0, 'scope_id']
        if tmp_scope != params_subb['scope_id']:
            print ('error: the scope_id in switching_prob.txt is not right \n')
            switch_error = 1
        # check the range of switching
        max_switch = switch['switching_prob'].max()
        min_switch = switch['switching_prob'].min()
        if max_switch > 1:
            print('error: the max switching_prob in switching_prob.txt is larger than 1.0 \n')
            switch_error = 1
        if max_switch < 0:
            print('error: the min switching_prob in switching_prob.txt is smaller than 0.0 \n')
            switch_error = 1
        if len(switch) >= 50:
            if max_switch == min_switch:
                print ('error: the min value and max value in switch are equal \n')
                switch_error = 1
        if switch_error == 0:
            switch_prob_all = pd.concat([switch_prob_all, switch], axis=0)
    else:
        print ('warning: there is no switching_prob.txt in %s' % (sub_pathh))

# load switching_prob results from tmp to pre
if len(switch_prob_all) > 0:
    if params["scope_type"] == 'lvl4':
        switch_prob_all.to_csv(cate3_path+"/switching_prob.txt",index=False,header=False,sep="\t",encoding="utf-8")

    assemble_filee = cate3_path + "/switching_prob.txt"
    if os.path.exists(assemble_filee):
        tmp_tablee = 'app.app_rmb_switching_sku_tmp'
        pre_tablee = 'app.app_rmb_switching_sku_pre'
        query = '''load data local inpath '%s' overwrite into table %s partition(dt='%s',cid3='%s');
                           set hive.exec.dynamic.partition = true;
                           set hive.exec.dynamic.partition.mode=nonstrict;
                           insert overwrite table %s partition(dt,cid3) select * from %s where dt='%s' and cid3 = '%s';
                        ''' % (assemble_filee, tmp_tablee, dt, cid3, pre_tablee, tmp_tablee, dt, cid3)
        query = query.replace('\n', '')
        cmd = 'hive -e "%s" ' % (query)
        status = subprocess.call(cmd, shell=True)
        if status == 0:
            print ('load %s of %s into pre table done' % (switch_name, cid3))
        else:
            print ('load %s of %s into pre table failed' % (switch_name, cid3))
f.close()