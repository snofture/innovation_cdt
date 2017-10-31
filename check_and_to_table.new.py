# -*- coding: utf-8 -*-
'''
@Author: Men Li

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
import codecs
from dateutil.relativedelta import relativedelta

### check input error
def input_checkk(params):
    try:
        # attr check
        input_check = 0
        input_path = params['worker']['dir'] + "/input/" + params['item_third_cate_cd']
        attr_path = input_path +"/"+"attr_"+params['EndDate']
        if os.path.exists(attr_path) is False:
            print('there is no app_ai_slct_attributes of %s for %s!' % (params['item_third_cate_cd'],params['EndDate']))
            input_check = 1
        else:
            attr = pd.read_table(attr_path,header='infer', quotechar='\0', sep='\t')
            if len(attr) == 0:
                print('there is no attributes data of %s for %s!' % (params['item_third_cate_cd'],params['EndDate']))
                input_check = 1
        # m03 check
        m03 = None
        m03_path = input_path +"/"+"gdm_m03_"+params['EndDate']
        if os.path.exists(m03_path) is False:
            print('there is no gdm_m03_item_sku_da of %s for %s!' % (params['item_third_cate_cd'], params['EndDate']))
            input_check = 1
        else:
            m03 = pd.read_table(m03_path,header='infer', quotechar='\0', sep='\t')
            if len(m03) == 0:
                print('there is no m03 data of %s for %s!' % (params['item_third_cate_cd'],params['EndDate']))
                input_check = 1
        # m04 check
        dt_month = params["EndDate"][0:7]
        m04_count = 0
        ords = None
        for i in range(24):
            dt_m = (datetime.strptime(dt_month, '%Y-%m') + relativedelta(months=-i)).strftime('%Y-%m')
            m04_path = input_path +"/"+"gdm_m04_%s" % (dt_m)
            if os.path.exists(m04_path) is False:
                print('there is no gdm_m04_ord_det_sum file directory of %s for %s!' % (params['item_third_cate_cd'], dt_m))
                input_check = 1
                break
            else:
                m04 = pd.read_table(m04_path,header='infer', quotechar='\0', sep='\t')
                if len(m04) == 0:
                    print('there is no m04 data of %s for %s!' % (params['item_third_cate_cd'],params['EndDate']))
                    m04_count += 1
                ords = pd.concat([ords,m04],axis=0)
        if m04_count == 24 or ords is None or len(ords) == 0:
            input_check = 1
        print("input check: " + str(input_check))
        return input_check, m03, ords
    except Exception as e:
        print(e)
        return -1,None,None

def sku_num_check(input_result,m03,ords,params,lvl4_check):
    try:
        # sku_num check
        if params["scope_type"] == 'lvl4':
            for param_fff in params_list:
                params_subbb = yaml.load(open(param_fff, "r"))
                cid4_file = params_subbb["worker"]["dir"] + '/temp/' + params_subbb["EndDate"] + \
                            "/" + params_subbb["item_third_cate_cd"] + '/item_fourth_cate'
                cid4 = pd.read_table(cid4_file, quotechar='\0', encoding="utf8",
                                     na_values=["NULL", "None", "NA", "NaN", "nan", "N/A", ""], dtype={"sku_id": str})
                cid4 = cid4.dropna(axis=0, how="all")
                cid4["sku_id"] = cid4["sku_id"].astype(str)
                sku4 = cid4[cid4["attr_value"] == params_subbb["scope_desc"]]["sku_id"].unique().tolist()
                sku_num = len(sku4)
                sub_scope_id = params_subbb['scope_id']
                lvl4_check[sub_scope_id] = []
                lvl4_check[sub_scope_id].append(str(input_result))
                lvl4_check[sub_scope_id].append(str(sku_num))
        elif params["scope_type"] == 'lvl3':
            gdm03_sku_list = m03[m03["sku_type"] == 'self']['item_sku_id'].unique().tolist()
            gdm04_sku_list = ords["item_sku_id"].unique().tolist()
            sku3 = [sku_id for sku_id in gdm03_sku_list if sku_id in gdm04_sku_list]
            sku3_num = len(sku3)
            scope_id = params["scope_id"]
            lvl4_check[scope_id] = []
            lvl4_check[scope_id].append(str(input_result))
            lvl4_check[scope_id].append(str(sku3_num))
    except Exception as e:
        print(e)
        for param_fff in params_list:
            params_subbb = yaml.load(open(param_fff, "r"))
            sub_scope_id = params_subbb['scope_id']
            lvl4_check[sub_scope_id] = []
            lvl4_check[sub_scope_id].append(str(input_result))
            lvl4_check[sub_scope_id].append(str(-1))

# 如果input数据没有问题，才检查switch和cdt结果
def switch_cdt_check(params_list):
    try:
        ### check switching_prob error
        switch_error = 0
        switch_prob_all = None
        for param_ff in params_list:
            print(param_ff)
            params_subb = yaml.load(open(param_ff,"r"))
            sub_scope_id = params_subb['scope_id']
            sub_pathh = "output/" + params_subb["EndDate"] +"/" + params_subb["scope_id"]+"/switching_prob.txt"
            if os.path.exists(sub_pathh):
                switch = pd.read_table(sub_pathh, quotechar='\0', header=None, sep='\t')
                switch.columns = ['scope_id', 'src_item_id', 'dst_item_id', 'switching_prob', 'model']
                #   check scope_id
                tmp_scope = switch.loc[0, 'scope_id']
                if tmp_scope != sub_scope_id:
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
                    print("test")
                    switch_prob_all = pd.concat([switch_prob_all, switch], axis=0)
            else:
                print ('warning: there is no switching_prob.txt in %s' % (sub_pathh))
            lvl4_check[sub_scope_id].append(str(switch_error))
        if switch_error == 0:
            load_switch_prob(switch_prob_all)
        print("switch error: "+ str(switch_error))
        ### check cdt error
        cdt_err = 0
        cdt_all = None
        for param_f in params_list:
            print(param_f)
            params_sub = yaml.load(open(param_f,"r"))
            sub_scope_id = params_sub["scope_id"]
            sub_path = "output/" + params_sub["EndDate"] +"/" + params_sub["scope_id"]+"/cdt_cn.txt"
            if os.path.exists(sub_path):
                cdt = pd.read_table(sub_path, quotechar='\0', header=None, sep='\t',encoding="utf-8")
                cdt.columns = ['scope_id', 'attr', 'coef', 'model']
                cdt = cdt[cdt.model == 'Random Forest Regression']
                scope_id = cdt['scope_id'].unique().tolist()
                max_coef = cdt['coef'].max()
                min_coef = cdt['coef'].min()
                #  是否含有null
                if len(scope_id) > 1:
                    print ('error: there exist more than one scope_id in cdt.txt of %s' % (params_sub['scope_id']))
                    cdt_err = 1
                if scope_id[0] != sub_scope_id:
                    print ('error: the scope_id of %s is not right' % params_sub['scope_id'])
                    cdt_err = 1
                if max_coef != 1.0:
                    print ('error: the max coef in cdt of %s is not right' % params_sub['scope_id'])
                    cdt_err = 1
                if min_coef == -1 * np.inf:
                    print ('error: the min coef in cdt of %s is not right' % params_sub['scope_id'])
                    cdt_err = 1
                if len(cdt[cdt.coef.isnull()] > 0):
                    print ('error: there exists null value in cdt coef of %s' % params_sub['scope_id'])
                    cdt_err = 1
                if cdt_err == 0:
                    cdt_all = pd.concat([cdt_all, cdt], axis=0)
            else:
                print ('warning: there is no cdt_cn.txt in %s' % (sub_path))
            lvl4_check[sub_scope_id].append(str(cdt_err))
        if cdt_err == 0:
            load_cdt(cdt_all)
        print("cdt error: " + str(cdt_err))
    except Exception as e:
        print(e)
        for param_f in params_list:
            params_sub = yaml.load(open(param_f, "r"))
            sub_scope_id = params_sub["scope_id"]
            lvl4_check[sub_scope_id].append(str(-1))
            lvl4_check[sub_scope_id].append(str(-1))

# load switching_prob results from tmp to pre
def load_switch_prob(switch_prob_all):
    try:
        switch_name = 'switching_prob.txt'
        if len(switch_prob_all) > 0:
            if params["scope_type"] == 'lvl4':
                switch_prob_all.to_csv(cate3_path + "/switching_prob.txt", index=False, header=False, sep="\t",encoding="utf-8")
            assemble_filee = cate3_path + "/switching_prob.txt"
            if os.path.exists(assemble_filee):
                tmp_tablee = 'app.app_rmb_switching_sku_tmp'
                pre_tablee = 'app.app_rmb_switching_sku_pre'
                final_tablee = 'app.app_rmb_switching_sku'
                query = '''load data local inpath '%s' overwrite into table %s partition(dt='%s',cid3='%s');
                                   set hive.exec.dynamic.partition = true;
                                   set hive.exec.dynamic.partition.mode=nonstrict;
                                   from %s insert overwrite table %s partition(dt,cid3) select * where dt='%s' and cid3 = '%s' 
                                           insert overwrite table %s partition(dt,cid3) select * where dt='%s' and cid3 = '%s';
                                ''' % (assemble_filee, tmp_tablee, dt, cid3,tmp_tablee, pre_tablee, dt, cid3, final_tablee,  dt, cid3)
                query = query.replace('\n', '')
                cmd = 'hive -e "%s" ' % (query)
                status = subprocess.call(cmd, shell=True)
                if status == 0:
                    print ('load %s of %s into pre table done' % (switch_name, cid3))
                else:
                    print ('load %s of %s into pre table failed' % (switch_name, cid3))
                return status
    except Exception as e:
        print(e)
# load cdt results from tmp to pre
def load_cdt(cdt_all):
    try:
        cdt_name = "cdt_cn.txt"
        if len(cdt_all) > 0:
            if params["scope_type"] == 'lvl4':
                cdt_all.to_csv(cate3_path + "/cdt_cn.txt", index=False, header=False, sep="\t", encoding="utf-8")
            assemble_file = cate3_path + "/cdt_cn.txt"
            if os.path.exists(assemble_file):
                tmp_table = 'app.app_cis_wric_auth_attr_cdt_tmp'
                pre_table = 'app.app_cis_wric_auth_attr_cdt_pre'
                final_table = 'app.app_cis_wric_auth_attr_cdt'
                query = '''load data local inpath '%s' overwrite into table %s partition(dt='%s',cid3='%s');
                                   set hive.exec.dynamic.partition = true;
                                   set hive.exec.dynamic.partition.mode=nonstrict;
                                   from %s insert overwrite table %s partition(dt,cid3) select * where dt='%s' and cid3 = '%s' 
                                           insert overwrite table %s partition(dt,cid3) select * where dt='%s' and cid3 = '%s';
                                ''' % (assemble_file, tmp_table, dt, cid3,tmp_table, pre_table, dt, cid3,final_table,dt,cid3)
                query = query.replace('\n', '')
                cmd = 'hive -e "%s" ' % (query)
                status = subprocess.call(cmd, shell=True)
                if status == 0:
                    print ('load %s of %s into pre table done' % (cdt_name, cid3))
                else:
                    print ('load %s of %s into pre table failed' % (cdt_name, cid3))
                return status
    except Exception as e:
        print(e)
# load error check results
def load_cid3_status(lvl4_check):
    lvl4_check_list = []
    for key in lvl4_check:
        check_list = lvl4_check[key]
        input_c = check_list[0]
        sku_number = check_list[1]
        switch_c = check_list[2]
        cdt_c = check_list[3]
        if input_c == '0' and switch_c == '0' and cdt_c == '0':
            lvl4_sub_check = [key,input_c,sku_number,switch_c,cdt_c,'0']
        else:
            lvl4_sub_check = [key,input_c,sku_number,switch_c,cdt_c,'1']
        lvl4_check_list.append(lvl4_sub_check)
    lvl4_check_str = ''
    if len(lvl4_check_list) >0:
        for sid,inc,skn,swc,ctc,sta in lvl4_check_list[:-1]:
            lvl4_check_str += "('%s','%s','%s','%s','%s','%s'),"%(sid,inc,skn,swc,ctc,sta)
        lvl4_check_str += "('%s','%s','%s','%s','%s','%s')"%(lvl4_check_list[-1][0],lvl4_check_list[-1][1],lvl4_check_list[-1][2],lvl4_check_list[-1][3],lvl4_check_list[-1][4],lvl4_check_list[-1][5])
        check_path = cate3_path + "/lvl_check.csv"
        with codecs.open(check_path,"w",encoding='utf-8') as fp:
            for sid,inc,skn,swc,ctc,sta in lvl4_check_list:
                fp.write(sid+"\t"+inc+"\t"+skn+"\t"+swc+"\t"+ctc+"\t"+sta+"\n")
        # load from tmp to finish
        tmp = 'app.app_ai_wric_cid3_status_tmp'
        finish = 'app.app_ai_wric_cid3_status'
        query = '''load data local inpath '%s' overwrite into table %s partition(dt='%s',cid3='%s');
                    set hive.exec.dynamic.partition = true;
                   set hive.exec.dynamic.partition.mode=nonstrict;
                   insert overwrite table %s partition(dt,cid3) select * from %s where dt='%s' and cid3 = '%s';
                                ''' % (check_path,tmp,dt,cid3,finish,tmp, dt, cid3)
        query = query.replace('\n', '')
        cmd = 'hive -e "%s" ' % (query)
        status = subprocess.call(cmd, shell=True)
        if status == 0:
            print ('insert status check to app.app_ai_wric_cid3_status successful')
        else:
            print ('insert status check to app.app_ai_wric_cid3_status failed')
if __name__ == '__main__':
    n = len(sys.argv) - 1
    if n < 1:
        print ('Usage: \n    python check_and_to_table.new.py param_file\n')
        sys.exit()
    else:
        param_file = sys.argv[1]
        print ('[INFO] check_and_to_table.new started')

    # read config
    params = yaml.load(open(param_file,"r"))
    cid3 = params["item_third_cate_cd"]
    dtt = params['EndDate']
    dt = (datetime.strptime(dtt, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

    # params
    params_list = []
    if params["scope_type"] == 'lvl4':
        params_list = ifc.main(params)
    else:
        params_list = [param_file]

    # output path
    cate3_path = params['worker']['dir'] + '/output/' + params['EndDate'] + '/' + params['super_scope_id']
    if os.path.exists(cate3_path) is False:
        os.mkdir(cate3_path)
    f = open(cate3_path + '/check.log', 'w')
    sys.stdout = f

    # input path
    lvl4_check = {}
    input_result,m03,ords =  input_checkk(params)
    if input_result == 0:
        sku_num_check(input_result,m03,ords,params,lvl4_check)
        switch_cdt_check(params_list)
    else:
        for param_f in params_list:
            params_sub = yaml.load(open(param_f,"r"))
            scope_id = params_sub["scope_id"]
            lvl4_check[scope_id] = []
            lvl4_check[scope_id].append(str(input_result))
            lvl4_check[scope_id].extend(['-1','1','1','1'])
    load_cid3_status(lvl4_check)
    f.close()


'''创新cdt结果检查表
CREATE TABLE `app.app_ai_wric_cid3_status`(
`scope_id` string COMMENT 'scope id',
`input_check` string COMMENT '下数据检查',
`sku_number` string COMMENT 'sku数量',
`switch_check` string COMMENT '替代性检查',
`cdt_check` string COMMENT 'cdt检查',
`status` string COMMENT '0：完成')
COMMENT '创新中心项目状态检查表'
PARTITIONED BY (
`dt` string COMMENT '格式yyyy-MM',
`cid3` string COMMENT '三级分类ID')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY  '\t'
STORED AS ORC;
alter table app.app_ai_wric_cid3_status set serdeproperties('serialization.null.format' = '');'''




'''
std_brand_path = params['worker']['dir'] + "/input/" + "app_aicm_jd_std_brand_da"
if os.file.exits(std_brand_path) is False:
    print('there is no app_aicm_jd_std_brand_da of %s!' % params['item_third_cate_cd'])
    input_check = 1
cate_tree_path = params['worker']['dir'] + "/input/" + "category_tree"
if os.file.exits(cate_tree_path) is False:
    print('there is no category_tree of %s!' % params['item_third_cate_cd'])
    input_check = 1
ign_att_path = params['worker']['dir'] + "/input/" + "ignored_attrs.txt"
if os.file.exits(ign_att_path) is False:
    print('there is no ignored_attrs.txt of %s!' % params['item_third_cate_cd'])
    input_check = 1
'''