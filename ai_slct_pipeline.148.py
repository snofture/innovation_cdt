#-*- coding:utf-8 -*-
'''
    This script prepare data for ai_slct
'''
__author__ = 'xiajiguang1'

import os
import sys
import yaml
import datetime
import subprocess
import re


def add_time_frame(params):
    ''' calculate time frame and add them into params if needed'''
    if 'EndDate' in params:
        end_date = datetime.datetime.strptime(params['EndDate'],'%Y-%m-%d')
    else:
        today = datetime.date.today()
        end_date = today - datetime.timedelta(days=today.day)
        params['EndDate'] = end_date.strftime('%Y-%m-%d')
    if 'StartDate' in params:
            pass
    else:
        year = end_date.year - params['nYears']
        month = end_date.month + 1
        if month > 12:
            month = month - 12
            year = year + 1
        start_date = datetime.date(year, month, 1)
        params['StartDate'] = start_date.strftime('%Y-%m-%d')
    return params


def add_category_information(cate3s, params):
    ''' query category structure for a given item_third_cate_cd, and add them
    into params 
    '''

    # construct a category dict
    file = open(params['category_tree'])
    line = file.readline().strip('\n')
    header = line.split('\t')
    cgs = dict()
    while 1:
        line = file.readline().strip('\n')
        if not line:
            break
        cg = dict()
        record = line.split('\t')
        for i in range(len(record)):
            cg[header[i]] = record[i]
        cgs[cg['item_third_cate_cd']] = cg

    # query full category information
    cate1s = list()
    cate2s = list()
    descs = list()
    for cate3 in cate3s.split('-'):
        if cate3 in cgs:
            cate1 = cgs[cate3]['item_first_cate_cd']
            cate2 = cgs[cate3]['item_second_cate_cd']
            desc = cgs[cate3]['item_third_cate_name']
        else:
            cate1 = '0'
            cate2 = '0'
            desc = 'None'
        cate1s.append(cate1)
        cate2s.append(cate2)
        descs.append(desc)

    params['item_first_cate_cd'] = '-'.join(cate1s)
    params['item_second_cate_cd'] = '-'.join(cate2s)
    params['item_third_cate_cd'] = cate3s
    params['scope_desc'] = '-'.join(descs)
    return params


def add_scope_id(params):
    params['scope_id'] = '%s_%s_%s_%s_%s_%s_%s' % (
        params['item_first_cate_cd'],
        params['item_second_cate_cd'],
        params['item_third_cate_cd'],
        params['self_pop'],
        params['scope_type'],
        params['scope_desc'],
        params['item_type'])
    if params['scope_type'] == 'lvl4':
        params['super_scope_id'] = params['scope_id'].replace('lvl4', 'super')
    else:
        params['super_scope_id'] = params['scope_id']
    return params


def add_log_file(params):
    params['log_file'] = params['worker']['dir'] + '/logs/' + params['EndDate'] + \
                         '/' + params['item_third_cate_cd'] + '.log'
    return params


def create_local_path(params):
    local_path = 'input/' + params['EndDate'] + '/' + params['item_third_cate_cd']
    if not os.path.exists(local_path):
        os.makedirs(local_path)
    log_path = params['worker']['dir'] + '/logs/' + params['EndDate']
    if not os.path.exists(log_path):
        os.makedirs(log_path)


def fetch_gdm_m04_ord_det_sum(params):
    local_path = 'input/' + params['EndDate'] + '/' + params['item_third_cate_cd']
    query = '''
    set hive.cli.print.header=true;
    select user_id, parent_sale_ord_id, item_sku_id, sale_ord_id, sale_ord_tm,
        sale_qtty, after_prefr_amount, before_prefr_amount 
    from dev.all_sku_order_det 
    where dt >= '%s'
    and dt <= '%s'
    and item_third_cate_cd in (%s)
    and sale_ord_valid_flag = 1;
    ''' % (params['StartDate'], params['EndDate'],str(params['item_third_cate_cd']).replace('-',','))
    query = query.replace('\n','')
    cmd = 'hive -e "%s" | grep -v \'^WARN:\' > %s/gdm_m04_ord_det_sum' % (query, local_path)
    print (cmd)
    status = subprocess.call(cmd,shell=True)
    if status == 0:
        print ('download gdm_m04_ord_det_sum success!')
    else:
        print ('download gdm_m04_ord_det_sum failed! \n')
    return status


def fetch_gdm_m03_item_sku_da(params):
    local_path = 'input/' + params['EndDate'] + '/' + params['item_third_cate_cd']
    query = '''
    set hive.cli.print.header=true;
    select
        sku.item_sku_id, sku.sku_name, sku.sku_status_cd, sku.wt, sku.spu_id,
        sku.item_third_cate_cd, sku.dt,sku.pop_vender_id,
        case when getDataTypeBySkuId(cast(sku.item_sku_id as bigint))=10
            then 'self'
        when getDataTypeBySkuId(cast(sku.item_sku_id as bigint)) in
        (1,2,3,4,5,6,7,8,9)
            then 'pop'
        else
            'other'
        end
        as sku_type
    from
        gdm.gdm_m03_item_sku_da sku
    where
        sku.dt = '%s'
        and sku.item_third_cate_cd in (%s);
    ''' % (params['EndDate'],
           str(params['item_third_cate_cd']).replace('-',','))

    query = query.replace('\n','')
    cmd = 'hive -e "%s" | grep -v \'^WARN:\' > %s/gdm_m03_item_sku_da' % (query, local_path)
    print (cmd)
    status = subprocess.call(cmd, shell=True)
    if status == 0:
        print ('download gdm_m03_item_sku_da success!')
    else:
        print ('download gdm_m03_item_sku_da failed! \n')
    return status


def fetch_app_ai_slct_sku(params):
    local_path = 'input/' + params['EndDate'] + '/' + params['item_third_cate_cd']
    query = '''
    set hive.cli.print.header=true;
    select * from app.app_ai_slct_sku
    where dt <= '%s'
    and item_third_cate_cd in (%s);
    ''' % (params['EndDate'],
           str(params['item_third_cate_cd']).replace('-',','))

    query = query.replace('\n','')
    #cmd = 'hive -e "%s" > %s/app_ai_slct_sku' % (query, local_path)
    cmd = 'hive -e "%s" | grep -v \'^WARN:\' > %s/app_ai_slct_sku' % (query, local_path)
    print (cmd)
    status = subprocess.call(cmd, shell=True)
    if status == 0:
        print ('download app_ai_slct_sku success!')
    else:
        print ('download app_ai_slct_sku failed! \n')
    return status

def fetch_gdm_m03_item_ext_attr(params):
    local_path = 'input/' + params['EndDate'] + '/' + params['item_third_cate_cd']
    query = '''
        set hive.cli.print.header=true;
        select data_type, item_sku_id, com_attr_name, com_attr_value_name, cate_id, dt
        from gdm.gdm_m03_item_sku_ext_attr_da
        where dt = '%s' 
        and cate_id in (%s)
        and data_type != 2;
        ''' % (params['EndDate'],str(params['item_third_cate_cd']).replace('-',','))
    query = query.replace('\n', '')
    # cmd = 'hive -e "%s" > %s/app_ai_slct_attributes' % (query, local_path)
    cmd = 'hive -e "%s" | grep -v \'^WARN:\' > %s/gdm_m03_item_ext_attr' % (query, local_path)
    print (cmd)
    status = subprocess.call(cmd, shell=True)
    if status == 0:
        print ('download gdm_m03_item_ext_attr success!')
    else:
        print ('download gdm_m03_item_ext_attr failed! \n')
    return status


def fetch_app_ai_slct_attributes(params):
    local_path = 'input/' + params['EndDate'] + '/' + params['item_third_cate_cd']
    query = '''
    set hive.cli.print.header=true;
    select * from app.app_ai_slct_attributes
    where dt = '%s'
    and item_third_cate_cd in (%s);
    ''' % ("wric_auth_attr",
           str(params['item_third_cate_cd']).replace('-',','))

    query = query.replace('\n','')
    #cmd = 'hive -e "%s" > %s/app_ai_slct_attributes' % (query, local_path)
    cmd = 'hive -e "%s" | grep -v \'^WARN:\' > %s/app_ai_slct_attributes' % (query, local_path)
    print (cmd)
    status = subprocess.call(cmd, shell=True)
    if status == 0:
        print ('download app_ai_slct_attributes success!')
    else:
        print ('download app_ai_slct_attributes failed! \n')
    return status


def fetch_app_ai_slct_match(params):
    local_path = 'input/' + params['EndDate'] + '/' + params['item_third_cate_cd']
    query = '''
    set hive.cli.print.header=true;
    select * from app.app_ai_slct_match
    where dt = '%s'
    and item_third_cate_cd in (%s);
    ''' % (params['EndDate'],
           str(params['item_third_cate_cd']).replace('-',','))

    query = query.replace('\n','')
    #cmd = 'hive -e "%s" > %s/app_ai_slct_match' % (query, local_path)
    cmd = 'hive -e "%s" | grep -v \'^WARN:\' > %s/app_ai_slct_match' % (query, local_path)
    print (cmd)
    status = subprocess.call(cmd, shell=True)
    if status == 0:
        print ('download app_ai_slct_match success!')
    else:
        print ('download app_ai_slct_match failed! \n')
    return status


def fetch_app_ai_slct_gmv(params):
    local_path = 'input/' + params['EndDate'] + '/' + params['item_third_cate_cd']
    query = '''
    set hive.cli.print.header=true;
    select * from app.app_ai_slct_gmv
    where dt <= '%s'
    and item_third_cate_cd in (%s);
    ''' % (params['EndDate'],
           str(params['item_third_cate_cd']).replace('-',','))

    query = query.replace('\n','')
    #cmd = 'hive -e " %s" > %s/app_ai_slct_gmv' % (query, local_path)
    cmd = 'hive -e "%s" | grep -v \'^WARN:\' > %s/app_ai_slct_gmv' % (query, local_path)
    print (cmd)
    status = subprocess.call(cmd, shell=True)
    if status == 0:
        print ('download app_ai_slct_gmv success!')
    else:
        print ('download app_ai_slct_gmv failed! \n')
    return status


def fetch_jd_tmall_brand_mapping(params):
    local_path = 'input/' + params['EndDate'] + '/' + params['item_third_cate_cd']
    query = '''
    set hive.cli.print.header=true;
    select jdstdbrandname, tmstdbrandname from app.app_sys_determined_jd_tmall_brand_mapping
    where dt = '%s'
    and jdcategoryid3 in (%s)
    and status = 1;
    ''' % (params['EndDate'],
           str(params['item_third_cate_cd']).replace('-',','))

    query = query.replace('\n','')
    #cmd = 'hive -e " %s" > %s/jd_tmall_brand_mapping' % (query, local_path)
    cmd = 'hive -e "%s" | grep -v \'^WARN:\' > %s/jd_tmall_brand_mapping' % (query, local_path)
    print (cmd)
    status = subprocess.call(cmd, shell=True)
    if status == 0:
        print ('download jd_tmall_brand_mapping success!')
    else:
        print ('download jd_tmall_brand_mapping failed! \n')
    return status


def fetch_app_cfo_profit_loss_b2c_det(params):
    local_path = 'input/' + params['EndDate'] + '/' + params['item_third_cate_cd']
    query = '''
    set hive.cli.print.header=true;
    select * from app.app_cfo_profit_loss_b2c_det
    where dt >= '%s'
    and dt <= '%s'
    and item_third_cate_name in (%s);
    ''' % (params['StartDate'], params['EndDate'],
           "'" + re.sub("-","','", params['scope_desc']) + "'")

    query = query.replace('\n','')
    #cmd = 'hive -e " %s" > %s/app_cfo_profit_loss_b2c_det' % (query, local_path)
    cmd = 'hive -e "%s" | grep -v \'^WARN:\' > %s/app_cfo_profit_loss_b2c_det' % (query, local_path)
    print (cmd)
    status = subprocess.call(cmd, shell=True)
    if status == 0:
        print ('download app_cfo_profit_loss_b2c_det success!')
    else:
        print ('download app_cfo_profit_loss_b2c_det failed! \n')
    return status


def fetch_app_aicm_jd_std_brand_da(params):
    local_path = 'input/' + params['EndDate']
    query = '''
    set hive.cli.print.header=true;
    select * from app.app_aicm_jd_std_brand_da
    where dt = '%s';
    ''' % (params['EndDate'],)

    query = query.replace('\n','')
    #cmd = 'hive -e " %s" > %s/app_aicm_jd_std_brand_da' % (query, local_path)
    cmd = 'hive -e "%s" | grep -v \'^WARN:\' > %s/app_aicm_jd_std_brand_da' % (query, local_path)
    print (cmd)
    status = subprocess.call(cmd, shell=True)
    if status == 0:
        print ('download app_aicm_jd_std_brand_da success!')
    else:
        print ('download app_aicm_jd_std_brand_da failed! \n')
    return status


def fetch_tm_sku_price(params):
    local_path = 'input/' + params['EndDate'] + '/' + params['item_third_cate_cd']
    query = ''' set hive.cli.print.header=true;
                select 
                        b.sku,
                        b.mean_price  
                    from 
                       (
                        select 
                            sku_id
                        from  app.app_ai_slct_sku 
                        where web_id='2' and dt = '%s' 
                              and item_third_cate_cd = '%s'
                       ) a
                    join(
                        select  
                            item_sku_id as sku,
                            avg_price as mean_price
                        from app.app_aicm_tm_sku_monthly_wide_da   
                        where  status = 1 and dt = '%s' 
                        ) b  
                    on a.sku_id == b.sku  
                '''%(params['EndDate'],params['item_third_cate_cd'],params['EndDate'])

    query = query.replace('\n','')
    cmd = 'hive -e "%s" | grep -v \'^WARN:\' > %s/tm_sku_price' % (query, local_path)
    print (cmd)
    status = subprocess.call(cmd, shell=True)
    if status == 0:
        print ('download tm_sku_price success!')
    else:
        print ('download tm_sku_price failed! \n')
    return status


def fetch_app_forecast_attributes(params):
    local_path = 'input/' + params['EndDate'] + '/' + params['item_third_cate_cd']
    query = '''
    set hive.cli.print.header=true;
    select * from app.app_forecast_attributes
    where  item_third_cate_cd in (%s);
    ''' % (str(params['item_third_cate_cd']).replace('-',','))
    query = query.replace('\n','')
    #cmd = 'hive -e "%s" > %s/app_ai_slct_attributes' % (query, local_path)
    cmd = 'hive -e "%s" | grep -v \'^WARN:\' > %s/app_forecast_attributes' % (query, local_path)
    print (cmd)
    status = subprocess.call(cmd, shell=True)
    if status == 0:
        print ('download app_forecast_attributes success!')
    else:
        print ('download app_forecast_attributes failed! \n')
    return status


def fetch_app_cis_ai_slct_assortment(params):
    local_path = 'input/' + params['EndDate'] + '/' + params['item_third_cate_cd']
    query = '''
    set hive.cli.print.header=true;
    select * from app.app_cis_ai_slct_assortment
    where cid3 in (%s) and dt < '%s';
    ''' % (str(params['item_third_cate_cd']).replace('-',','),params['EndDate'])
    query = query.replace('\n','')
    #cmd = 'hive -e "%s" > %s/app_ai_slct_attributes' % (query, local_path)
    cmd = 'hive -e "%s" | grep -v \'^WARN:\' > %s/app_cis_ai_slct_assortment' % (query, local_path)
    print (cmd)
    status = subprocess.call(cmd, shell=True)
    if status == 0:
        print ('download app_cis_ai_slct_assortment success!')
    else:
        print ('download app_cis_ai_slct_assortment failed! \n')
    return status


def fetch_app_ai_product_key_factor_category_role(params):
    local_path = 'input/' + params['EndDate'] + '/'
    query = '''
    set hive.cli.print.header=true;
    select * from app.app_ai_product_key_factor_category_role ;
    '''
    query = query.replace('\n','')
    #cmd = 'hive -e "%s" > %s/app_ai_slct_attributes' % (query, local_path)
    cmd = 'hive -e "%s" | grep -v \'^WARN:\' > %s/app_ai_product_key_factor_category_role' % (query, local_path)
    print (cmd)
    status = subprocess.call(cmd, shell=True)
    if status == 0:
        print ('download app_ai_product_key_factor_category_role success!')
    else:
        print ('download app_ai_product_key_factor_category_role failed! \n')
    return status


def new_month(params):
    local_file = 'input/' + params['EndDate'] + '/app_aicm_jd_std_brand_da'
    if os.path.isfile(local_file):
        return False
    else:
        return True


def create_param(params):
    local_path = 'params/sku_' + params['item_third_cate_cd'] + '.yaml'
    stream = open(local_path, 'w')
    yaml.safe_dump(params, stream, allow_unicode=True, default_flow_style=False)
    return local_path


def create_param_brand(params):
    local_path = 'params/brand_' + params['item_third_cate_cd'] + '.yaml'
    stream = open(local_path, 'w')
    params['scope_id'] = params['scope_id'].replace('_sku', '_brand')
    params['item_type'] = 'brand'
    yaml.safe_dump(params, stream, allow_unicode=True, default_flow_style=False)
    return local_path


def run_task_local(params):
    work_dir = params['worker']['dir']
    item_third_cate_cd = params['item_third_cate_cd']
    lvl = params['scope_type']

    # 在本机执行shell命令
    cmd = '../../chenzhiquan/anaconda3/bin/python %s/computation_pipe.cdt.py %s %s' % (work_dir, item_third_cate_cd, lvl)
    (status, output) = subprocess.getstatusoutput(cmd)
    if status == 0:
        print ('computation task done!')
    else:
        print ('computation task failed: \n' + output + '\n')
    return params

def load_data_into_bdp(params):
    # 程序运行完成，搜索所有scope_id
    scopes = [params['scope_id']]
    cmd = 'grep \'\[PIPE\]\' %s' % (params['log_file'],)
    (status, output) = subprocess.getstatusoutput(cmd)
    if status==0:
        lines = output.split('\n')
        lines = filter(lambda x: x!='', lines)
        if len(lines)>1:
            scopes = map(lambda x: x.replace('[PIPE]', '').strip(), lines)
    scopes = list(set(scopes))
    tables = ['scope', 'switching_prob', 'sku_in_scope','spendswitch',
             'cdt', 'predicted', 'assortment','impact']
    filename_dict = {'cdt': 'cdt_cn'}
    dt = params['EndDate']
    # clear assemble dir
    cid3s = set()
    for scope in scopes:
        cid3 = scope.split('_')[2]
        cid3s.add(cid3)
    for cid3 in cid3s:
        asb_dir = 'assemble/%s/%s' % (dt, cid3)
        if os.path.exists(asb_dir):
            cmd = 'rm -rf %s' % asb_dir
            status = subprocess.call(cmd)
            if status == 0:
                print ('remove %s success! \n' % (asb_dir,))
            else:
                print ('remove %s failed! \n' % (asb_dir))
        os.makedirs(asb_dir)
    # assemble text file by cid3
    for scope in scopes:
        cid3 = scope.split('_')[2]
        for table in tables:
            file_name = filename_dict.get(table,table)
            path = 'output/%s/%s/%s.txt' % (dt, scope, file_name)
            asb_path = 'assemble/%s/%s/%s.txt' % (dt, cid3, file_name)
            if os.path.exists(path):
                cmd = 'cat %s >> %s' % (path, asb_path)
                status = subprocess.call(cmd, shell=True)
                if status == 0:
                    print ('assemble %s success! \n' % (path,))
                else:
                    print ('assemble %s failed! \n' % (path,))
    # load text file into hive
    for cid3 in cid3s:
        for table in tables:
            file_name = filename_dict.get(table,table)
            asb_path = 'assemble/%s/%s/%s.txt' % (dt, cid3, file_name)
            if os.path.exists(asb_path):
                query = '''
                LOAD DATA LOCAL INPATH '%s' OVERWRITE INTO TABLE app.app_cis_ai_slct_%s PARTITION(dt='%s',cid3='%s');
                ''' % (asb_path, table, dt, cid3)
                cmd = 'hive -e "%s" ' % (query,)
                print (cmd)
                status = subprocess.call(cmd)
                if status == 0:
                    print ('load %s success! \n' % (asb_path,))
                else:
                    print ('load %s failed! \n' % (asb_path,))


if __name__ == '__main__':
    # read command line arguments
    print(len(sys.argv))
    n = len(sys.argv) - 1
    print(n)
    if n < 7:
        print ('Usage: \n    python ai_slct_pipeline.148.py cid3 dt lvl classfiy_attr ord_log rerun ext_attr\n')
        sys.exit()
    else:
        cid3 = sys.argv[1]
        dt = sys.argv[2]
        lvl = sys.argv[3]
        classify_attr = sys.argv[4]
        ord_log = sys.argv[5]
        rerun = sys.argv[6]
        ext_attr = sys.argv[7]


    # read parameters
    params = yaml.load( open('params/default.yaml', 'r') )
    params['EndDate'] = dt
    params['scope_type'] = lvl
    params['rerun'] = rerun
    #params['sales_estimate'] = sales_estimate
    params['classify_attr'] = classify_attr
    params['ord_log'] = ord_log
    params["ext_attr"] = ext_attr
    # calculate time frame
    params = add_time_frame(params)

    # add category information
    params = add_category_information(cid3, params)

    # add scope id
    params = add_scope_id(params)

    # add log file
    params = add_log_file(params)

    # create directory if needed
    create_local_path(params)

    # download data from bdp.jd.com
    # params_file = './params/sku_%s.yaml'%(9435)
    # params = yaml.load(file(params_file, 'r'))

    local_path = 'input/' + params['EndDate'] + '/' + params['item_third_cate_cd']
    local_gdm_m04 = local_path + '/gdm_m04_ord_det_sum'
    local_gdm_m03 = local_path + '/gdm_m03_item_sku_da'
    local_tm_sku_price = 'input/' + params['EndDate'] + '/tm_sku_price'
    if rerun == 'yes':
        fetch_gdm_m04_ord_det_sum(params)
        fetch_gdm_m03_item_sku_da(params)
    else:
        if not os.path.exists(local_gdm_m04):
            fetch_gdm_m04_ord_det_sum(params)
        if not os.path.exists(local_gdm_m03):
            fetch_gdm_m03_item_sku_da(params)


    if ext_attr == 'no':
        fetch_app_ai_slct_attributes(params)
    elif ext_attr == "yes":
        #ext_attr_file = 'input/'+params["EndDate"] +"/"+params['item_third_cate_cd']+"/gdm_m03_item_ext_attr"
        #if os.path.exists(ext_attr_file) is False:
        fetch_gdm_m03_item_ext_attr(params)

    #fetch_app_ai_slct_sku(params)
    #fetch_app_ai_slct_attributes(params)
    #fetch_app_ai_slct_match(params)
    #fetch_app_ai_slct_gmv(params)
    #fetch_jd_tmall_brand_mapping(params)
    #fetch_app_cfo_profit_loss_b2c_det(params)
    #fetch_app_forecast_attributes(params)
    #fetch_tm_sku_price(params)
    #fetch_app_cis_ai_slct_assortment(params)

    #local_key_role = './input/%s/app_ai_product_key_factor_category_role'%(params['EndDate'])
    #if not os.path.exists(local_key_role):
    #    fetch_app_ai_product_key_factor_category_role(params)

    #if new_month(params):
    #    fetch_app_aicm_jd_std_brand_da(params)

    # transfer data to worker
    p1 = create_param(params)
    p2 = create_param_brand(params)

    # run task on worker
    scopes = run_task_local(params)

    # load text file into bdp hive
    # load_data_into_bdp(params)