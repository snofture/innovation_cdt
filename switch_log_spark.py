# -*- coding: utf-8 -*-
'''

@Author: Zhang Zhan

@License: (C) Copyright 2013-2017, Revenue Management R & D Department, JD.COM.

@Email: zhangzhan19901015@gmail.com

@Software: PyCharm Community Edition

@File: switch_log_spark.py

@Time: 2017/10/18 14:44

@Desc:

'''

import os
import yaml
import csv
from pyspark.sql import HiveContext
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark import SparkContext, SparkConf
from datetime import datetime,timedelta
import subprocess
import sys
reload(sys)
sys.setdefaultencoding('utf8')

if __name__ == '__main__':

    # read argv
    n = len(sys.argv) - 1
    if n < 1:
        print("Usage : \n spark-submit --executor-memory 10G --total-executor-cores 40 switch_log_spark.py param_file")
    else:
        param_file = sys.argv[1]

# init spark conf
sc = SparkContext(appName='caculate switch')
hc = HiveContext(sc)

# read params config
params = yaml.load(open('params/default.yaml', 'r'))
user_params = yaml.load(open(param_file, 'r'))
for key, value in user_params.items():
    params[key] = value

output_path = params['worker']['dir'] + '/output/' + params['EndDate'] + '/' + params['scope_id']
if not os.path.exists(output_path):
    os.makedirs(output_path)

# create hdfs path
hdfs_path = '/user/mart_cis/zhangzhan/ai_slct_cdt/input/' + params['EndDate'] +"/" +params["item_third_cate_cd"]
print(hdfs_path)
status = subprocess.call("hadoop fs -test -e %s"%hdfs_path,shell=True)
if status != 0:
    cmd = "hadoop fs -mkdir -p %s"%hdfs_path
    status = subprocess.call(cmd,shell=True)
    if status == 0:
        print("hdfs path create successful")
    else:
        print("hdfs path create failed")

# put app_ai_slct_jd_std_brand_da to hdfs
jd_brand_local_path = 'input/app_aicm_jd_std_brand_da'
jd_brand_hdfs = "/user/mart_cis/zhangzhan/ai_slct_cdt/input/"
status = subprocess.call("hadoop fs -put -f %s %s"%(jd_brand_local_path,jd_brand_hdfs),shell=True)
if status == 0:
    print("copy app_aicm_jd_std_brand_da successful")
else:
    print("copy app_aicm_jd_std_brand_da failed")

# put gdm_m03, attributes to hdfs
local_data = "input/"+params["EndDate"] + "/" + params["item_third_cate_cd"] +"/*"
cmd = "hadoop fs -put -f %s %s"%(local_data,hdfs_path)
status = subprocess.call(cmd,shell=True)
if status == 0:
    print("put local data successful")
else:
    print("put local data failed")

# read log data
dt = params['EndDate']
begin_dt = (datetime.strptime(dt, '%Y-%m-%d') - timedelta(days=11)).strftime('%Y-%m-%d')
query = ''' select  sku_id as item_sku_id,user_log_acct,date(request_tm) as request_dt,
                        concat(session_id,bs) as session_id
                from dev.gdm_m14_online_log_item_d_op_full
                where  dt >= "%s" and dt < "%s" and item_third_cate_id = "%s"
                and session_id is not null and bs in ("1","13","8","311210")'''%(begin_dt,dt,params['item_third_cate_cd'])
data = hc.sql(query).coalesce(1000)



sku_file = "/user/mart_cis/zhangzhan/ai_slct_cdt/input/"+params["EndDate"]+"/"+params["item_third_cate_cd"]+"/gdm_m03_item_sku_da"
m03 = hc.read.csv(sku_file,header=True,sep='\t')

# 过滤自营
if params['self_pop'] == 'self':
    self_sku = m03.filter(m03.sku_type == 'self').select('item_sku_id').rdd.flatMap(lambda x:x).collect()
    data = data.filter(data.item_sku_id.isin(self_sku))
elif params['self_pop'] == 'pop':
    self_sku = m03.filter(m03.sku_type == 'pop').select('item_sku_id').rdd.flatMap(lambda x: x).collect()
    data = data.filter(data.item_sku_id.isin(self_sku))
else:
    all_sku = m03.filter(m03.sku_type.isin('pop','self')).select('item_sku_id').rdd.flatMap(lambda x:x).collect()
    data = data.filter(data.item_sku_id.isin(all_sku))

# 过滤四级分类范围的sku
if params['scope_type'] == 'lvl4':
    cid4_file = "/user/mart_cis/tmp/" + params["EndDate"] + "/" + params["item_third_cate_cd"] + "/item_fourth_cate"
    cid4_file = hc.read.csv(sku_file, header=True, sep='\t')
    sku4 = cid4.filter(cid4.attr_value == params['scope_desc']).select('sku_id').rdd.flatMap(lambda x:x).collect()
    sku4 = [str(x) for x in sku4]
    data = data.filter(data.item_sku_id.isin(sku4))

#### 将sku 映射到标准品牌
if params['item_type'] == 'brand':
    attr_file = "/user/mart_cis/input/"+params["EndDate"]+"/"+params["item_third_cate_cd"]+'/app_ai_slct_attributes'
    brand_file = "/user/mart_cis/input/" + '/app_aicm_jd_std_brand_da'
    attr = hc.read.csv(attr_file,header=True,sep='\t')
    brand = hc.read.csv(brand_file,header=True,sep='\t')
    brand = brand.select('jd_brand_id','jd_brand_name').drop_duplicates().dropna(how='any')
    sku_brand = attr.filter((attr.web_id < 2) & (attr.attr_name == '品牌')).select('sku_id','attr_value').drop_duplicates()
    brand = brand.withColumnRenamed('jd_brand_name','attr_value')
    sku_brand = sku_brand.join(brand,on='attr_value',how = 'inner').select(sku_brand.sku_id,sku_brand.attr_value,brand.jd_brand_id)
    sku_brand = sku_brand.withColumn('scope_id', lit(params['scope_id']))
    sku_brand = sku_brand.withColumn('item_type', lit('brand'))
    sku_brand = sku_brand.withColumn('model', lit('log'))
    sku_brand = sku_brand.select('sku_id', 'jd_brand_id')
    sku_brand = sku_brand.withColumnRenamed('sku_id', 'item_sku_id')
    data = data.join(sku_brand, 'item_sku_id', 'inner')
    data = data.withColumn('item_sku_id', data.jd_brand_id).drop('jd_brand_id')

##### 计算替代性
data = data.withColumnRenamed('item_sku_id', 'src_item_id')
tmp = data.withColumnRenamed('src_item_id', 'dst_item_id')
switch = data.join(tmp,['user_log_acct','session_id','request_dt'],'inner')
switch = switch.withColumn('page_views', F.lit(1))
switch = switch.groupby('src_item_id', 'dst_item_id').agg(F.sum('page_views').alias('page_views_switch'))
from_sku_total_switch = switch.groupby('src_item_id').agg(F.sum('page_views_switch').alias('from_sku_total_switch'))
new_column = from_sku_total_switch.src_item_id.cast("string")
from_sku_total_switch = from_sku_total_switch.withColumn('src_item_id',new_column)
switch = switch.join(from_sku_total_switch,'src_item_id','inner')
switch = switch.withColumn('switching_prob', switch.page_views_switch/switch.from_sku_total_switch)
switch = switch.withColumn('model',F.lit('log')).withColumn('scope_id',F.lit(params['scope_id']))
switch = switch.select('scope_id','src_item_id','dst_item_id','switching_prob','model')
switch = switch.collect()

import codecs
switch_prob = output_path + '/switching_prob.txt'
with codecs.open(switch_prob,"w",encoding="utf-8") as fp:
    for row in switch:
        fp.write(str(row[0])+"\t"+str(row[1])+"\t"+str(row[2])+"\t"+str(row[3])+"\t"+str(row[4])+"\n")

scope = output_path + '/scope.txt'
with codecs.open(scope,"w",encoding="utf-8") as fp:
    fp.write(params["scope_id"]+"\t"+params["item_first_cate_cd"]+"\t"+params["item_second_cate_cd"]+"\t"+params["item_third_cate_cd"]+"\t"+params["self_pop"]+"\t"+params["scope_type"]+"\t"+params["scope_desc"]+"\t"+params["item_type"])

switch = hc.createDataFrame(switch)
self_switch = switch.filter(switch.src_item_id == switch.dst_item_id).select('src_item_id','dst_item_id')
self_switch = self_switch.withColumnRenamed('src_item_id','item_sku_id').withColumnRenamed('dst_item_id','item_id')
m03 = m03.select('item_sku_id','sku_name')
sku_in_scope = self_switch.join(m03,on='item_sku_id',how='inner').select(self_switch.item_sku_id,self_switch.item_id,m03.sku_name)
sku_in_scope = sku_in_scope.withColumn('item_type', F.lit(params['item_type']))
sku_in_scope = sku_in_scope.withColumn('scope_id',F.lit(params['scope_id']))
sku_in_scope = sku_in_scope.withColumnRenamed('sku_name','item_name')
sku_in_scope = sku_in_scope.select('scope_id', 'item_sku_id', 'item_id', 'item_name', 'item_type')

sku_in_scope = sku_in_scope.collect()
sku_scope = output_path+'/sku_in_scope.txt'
with codecs.open(sku_scope,"w") as fp:
    for row in sku_in_scope:
        fp.write(row[0]+"\t"+row[1]+"\t"+row[2]+"\t"+row[3]+"\t"+row[4]+"\n")