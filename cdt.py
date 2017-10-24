#-*- coding:utf-8 -*-
'''
CDT

Description:

Settings:
Input:
Output:
Functions:

'''

import yaml
import os
import sys
import subprocess
from datetime import datetime


from cdt_aux.cdt import CDTGenerationBasicElasticNet
from cdt_aux.cdt import CDTGenerationRFRegression
from attributes_cleaner.attrs_cleaner import *

# read command line arguments
n = len(sys.argv) - 1
if n < 1:
    print ('Usage: \n    python cdt.py param_file\n')
    sys.exit()
else:
    param_file = sys.argv[1]
    print ('[INFO] cdt started')

# read parameters
params = yaml.load( open('params/default.yaml', 'r') )
#param_file = 'params/sku_12214.yaml'
user_params = yaml.load( open(param_file, 'r') )
for key, value in user_params.items():
    params[key] = value


output_path = params['worker']['dir'] + '/output/' + params['EndDate'] + '/' + params['scope_id']
#  删除之前计算的结果文件
if os.path.exists(output_path + '/cdt.txt') | os.path.exists(output_path + '/cdt_cn.txt'):
    cmd = 'rm -f  %s '%(output_path + '/cdt*')
    status = subprocess.call(cmd,shell=True)
    if status == 0:
        print ('remove previous cdt.txt')
    else:
        print ('failed to remove previous cdt.txt')
        sys.exit()

if params['do_attribute_clean']:
    code = clean_attributes(params)
    if code != 'success':
        print ('attributes cleaner failed!')
        sys.exit()
    else:
        attr_cg_file = params['worker']['dir'] + '/temp/' + params['EndDate'] + '/' + \
                       params['item_third_cate_cd'] + '/attributes_categorical'
        attr_nm_file = params['worker']['dir'] + '/temp/' + params['EndDate'] + '/' + \
                       params['item_third_cate_cd'] + '/attributes_numerical'
        print ('attributes cleaner done!')
else:
    if 'attr_cg_file' not in params and 'attr_nm_file' not in params:
        print ('You did not specify any of attr_cg_file and attr_nm_file, use default')
        attr_cg_file = params['worker']['dir'] + '/temp/' + params['EndDate'] + '/' + \
                       params['item_third_cate_cd'] + '/attributes_categorical'
        attr_nm_file = params['worker']['dir'] + '/temp/' + params['EndDate'] + '/' + \
                       params['item_third_cate_cd'] + '/attributes_numerical'
    else:
        attr_cg_file = params['attr_cg_file']
        attr_nm_file = params['attr_nm_file']
        pass

# read raw data
switch_file = 'output/' + params['EndDate'] + '/' + params['scope_id'] + '/switching_prob.txt'
switchprob = pd.read_table(switch_file,quotechar='\0', header=None)
switchprob.columns=['scope_id','from.item_id','item_id','switch_prob','model']
attrs_cg = pd.read_table(attr_cg_file, quotechar='\0')
attrs_nm = pd.read_table(attr_nm_file, quotechar='\0')
if params['scope_type'] == 'lvl4':
    attrs_cg = attrs_cg[attrs_cg.attr_key != params['classify_attr']]

# filter ignored attributes for this scope
ign_file = 'input/ignored_attrs.txt'
ign = pd.read_table(ign_file, quotechar='\0')
ign_attr_names = ign.loc[ign['scope_id']==params['scope_id'].encode('utf-8'),'ignored_attr']
attrs_cg = attrs_cg[~attrs_cg['attr_key'].isin(ign_attr_names)]
attrs_nm = attrs_nm[~attrs_nm['attr_key'].isin(ign_attr_names)]

# convert item_id to string type
switchprob['from.item_id'] = switchprob['from.item_id'].apply(str)
switchprob['item_id'] = switchprob['item_id'].apply(str)
attrs_cg['item_id'] = attrs_cg['item_id'].apply(str)
attrs_nm['item_id'] = attrs_nm['item_id'].apply(str)

## format data
attr_matrix = switchprob[['item_id','from.item_id','switch_prob']].copy()
attr_matrix = attr_matrix[attr_matrix['switch_prob']>0]
attr_matrix = attr_matrix[attr_matrix['item_id']!=attr_matrix['from.item_id']]

# category variables
attrs_cg = attrs_cg.drop_duplicates()
attrs_cg = attrs_cg.groupby(['attr_key', 'item_id']).max().reset_index()
attrs_cg.index = attrs_cg['item_id']
attr_keys = attrs_cg.attr_key.unique()
for i_key in attr_keys:
    colname = i_key     #colname = i_key + '_comp'
    attr_vals = attrs_cg[attrs_cg['attr_key'] == i_key]['attr_val']
    val1 = attr_matrix['item_id'].map(lambda x: attr_vals.get(x, 'None'))
    val2 = attr_matrix['from.item_id'].map(lambda x: attr_vals.get(x, 'None'))
    attr_matrix[colname] = (val1 == val2) + 0
    attr_matrix.loc[(val1.isnull())|(val2.isnull()), colname] = attr_matrix[colname].mean()
# numeric variables
attrs_nm = attrs_nm.drop_duplicates()
attrs_nm = attrs_nm.groupby(['attr_key', 'item_id']).max().reset_index()
attrs_nm.index = attrs_nm['item_id']
attr_keys = attrs_nm.attr_key.unique()

#tmp = attr_matrix.copy()
for i_key in attr_keys:
    colname = i_key      # colname = i_key + '_comp'
    attr_vals = attrs_nm[attrs_nm['attr_key'] == i_key]['attr_val']
    #avg = np.mean(attr_vals)
    val1 = attr_matrix['item_id'].map(lambda x: attr_vals.get(x, np.nan))
    val2 = attr_matrix['from.item_id'].map(lambda x: attr_vals.get(x, np.nan))
    distance = map(lambda x: np.abs(np.log2(val1.iloc[x]+1)-np.log2(val2.iloc[x]+1)), range(len(val1)))
    distance = list(distance)
    attr_matrix[colname] = 1 -  distance / np.nanmax(distance)
    attr_matrix.loc[(np.isnan(val1)|np.isnan(val2)),colname] = attr_matrix[colname].mean()

# total net sales
#if params['sales_estimate'] == 'pred':
#    sale_summary_file = './output/%s/%s/pred/sale_summary_pred' % (params['EndDate'], params['scope_id'])
#else:
#    sale_summary_file = params['worker']['dir'] + '/temp/' + params['EndDate'] + '/' + \
#                    params['item_third_cate_cd'] + '/sale_summary'

#sale_summary = pd.read_table(sale_summary_file, quotechar='\0', dtype={'item_sku_id': str})
#sale_summary = sale_summary[sale_summary.sku_type == 'self']
#from_sale = sale_summary[['item_sku_id','total_net_sales']]
#from_sale.columns = ['from.item_id','fromSales']
#to_sale = sale_summary[['item_sku_id','total_net_sales']]
#to_sale.columns = ['item_id','toSales']

#sale_summary.set_index('item_sku_id', inplace=True)
#attr_vals = sale_summary['total_net_sales']
#attr_matrix['fromSales'] = attr_matrix['from.item_id'].map(lambda x: attr_vals.get(x, np.nan))
#attr_matrix['toSales'] = attr_matrix['item_id'].map(lambda x: attr_vals.get(x, np.nan))

ord_file = './input/%s/%s/gdm_m04_ord_det_sum' % (params['EndDate'], params['item_third_cate_cd'])
gdm04 = pd.read_table(ord_file,quotechar='\0',header='infer',sep='\t',dtype={'item_sku_id':'str'})
from_sale = gdm04.groupby(['item_sku_id'],as_index=False)['after_prefr_amount'].sum()
from_sale.columns = ['from.item_id','fromSales']
to_sale = from_sale.copy()
to_sale.columns = ['item_id','toSales']

attr_matrix = pd.merge(attr_matrix,from_sale,on='from.item_id',how='left')
attr_matrix = pd.merge(attr_matrix,to_sale,on='item_id',how='left')
attr_matrix['fromSales'].fillna(value=np.nanmean(attr_matrix['fromSales']), inplace=True)
attr_matrix['toSales'].fillna(value=np.nanmean(attr_matrix['toSales']), inplace=True)

#attr_matrix.to_csv("temp/attr_matrix.txt", index=False, header=True, sep="\t")
AttMatrix = attr_matrix.drop(['item_id','from.item_id'], axis = 1)
AttMatrix = AttMatrix.rename(columns={'switch_prob':'switchProb'})
Exclude = []

# remove duplicate attributes
name_cn = {'unit': '包装件数', 'volume': ['总容量','总重量'],
           'unit_volume': '单件容量', 'unit_price': '单件价格',
           'per_price': '单位容量价格','price':'价格'}

map_dict = {}
for k in name_cn:
    if (k in AttMatrix):
        cn_names = name_cn[k]
        flag = 0
        for v in cn_names:
            if (v in AttMatrix):
                del AttMatrix[v]
                map_dict[k] = v
                flag = 1
        if flag == 0:
            del AttMatrix[k]

# Generate the CDT
#RawCDTElasticNet = CDTGenerationElasticNet(Org=AttMatrix, Exclusions=Exclude)
#print('Elastic net regression successful')
#RawCDTElasticNet.to_sql(name='rawCDT', con=engine, schema='Group'+str(CannGroup), if_exists='replace')
#print('Elastic net Regression CDT transferred \n')

# Generate the CDT with scikit elastic net regression
#RawCDTBasicElasticNet = CDTGenerationBasicElasticNet(Org=AttMatrix, Exclusions=Exclude)
#print('Basic elastic net regression successful')

RawCDTRFRegression = CDTGenerationRFRegression(Org=AttMatrix, Exclusions=Exclude)
print('Random forest regression successful')

#res = RawCDTBasicElasticNet.append(RawCDTRFRegression)
res = RawCDTRFRegression
res['scope_id'] = params['scope_id']
res = res[['scope_id','attdesc','Estimate','SimulationType']]
output_file =  output_path + '/cdt.txt'
res.to_csv(output_file, sep='\t', header=False, index=False, quoting=None, encoding='utf-8')

# a copy of cdt result, name use defined attributes in chinese
name_cn = {'price':'价格', 'unit': '包装件数', 'volume': '总容量/总重量',
           'unit_volume': '单件容量/单件重量', 'unit_price': '单件价格',
           'per_price': '单位容量价格/单位重量价格'}

for k,v in map_dict.items():
    name_cn[k] = v

res['attdesc'] = res['attdesc'].apply(lambda x: name_cn.get(x, x))
output_file =  output_path + '/cdt_cn.txt'
res.to_csv(output_file, sep='\t', header=False, index=False, quoting=None, encoding='utf-8')

print ('[PIPE]', params['scope_id'])
print ('[INFO] cdt completed at %s \n'%(datetime.now()))

