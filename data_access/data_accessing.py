from pandas import read_excel
from data_access.query import DatabaseQuery, db_config
from discretionary_aua import append_fund_size, append_share_class_units

data_service = DatabaseQuery(**db_config)

nnb_data_name = 'NNB data.xlsx'
nnb_data_sheet = ['total nnb','clients']
assumptions_name = 'control panel.xlsx'
assumptions_sheet = ['nnb distribution', 'nnb pcent total asset', 'compound growth', 'aua margin', 'growth rate',
                     'tax rate', 'nnc pcent total client', 'cash interest rebate']


# report_data_name = 'financial report data.xlsx'
report_data_sheet = ['revenue','costs','aua']
# fund_data_name = 'HL funds data.xlsx'
fund_data_types = ['acc price','inc price','acc size','inc size','fund size','account number','trades']
# index_hl_data_name = 'index and hl price data.xlsx'
index_hl_data_sheet = ['Index price', 'HL price', 'fx_rates']

data_dic_keys_to_table_db = {'acc price': 'hlf_acc_price', 'inc price': 'hlf_inc_price', 'acc size': 'hlf_acc_size',
                             'inc size': 'hlf_inc_size', 'Index price': 'index_price', 'HL price': 'hl_price',
                             'fx_rates': 'currency_price'}

def read_data(data_name, sheets):
    '''
    Return a dictionary of dataframes
    '''
    if type(sheets) == str:
        return read_excel(data_name, sheets).sort_index(axis='columns')
    else:
        dic = {}
        for items in sheets:
            dic[items] = read_excel(data_name, items)
            dic[items].sort_index(axis='columns',inplace=True)
        return dic

def pull_data_from_db(from_dt, data_types=list(data_dic_keys_to_table_db.keys())):
    result_dic = {}
    for data_type in data_types:
        result_dic[data_type] = data_service.select_time_series_table(data_dic_keys_to_table_db[data_type],
                                                                      from_dt=from_dt)
    return result_dic

report_data = pull_data_from_db(from_dt=None, data_types=report_data_sheet)

def pull_data_for_model(from_dt):
    price_dic = pull_data_from_db(from_dt)
    nnb_dic = read_data(nnb_data_name, nnb_data_sheet)
    append_fund_size(price_dic)
    append_share_class_units(price_dic)
    data_dic = {**price_dic, **nnb_dic}
    return data_dic







