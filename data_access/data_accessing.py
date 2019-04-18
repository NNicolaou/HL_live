from pandas import read_excel, to_datetime
from data_access.query import DatabaseQuery, db_config
from parameters import load_parameters_as_df

data_service = DatabaseQuery(**db_config)

assumptions_name = 'control panel.xlsx'
assumptions_sheet = ['nnb distribution', 'nnb pcent total asset', 'compound growth', 'aua margin', 'growth rate',
                     'tax rate', 'nnc pcent total client', 'cash interest rebate']

report_data_sheet = ['revenue','costs','aua']
index_hl_data_sheet = ['Index price', 'HL price', 'fx_rates']

data_dic_keys_to_table_db = {'acc price': 'hlf_acc_price', 'inc price': 'hlf_inc_price', 'acc size': 'hlf_acc_size',
                             'inc size': 'hlf_inc_size', 'Index price': 'index_price', 'HL price': 'hl_price',
                             'fx_rates': 'currency_price', 'total nnb': 'hl_nnb', 'clients': 'hl_nnc',
                             'revenue': 'hl_revenue', 'costs': 'hl_cost', 'aua': 'hl_aua', 'no_of_accounts': 'hl_accounts'}

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
        return dic.copy()

def read_assumptions_from_google_sheet(tabs=assumptions_sheet):
    dic ={}
    for tab in tabs:
        dic[tab] = load_parameters_as_df(tab)
    return dic.copy()


def append_fund_size(dic_data):
    dic_data['fund size'] = dic_data['acc size'] + dic_data['inc size']

def append_share_class_units(dic_data):#,idx=general.temp_month_end):
    '''
    dic_data as a result of read_funds_data from data_accessing library
    '''
    for x, y, z in zip(['acc unit','inc unit'],['acc size', 'inc size'],['acc price','inc price']):
        #dic_data[x] = dic_data[y].reindex(index=idx).divide(dic_data[z].reindex(index=idx) /100)
        dic_data[x] = dic_data[y].divide(dic_data[z]/100)
        dic_data[x].where(dic_data[y]!=0,0,inplace=True)

def pull_data_from_db(from_dt=None, data_types=list(data_dic_keys_to_table_db.keys())):
    result_dic = {}
    for data_type in data_types:
        result_dic[data_type] = data_service.select_time_series_table(data_dic_keys_to_table_db[data_type],
                                                                      from_dt=from_dt)
        result_dic[data_type]['Date'] = to_datetime(result_dic[data_type]['Date'])
        result_dic[data_type] = result_dic[data_type].sort_values(by='Date')
        result_dic[data_type] = result_dic[data_type].set_index('Date')
        result_dic[data_type].index.name = ''
    return result_dic.copy()

report_data = pull_data_from_db(from_dt=None, data_types=report_data_sheet)

def pull_data_for_model(from_dt=None):
    data_dic = pull_data_from_db(from_dt=from_dt)
    append_fund_size(data_dic)
    append_share_class_units(data_dic)
    return data_dic.copy()

if __name__ == '__main__':
    dic = read_assumptions_from_google_sheet()





