import pandas
import numpy
import datetime
# import psycopg2
# from sqlalchemy import create_engine
# import sqlalchemy
idx = pandas.IndexSlice

fund_data_name = 'HL funds data.xlsx'
nnb_data_name = 'NNB data.xlsx'
fund_data_types = ['acc price','inc price','acc size','inc size','fund size','account number','trades']
nnb_data_sheet = ['total nnb','clients']
assumptions_name = 'control panel.xlsx'
assumptions_sheet = ['nnb distribution','nnb pcent total asset','compound growth','aua margin','growth rate','tax rate', 'nnc pcent total client']
index_hl_data_name = 'index and hl price data.xlsx'
index_hl_data_sheet = ['Index price', 'HL price', 'fx_rates']
report_data_name = 'financial report data.xlsx'
report_data_sheet = ['revenue','costs','aua']

def read_data(data_name, sheets):
    '''
    Return a dictionary of dataframes
    '''
    if type(sheets) == str:
        return pandas.read_excel(data_name, sheets).sort_index(axis='columns')
    else:
        dic = {}
        for items in sheets:
            dic[items] = pandas.read_excel(data_name, items)
            dic[items].sort_index(axis='columns',inplace=True)
        return dic
report_data = read_data(report_data_name, report_data_sheet)
#=======================================SQL DATA IMPORTS AND INTEGRATION========================================
'''
engine = create_engine('postgresql+psycopg2://postgres:H0rAT10@WP-AP1/postgres')
sheets = list(range(0,11))
sql_data_names = {}
sql_data_names['Index_Close_Price'] = 'Index price'
sql_data_names['Equity_Close_Price'] = 'HL price'
sql_data_names['NNB'] = 'total nnb'
sql_data_names['Funds_Close_Acc_Price'] = 'acc price'
sql_data_names['Funds_Close_Inc_Price'] = 'inc price'
sql_data_names['Funds_Size'] = 'fund size'
sql_data_names['Funds_Acc_Size'] = 'acc size'
sql_data_names['Funds_Inc_Size'] = 'inc size'

fin_report_names = {}
fin_report_names['Report_AUA'] = 'aua'
fin_report_names['Report_Costs'] = 'costs'
fin_report_names['Report_Revenue'] = 'revenue'
dic = {}
table_names = list(sql_data_names.keys()) + list(fin_report_names.keys())
data_dic = {}
for keys in sql_data_names.keys():
    data_dic[sql_data_names[keys]] = pandas.read_sql_table(keys, engine)
    data_dic[sql_data_names[keys]].set_index('Date', inplace=True)
    data_dic[sql_data_names[keys]].sort_index(axis='columns', inplace=True)
    
report_data = {}
for keys in fin_report_names.keys():
    report_data[fin_report_names[keys]] = pandas.read_sql_table(keys, engine)
    report_data[fin_report_names[keys]].set_index('Date', inplace=True)
    report_data[fin_report_names[keys]].sort_index(axis='columns', inplace=True)   
'''


