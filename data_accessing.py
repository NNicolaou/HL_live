import pandas
import numpy
import datetime
idx = pandas.IndexSlice

fund_data_name = 'HL funds data.xlsx'
nnb_data_name = 'NNB data.xlsx'
fund_data_types = ['acc price','inc price','acc size','inc size','fund size','account number','trades']
nnb_data_sheet = ['total nnb','clients']
assumptions_name = 'control panel.xlsx'
assumptions_sheet = ['nnb distribution','nnb pcent total asset','compound growth','aua margin','growth rate','tax rate']
index_hl_data_name = 'index and hl price data.xlsx'
index_hl_data_sheet = ['Index price', 'HL price']
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
