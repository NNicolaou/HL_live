import pandas
import numpy
import datetime
import data_accessing
idx = pandas.IndexSlice
##############################################################################################################
'''
Inputs
'''
recent_end_year = 2017
last_result_month = 6
financial_year_month = 6
discretionary_aua_headers = ['pms_aua', 'pms_hlf_aua','pms_others_aua','vantage_hlf_aua','thirdparty_hlf_aua','hlf_aua','discretionary_aua']
                      
vantage_aua_headers =  ['vantage_hl_shares_aua','vantage_other_shares_aua','vantage_shares_aua','vantage_other_funds_aua','vantage_aua','vantage_cash_aua']

account_aua_headers = ['sipp_aua','isa_aua','fs_aua','sipp_cash_aua','sipp_funds_aua','sipp_shares_aua','isa_cash_aua','isa_funds_aua','isa_shares_aua','fs_cash_aua','fs_funds_aua','fs_shares_aua']
new_deposit_service_headers = ['deposit_cash_aua']

disc_known_cols = ['pms_hlf_aua','pms_others_aua','vantage_hlf_aua','thirdparty_hlf_aua']
disc_known_values = [3159000000.0,50000000.0,4303000000.0,69000000.0]

vantage_known_cols = ['vantage_hl_shares_aua','vantage_other_shares_aua','vantage_other_funds_aua','vantage_cash_aua']
vantage_known_values = [2134400000.0,20543600000.0,31692000000.0,7337000000.0]

fund_distribution_cols = ['FTSE All Share', 'FTSE All Share TR', 'Eurostoxx 50', 'SP Asia 50', 'S&P500', 'Nikkei 225','FTSE UK Property', 'Cash']
fund_distribution_values = [0.275,0.275,0.15,0.09,0.13,0.03,0.02,0.03]

nnb_quarterly_dist = {1:0.18, 2:0.18, 3:0.385, 4:0.255}
account_cash_dist = {'sipp':0.53125, 'isa':0.25, 'f&s':0.21875}

revenue_known_cols = ['renewal_income','management_fee','stockbroking_commission','stockbroking_income','interest_on_cash','hlf_amc','platform_fee','pms_advice', 'advice_fee','funds_library','paper_income','other_income','currency_revenue','interest_on_reserve']
revenue_known_values = [2800000.0,8300000.0,28000000.0,1400000.0,18500000.0,26300000.0,80500000.0,5400000.0,5150000.0,3900000.0,2150000.0,1500000.0,900000.0,440000.0]

growth_revenue_cols = ['advice_fee','currency_revenue','funds_library','interest_on_reserve','management_fee','other_income','renewal_income','stockbroking_commission','stockbroking_income']

growth_costs_cols = ['staff_costs','marketing_distribution','depre_amort_financial','office_running','FSCS_levy','others', 'capital_expenditure']

costs_known_cols = ['staff_costs','marketing_distribution','depre_amort_financial','office_running','FSCS_levy','others']

costs_known_values = [-31900000.0,-5800000.0,-3300000.0,-1800000.0,300000.0,-11900000.0, -5000000.0]

client_number_growth_semi = 0.05
paper_client_pcent = 0.24
paper_charge_semi = 10
###############################################################################################################
if last_result_month == 12:
    last_result_month = 0
    recent_end_year = recent_end_year + 1

prev_financial_year_end = (datetime.date(recent_end_year, last_result_month+1,1) - datetime.timedelta(days=1))
last_day_prev_month = (datetime.date.today().replace(day=1)-datetime.timedelta(days=1))

def prev_weekday(date):
    '''
    Given a datetime.date object, return its previous week day.
    '''
    if date.weekday() < 5:
        return date
    else:
        return date-datetime.timedelta(days=date.weekday()-4)

last_day_prev_month = prev_weekday(last_day_prev_month)
prev_financial_year_end = prev_weekday(prev_financial_year_end)
month_end_series = pandas.date_range(prev_financial_year_end,prev_weekday(datetime.date(recent_end_year+16,financial_year_month+1,1)-datetime.timedelta(days=1)),freq='BM')
libor_month_end = pandas.date_range(prev_weekday(datetime.date(recent_end_year-1,last_result_month+1,1)-datetime.timedelta(days=1)),prev_weekday(datetime.date(recent_end_year+17,financial_year_month+1,1)-datetime.timedelta(days=1)),freq='BM')
semi_annual_series = pandas.date_range(prev_financial_year_end, prev_weekday(datetime.date(recent_end_year+16,financial_year_month+1,1)-datetime.timedelta(days=1)), freq='6BM')
temp_month_end = pandas.date_range((datetime.date(2016, last_result_month+1,1) - datetime.timedelta(days=1)),prev_weekday(datetime.date(recent_end_year+16,financial_year_month+1,1)-datetime.timedelta(days=1)),freq='BM')

def compute_quarter_half_no(series, _recent_end_year=recent_end_year,_financial_year_month=financial_year_month,_last_result_month = last_result_month):
    '''
    This function computes the index for aua dataframe useage.
    Monthly index with quarter, half and financial year number attached for reference.
    It takes two integers and a series
    '''
    
    # month_end_series = pandas.date_range(prev_weekday(datetime.date(_recent_end_year,_last_result_month+1,1)-datetime.timedelta(days=1)),prev_weekday(datetime.date(_recent_end_year+16,_financial_year_month+1,1)-datetime.timedelta(days=1)),freq='BM')
    quarter = [1,1,1,2,2,2,3,3,3,4,4,4]
    half = [1,1,1,1,1,1,2,2,2,2,2,2]
    
    month_no = list(range(_financial_year_month+1, 13))
    month_no2 = list(range(1, _financial_year_month+1))
    months = month_no+month_no2
    quarter_no = dict(zip(months,quarter))
    half_no = dict(zip(months,half))
    temp_month_series = pandas.Series(series.month)
    
    quarter_series = temp_month_series.map(quarter_no)
    half_series = temp_month_series.map(half_no)
    financial_year_series = series.year.where(series.month <= _financial_year_month,series.year.where(series.month >_financial_year_month) +1)
    financial_year_series = pandas.Series(financial_year_series)
    
    result = pandas.concat([pandas.Series(series),financial_year_series,quarter_series,half_series],axis='columns')

    result.columns = ['month_end','financial_year','quarter_no','half_no']

    return result
    
def create_aua_df(cols,inx):
    '''
    Create an AUA dataframe with month end as its index
    '''
    result = pandas.DataFrame(columns=cols, index=inx)
    result.sort_index(axis='columns', inplace=True)
    result.sort_index(axis='index', inplace=True)
    result.fillna(numpy.nan,inplace=True)
    result.index.name='month_end'
    return result
    
def set_values(**kwargs):
    '''
    This function sets the values for items that on a specific month end date in the dataframe.
    Keywords: col_names, date, values, df
    '''
    if type(kwargs['col_names']) == str:
        kwargs['df'].loc[kwargs['date'],kwargs['col_names']] = kwargs['values']
    else:    
        for items,value in zip(kwargs['col_names'],kwargs['values']):
            kwargs['df'].loc[kwargs['date'],items] = value
            
def fillna_monthly(df):
    '''
    This function return the dataframe with the N/A values filled by the last available data, if there were no last available data, fill it using the next available data.
    '''
    result = df.fillna(method='ffill').fillna(method='bfill') #.loc[:last_day_prev_month,:]
    #result = result.reindex(index=month_end_series)
    return result

def monthly_fulfill(input_dic):
    '''
    Takes inputs dictionary and fill all the NA value up
    '''
    result = {}
    for keys in input_dic.keys():
        result[keys] = fillna_monthly(input_dic[keys])
    return result
        
def annual_libor_mean(dic_data):
    libor = dic_data['Index price'].loc[:,'Annual LIBOR']
    # assuming libor stay the same in the future, if not then type in the rate that you expect in the index and hl price data
    result = libor.reindex(index=libor_month_end).fillna(method='ffill').rolling(12).mean().reindex(index=month_end_series)
    return result


def compound_growth_rate(rate, freq='Monthly', to_annual=False):
    if type(freq) == str:
        dic = {'Daily':365, 'Monthly':12, 'Quarterly':4, 'Semi_annually':2}
        if to_annual is True:
            result = (1+rate)**dic[freq] - 1

        else:
            result = (rate+1)**(1.0/dic[freq]) - 1
        
        
    elif type(freq) == int:
        if to_annual is True:
            result = (1+rate)**freq - 1

        else:
            result = (rate+1)**(1.0/freq) - 1
        
    return result

def convert_fy_quarter_half_index(df, index):
    df.index.name='month_end'
    df2 = compute_quarter_half_no(index)
    df2 = df2.set_index('month_end')
    result = pandas.concat([df, df2], axis='columns')
    result = result.reset_index()
    result = result.set_index(['month_end','financial_year','quarter_no','half_no'])
    return result

def convert_report_data(dic):
    dic2 = {}
    for sheets in dic.keys():
        if sheets == 'aua':
            dic2[sheets] = dic[sheets].reindex(index=month_end_series)
            dic2[sheets].iloc[1:,:] = numpy.nan
        else:
            df = pandas.DataFrame(dic[sheets].iloc[1::2].values - dic[sheets].iloc[::2].values,index = dic[sheets].iloc[1::2].index, columns = dic[sheets].columns)
            dic[sheets].iloc[1::2] = df
            dic2[sheets] = dic[sheets].reindex(semi_annual_series)
            dic2[sheets].iloc[1:,:] = numpy.nan
        dic[sheets].index.name='month_end'
    return dic2

report_dic = convert_report_data(data_accessing.report_data)


    