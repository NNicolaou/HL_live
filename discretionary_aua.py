import pandas
import general
import datetime
import numpy
idx = pandas.IndexSlice


aua_frame = general.report_dic['aua'].loc[:,general.disc_known_cols]
# general.set_values(col_names=general.disc_known_cols,values=general.disc_known_values,date = general.prev_financial_year_end,df=aua_frame)
 
def get_composite_return(dic_data):
    '''
    Return a composite return series of HL funds, taking into account of proportion invested in the acc and inc units as well as fund size proportion
    '''
    
    acc_percent = dic_data['acc unit'] / (dic_data['acc unit'] + dic_data['inc unit'])
    inc_percent = dic_data['inc unit'] / (dic_data['acc unit'] + dic_data['inc unit'])
    
    acc_percent = general.fillna_monthly(acc_percent).reindex(index=general.month_end_series)
    inc_percent = general.fillna_monthly(inc_percent).reindex(index=general.month_end_series)
    #fund_size = dic_data['fund size'].reindex(index=general.month_end_series)
    #fund_size_percent = fund_size.div(fund_size.sum(axis='columns'), axis='index')
    
    acc_bid_price = dic_data['acc price'].reindex(index=general.month_end_series)
    inc_bid_price = dic_data['inc price'].reindex(index=general.month_end_series)
    acc_bid_return = acc_bid_price / acc_bid_price.shift(1) - 1
    inc_bid_return = inc_bid_price / inc_bid_price.shift(1) - 1
    
    fund_units = general.fillna_monthly((dic_data['acc unit'] + dic_data['inc unit'])).reindex(index=general.month_end_series)
    fund_units_percent = fund_units.div(fund_units.sum(axis='columns'), axis='index')
    
    
    composite_bid_return = acc_bid_return.where(acc_percent!=0,0) * acc_percent + inc_bid_return.where(inc_percent!=0,0) * inc_percent
    composite_bid_return = (composite_bid_return * fund_units_percent).sum(axis='columns')
    composite_bid_return = composite_bid_return.where(composite_bid_return.index <= pandas.to_datetime(general.last_day_prev_month))
    return composite_bid_return

def get_acc_composite_mul(dic_data):
    composite_return = get_composite_return(dic_data) # this is a series
    result = (composite_return+1).cumprod()
    return result

def compute_historic_aua(dic_data, df=aua_frame):
    test = dic_data['fund size']
    daily_fund_size = test.reindex(index=pandas.date_range(test.index.min(), general.month_end_series.max()))
    daily_fund_size = daily_fund_size.fillna(method='ffill')
    daily_fund_size[(daily_fund_size.index >= datetime.datetime.today())] = numpy.nan
    daily_fund_size = daily_fund_size.sum(axis='columns')
    current_fund_size = daily_fund_size.reindex(index=df.iloc[1:].index)
    total_last_result_size = df.iloc[0, :].sum() - df.iloc[0, :]['pms_others_aua']
    proportion_dic = {}
    for aua_type in ['pms_hlf_aua', 'vantage_hlf_aua', 'thirdparty_hlf_aua']:
        proportion_dic[aua_type] = df.iloc[0, :][aua_type] / total_last_result_size

    final_aua = df.copy()
    for aua_type, proportion in proportion_dic.items():
        final_aua.loc[1:, aua_type] = current_fund_size * proportion

    composite_mul = get_acc_composite_mul(dic_data)
    result = final_aua.fillna(method='ffill').multiply(composite_mul.where(composite_mul.isnull(), 1.0), axis='index')
    return result