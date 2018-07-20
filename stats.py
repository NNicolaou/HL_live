import pandas
import numpy
import datetime
idx = pandas.IndexSlice
import general
import discretionary_aua
import vantage_aua
import combined
import revenue
import costs
import data_accessing
import discf
import consolidated
'''
FY annual statistics
'''

def total_revenue(data_dic, input_dic, cal=False):
    # including currency and interest on reserve
    result = revenue.annual_revenue(data_dic,input_dic,cal).sum(axis='columns')
    result.name = 'Total Revenue'
    return result

def costs_no_capex(input_dic, cal=False):
    result = costs.annual_costs(input_dic,cal).sum(axis='columns')-costs.annual_costs(input_dic,cal)['capital_expenditure']
    result.name = 'Total Costs'
    return result

def net_earning_before_tax(data_dic, input_dic, cal=False):
    revenue = total_revenue(data_dic, input_dic, cal)
    costs = costs_no_capex(input_dic, cal)
    result = revenue + costs
    result.name = 'Net Earning Before Tax'
    return result

def net_earning_after_tax(data_dic, input_dic, cal=False):
    net_earning_bef_tax = net_earning_before_tax(data_dic, input_dic, cal)
    tax_rate = general.fillna_monthly(input_dic['tax rate']).reindex(net_earning_bef_tax.index).fillna(method='ffill')['Tax']
    result = net_earning_bef_tax*(1-tax_rate)
    result.name = 'Net Earning After Tax'
    return result

def earning_per_share(data_dic, input_dic, cal=False):
    '''
    Capital expenditure is not included in the calculation
    '''
    net_earning_af_tax = net_earning_after_tax(data_dic, input_dic, cal)
    result = net_earning_af_tax / discf.no_of_shares
    result.name = 'EPS'
    return result
'''
def total_aua(data_dic, input_dic):
    test = combined.total_aua(data_dic,input_dic)
    test.index  = test.index.get_level_values(level='month_end')
    if general.last_result_month == 6:
        temp = general.recent_end_year + 1
    else:
        temp = general.recent_end_year
    result = test[test.index.month==general.financial_year_month]['total_assets_aua']
    result =  general.convert_fy_quarter_half_index(result, result.index).groupby('financial_year').sum(min_count=1)['total_assets_aua'].loc[temp:]
    result.name='Total AUA'
    return result
'''

def summary_total(data_dic, input_dic, cal=False):
    df1 = total_revenue(data_dic, input_dic, cal)
    df2 = costs_no_capex(input_dic, cal)
    df = net_earning_before_tax(data_dic, input_dic, cal)
    df3 = net_earning_after_tax(data_dic, input_dic, cal)
    df4 = earning_per_share(data_dic, input_dic, cal)
    #df5 = total_aua(data_dic, input_dic)
    return pandas.concat([df1, df2, df, df3, df4],axis='columns')

def summary_revenue_dist(data_dic, input_dic):
    revenue_shares = revenue.annual_revenue(data_dic, input_dic)['management_fee']+revenue.annual_revenue(data_dic, input_dic)['stockbroking_commission']
    revenue_shares.name = 'Shares'
    revenue_funds = revenue.annual_revenue(data_dic, input_dic)['platform_fee']
    revenue_funds.name = 'Funds'
    revenue_hlf_amc = revenue.annual_revenue(data_dic, input_dic)['hlf_amc']
    revenue_hlf_amc.name = 'HLF AMC'
    revenue_cash = revenue.annual_revenue(data_dic, input_dic)['interest_on_cash']
    revenue_cash.name = 'Cash'
    revenue_cash_service = revenue.annual_revenue(data_dic, input_dic)['cash_service']
    revenue_cash_service.name = 'Cash Service'
    revenue_other = revenue.annual_revenue(data_dic, input_dic).drop(['management_fee','stockbroking_commission','platform_fee','hlf_amc','interest_on_cash','cash_service'], axis='columns').sum(axis='columns')
    revenue_other.name = 'Other'
    return pandas.concat([revenue_shares,revenue_funds,revenue_hlf_amc,revenue_cash,revenue_cash_service,revenue_other],axis='columns')

def summary_revenue_dist_percent(data_dic, input_dic):
    df = summary_revenue_dist(data_dic, input_dic)
    return df.divide(df.sum(axis='columns'),axis='index')

def summary_avg_aua_dist(data_dic, input_dic, period='financial_year'):
    if (period == 'financial_year') or (period == 'calendar_year'):
        df = combined.total_aua(data_dic, input_dic).groupby(period).mean()
    elif period=='month_no':
        df = combined.total_aua(data_dic, input_dic).groupby(['calendar_year',period]).mean()
    else:
        df = combined.total_aua(data_dic, input_dic).groupby(['financial_year',period]).mean()
    avg_aua_funds = df['Funds']
    avg_aua_shares = df['Shares']
    avg_aua_hlf_amc = df['HLF']
    avg_aua_cash = df['Cash']
    avg_aua_cash_service = df['cash_service_aua']
    avg_aua_cash_service.name = 'Cash Service'
    
    if general.last_result_month == 6:
        temp = general.recent_end_year + 1
    else:
        temp = general.recent_end_year
    result = pandas.concat([avg_aua_funds,avg_aua_shares,avg_aua_hlf_amc,avg_aua_cash,avg_aua_cash_service],axis='columns')
    return result.loc[temp:,:]

def cash_margin(data_dic, period):
    result = revenue.cash_interest_margin(data_dic)
    result.name='Cash Margin'
    if general.last_result_month == 6:
        temp = general.recent_end_year + 1
    else:
        temp = general.recent_end_year
    result = general.convert_fy_quarter_half_index(result, result.index)
    if period=='monthly':
        return result
    elif (period=='financial_year') or (period=='calendar_year'):
        return result.groupby(period).mean().loc[temp:,:]
    else:
        return result.groupby(['financial_year',period]).mean().loc[temp:,:]

def hlf_implied_actual_nnb(data_dic, input_dic):
    result = combined.historic_nnb_distribution(data_dic, input_dic)
    hlf_nnb = result['pms_others_aua'] +result['pms_hlf_aua'] +result['thirdparty_hlf_aua']+result['vantage_hlf_aua']
    result = general.convert_fy_quarter_half_index(hlf_nnb,hlf_nnb.index)
    return result

def hlf_to_date_implied_nnb(data_dic,typ=None, fund_opt=None):
    '''
    typ: 'day','month','quarter','annual'
    '''
    df = combined.get_historic_implied_nnb(data_dic,idx=data_dic['acc price'].index, funds_opt=fund_opt)
    df.name = 'HLF nnb'
    df2 = general.convert_fy_quarter_half_index(df,df.index)
    #df2 = df2.reset_index()
    #df2.loc[:,'month_no'] = pandas.DatetimeIndex(df2['month_end']).month
    result = df2#.set_index(['month_end','financial_year','quarter_no','half_no','calendar_year','month_no'])
    if typ=='day':
        return df[df.index<=pandas.to_datetime(datetime.datetime.today())]
    elif typ=='month':
        return result.groupby(['calendar_year','month_no']).sum(min_count=1)
    elif typ=='quarter':
        return result.groupby(['financial_year','quarter_no']).sum(min_count=1)
    elif typ=='semi-annual':
        return result.groupby(['financial_year','half_no']).sum(min_count=1)
    elif typ=='annual':
        return result.groupby('financial_year').sum(min_count=1)
    else:
        return result
    
def hlf_to_date_unit_change(data_dic, unit_type, typ=None, fund_opt=None):
    acc_df = data_dic['acc unit'].fillna(method='ffill')
    inc_df = data_dic['inc unit'].fillna(method='ffill')
    
    if fund_opt == 'no_select':
        acc_df.drop(['Select UK Growth Shares', 'Select UK Income Shares'], axis='columns')
        inc_df.drop(['Select UK Growth Shares', 'Select UK Income Shares'], axis='columns')
    
    acc_change = acc_df - acc_df.shift(1)
    inc_change = inc_df - inc_df.shift(1)
    
    acc_change_df = general.convert_fy_quarter_half_index(acc_change,acc_change.index)
    inc_change_df = general.convert_fy_quarter_half_index(inc_change,inc_change.index)
    
    if unit_type == 'acc':
        result = acc_change_df
        result2 = acc_change
    elif unit_type == 'inc':
        result = inc_change_df
        result2 = inc_change
    
    if typ=='day':
        return result2[result2.index<=pandas.to_datetime(datetime.datetime.today())]
    elif typ=='month':
        return result.groupby(['calendar_year','month_no']).sum(min_count=1)
    elif typ=='quarter':
        return result.groupby(['financial_year','quarter_no']).sum(min_count=1)
    elif typ=='semi-annual':
        return result.groupby(['financial_year','half_no']).sum(min_count=1)
    elif typ=='annual':
        return result.groupby('financial_year').sum(min_count=1)
    else:
        return result
    
def pat_projection(data_dic, input_dic):
    a = revenue.semi_revenue(data_dic, input_dic)
    b = costs.semi_costs(input_dic)
    r_result = a.sum(axis='columns')
    c_result = b.drop('capital_expenditure',axis='columns').sum(axis='columns')
    final = (r_result + c_result)
    tax_dic = input_dic['tax rate'].fillna(method='ffill').fillna(method='bfill').to_dict()['Tax']
    test = final.to_frame().reset_index()
    tax_rate = 1 - test['calendar_year'].map(tax_dic).fillna(method='ffill')
    tax_rate.index=final.index
    return final * tax_rate


def hlf_revenue_margin(data_dic, input_dic, period):
    if period is not None:
        df1 = revenue.hlf_daily_revenue(data_dic, input_dic, period)
        df2 = revenue.hlf_daily_fund_size(data_dic, input_dic, period)
        df2 = df2.sum(min_count=1,axis='columns')
        
        return result.loc[idx[general.recent_end_year-1:,:]]
    else:
        df1 = revenue.hlf_daily_revenue(data_dic, input_dic, period).reset_index().set_index('month_end')
        df2 = revenue.hlf_daily_fund_size(data_dic, input_dic, period).sum(min_count=1,axis='columns')
        df2.index.name='month_end'
        return df1['hlf_revenue'] / df2
    

def avg_hlf_size(data_dic, input_dic, period):
    df = revenue.hlf_daily_fund_size(data_dic, input_dic, period=period)
    result = df.sum(axis='columns')
    return result.loc[idx[general.recent_end_year-1:,:]]

def quarter_revenue(data_dic, input_dic):
    df = consolidated.revenue_analysis(data_dic, input_dic).reset_index()
    dic = {1: 0.5, 2:4.0/6}
    dic2 = {2:3, 1:1}
    dic3 = {1: 1, 2:0}
    df['quarter_dist'] = df['half_no'].map(dic)
    df['quarter_no'] = df['half_no'].map(dic2)
    df['year_to_date_dist'] = df['half_no'].map(dic3)
    df['Total quarter revenue'] = df['Total revenue'] * df['quarter_dist']
    df['Year_to_date_revenue'] = ((df['Total revenue'] * df['year_to_date_dist']).shift(1)).fillna(0) + df['Total quarter revenue']
    result = df[['quarter_no','financial_year','calendar_year','Total quarter revenue','Year_to_date_revenue']]
    return result.set_index(['quarter_no','financial_year','calendar_year']).iloc[idx[1::,:]]