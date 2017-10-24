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
'''
FY annual statistics
'''

def total_revenue(data_dic, input_dic):
    result = revenue.annual_revenue(data_dic,input_dic).sum(axis='columns')
    result.name = 'Total Revenue'
    return result

def costs_no_capex(input_dic):
    result = costs.annual_costs(input_dic).sum(axis='columns')-costs.annual_costs(input_dic)['capital_expenditure']
    result.name = 'Total Costs'
    return result

def net_earning_before_tax(data_dic, input_dic):
    revenue = total_revenue(data_dic, input_dic)
    costs = costs_no_capex(input_dic)
    result = revenue + costs
    result.name = 'Net Earning Before Tax'
    return result

def net_earning_after_tax(data_dic, input_dic):
    net_earning_bef_tax = net_earning_before_tax(data_dic, input_dic)
    tax_rate = general.fillna_monthly(input_dic['tax rate']).reindex(net_earning_bef_tax.index).fillna(method='ffill')['Tax']
    result = net_earning_bef_tax*(1-tax_rate)
    result.name = 'Net Earning After Tax'
    return result

def earning_per_share(data_dic, input_dic):
    '''
    Capital expenditure is not included in the calculation
    '''
    net_earning_af_tax = net_earning_after_tax(data_dic, input_dic)
    result = net_earning_af_tax / discf.no_of_shares
    result.name = 'EPS'
    return result

def total_aua(data_dic, input_dic):
    test = combined.total_aua(data_dic,input_dic)
    test.index  = test.index.get_level_values(level='month_end')
    if general.last_result_month == 6:
        temp = general.recent_end_year + 1
    else:
        temp = general.recent_end_year
    result = test[test.index.month==general.financial_year_month]['total_assets_aua']
    result =  general.convert_fy_quarter_half_index(result, result.index).groupby('financial_year').sum()['total_assets_aua'].loc[temp:]
    result.name='Total AUA'
    return result

def summary_total(data_dic, input_dic):
    df1 = total_revenue(data_dic, input_dic)
    df2 = costs_no_capex(input_dic)
    df = net_earning_before_tax(data_dic, input_dic)
    df3 = net_earning_after_tax(data_dic, input_dic)
    df4 = earning_per_share(data_dic, input_dic)
    df5 = total_aua(data_dic, input_dic)
    return pandas.concat([df1, df2, df, df3, df4, df5],axis='columns')

def summary_revenue_dist(data_dic, input_dic):
    revenue_shares = revenue.annual_revenue(data_dic, input_dic)['management_fee']+revenue.annual_revenue(data_dic, input_dic)['stockbroking_commission']
    revenue_shares.name = 'Shares'
    revenue_funds = revenue.annual_revenue(data_dic, input_dic)['platform_fee']
    revenue_funds.name = 'Funds'
    revenue_hlf_amc = revenue.annual_revenue(data_dic, input_dic)['hlf_amc']
    revenue_hlf_amc.name = 'HLF AMC'
    revenue_cash = revenue.annual_revenue(data_dic, input_dic)['interest_on_cash']
    revenue_cash.name = 'Cash'
    revenue_other = revenue.annual_revenue(data_dic, input_dic).drop(['management_fee','stockbroking_commission','platform_fee','hlf_amc','interest_on_cash'], axis='columns').sum(axis='columns')
    revenue_other.name = 'Other'
    return pandas.concat([revenue_shares,revenue_funds,revenue_hlf_amc,revenue_cash,revenue_other],axis='columns')

def summary_revenue_dist_percent(data_dic, input_dic):
    df = summary_revenue_dist(data_dic, input_dic)
    return df.divide(df.sum(axis='columns'),axis='index')

def summary_avg_aua_dist(data_dic, input_dic):
    df = combined.total_aua(data_dic, input_dic).groupby('financial_year').mean()
    avg_aua_funds = df['total_funds_aua']
    avg_aua_funds.name = 'Funds'
    avg_aua_shares = df['vantage_shares_aua']
    avg_aua_shares.name = 'Shares'
    avg_aua_hlf_amc = df['discretionary_aua']
    avg_aua_hlf_amc.name = 'HLF AMC'
    avg_aua_cash = df['vantage_cash_aua']
    avg_aua_cash.name = 'Cash'
    if general.last_result_month == 6:
        temp = general.recent_end_year + 1
    else:
        temp = general.recent_end_year
    result = pandas.concat([avg_aua_funds,avg_aua_shares,avg_aua_hlf_amc,avg_aua_cash],axis='columns')
    return result.loc[temp:,:]

def cash_margin(data_dic):
    result = revenue.cash_interest_margin(data_dic)
    result.name='Cash Margin'
    if general.last_result_month == 6:
        temp = general.recent_end_year + 1
    else:
        temp = general.recent_end_year
    result = general.convert_fy_quarter_half_index(result, result.index)
    return result.groupby('financial_year').mean().loc[temp:,:]
    
    