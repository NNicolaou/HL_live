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
import consolidated

if general.last_result_month == 6:
    dcf_start_year = general.recent_end_year+1
else:
    dcf_start_year = general.recent_end_year
#===============================================Inputs=========================================================
dcf_period = 9
perpetuity_growth_rate = 0.0375  # Gordon growth rate - long term growth rate
no_of_shares = 474720010
net_debt_cash = 211000000.0
discount_rate = 0.0825 # WACC - cost of equity
#=============================================================================================================
dcf_end_year = dcf_start_year + dcf_period - 1
def disc_cash_flow(dic_data, input_dic):
    if general.last_result_month == 6:
        df1 = consolidated.annual_revenue_analysis(dic_data, input_dic).loc[dcf_start_year:dcf_end_year,:]
        df2 = consolidated.annual_costs_analysis(input_dic).loc[dcf_start_year:dcf_end_year,:]
        ebit = df1['Total revenue']+revenue.annual_revenue(dic_data, input_dic).loc[dcf_start_year:dcf_end_year,:]['interest_on_reserve'] + df2['Total operating costs'] + revenue.annual_revenue(dic_data, input_dic).loc[dcf_start_year:dcf_end_year,:]['currency_revenue'] 
        dcf = ebit.to_frame(name='EBIT').transpose()
        tax_rate = general.fillna_monthly(input_dic['tax rate']).reindex(index=dcf.columns).transpose()
        dcf.loc['Tax',:] = dcf.loc['EBIT',:] * tax_rate.loc['Tax',:]
        dcf.loc['EAT',:] = dcf.loc['EBIT',:] - dcf.loc['Tax',:]
        dcf.loc['Depreciation',:] = costs.annual_costs(input_dic).loc[dcf_start_year:dcf_end_year,:]['depre_amort_financial'].abs()
        dcf.loc['Capital Expenditure',:] = costs.annual_costs(input_dic).loc[dcf_start_year:dcf_end_year,:]['capital_expenditure'].abs()
        dcf.loc['Free cash flow',:] = dcf.loc['EAT',:] + dcf.loc['Depreciation',:] - dcf.loc['Capital Expenditure',:]
    else:
        df1 = consolidated.annual_revenue_analysis(dic_data, input_dic, cal_year=True).loc[dcf_start_year:dcf_end_year,:]
        df2 = consolidated.annual_costs_analysis(input_dic,cal_year=True).loc[dcf_start_year:dcf_end_year,:]
        ebit = df1['Total revenue']+revenue.annual_revenue(dic_data, input_dic,cal_year=True).loc[dcf_start_year:dcf_end_year,:]['interest_on_reserve'] + df2['Total operating costs'] + revenue.annual_revenue(dic_data, input_dic,cal_year=True).loc[dcf_start_year:dcf_end_year,:]['currency_revenue'] 
        dcf = ebit.to_frame(name='EBIT').transpose()
        tax_rate = general.fillna_monthly(input_dic['tax rate']).reindex(index=dcf.columns).transpose()
        dcf.loc['Tax',:] = dcf.loc['EBIT',:] * tax_rate.loc['Tax',:]
        dcf.loc['EAT',:] = dcf.loc['EBIT',:] - dcf.loc['Tax',:]
        dcf.loc['Depreciation',:] = costs.annual_costs(input_dic,cal_year=True).loc[dcf_start_year:dcf_end_year,:]['depre_amort_financial'].abs()
        dcf.loc['Capital Expenditure',:] = costs.annual_costs(input_dic,cal_year=True).loc[dcf_start_year:dcf_end_year,:]['capital_expenditure'].abs()
        dcf.loc['Free cash flow',:] = dcf.loc['EAT',:] + dcf.loc['Depreciation',:] - dcf.loc['Capital Expenditure',:]
    
    s1 = pandas.Series(1, index=dcf.columns).cumsum()
    s2 = pandas.Series(1+discount_rate, index=dcf.columns)
    
    discount_factors = s2 ** s1
    dcf.loc['Discounted cash flow',:] = dcf.loc['Free cash flow',:] / (s2 ** s1)
    
    return dcf

def fair_value(dic_data, input_dic):
    df = disc_cash_flow(dic_data, input_dic)
    hl = pandas.DataFrame(index=['Terminal value','Enterprise value', 'Net debt&cash','Fair value','No. of shares','Fair value per share'], columns=['HL'])
    terminal_value = (df.loc['Free cash flow',dcf_end_year]*(1+perpetuity_growth_rate)) / (discount_rate-perpetuity_growth_rate)
    hl.loc['Terminal value',:] = terminal_value
    enterprise_value = df.loc['Discounted cash flow',:].sum() + (terminal_value/((1+discount_rate)**dcf_period))
    hl.loc['Enterprise value',:] = enterprise_value
    hl.loc['Net debt&cash',:] = net_debt_cash
    hl.loc['Fair value',:] = enterprise_value + net_debt_cash
    hl.loc['No. of shares',:] = no_of_shares
    hl.loc['Fair value per share',:] = hl.loc['Fair value',:] / no_of_shares
    return hl
    
    