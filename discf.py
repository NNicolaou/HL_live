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
import data_accessing

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
def disc_cash_flow(dic_data, input_dic, now=False):
    dcf_end = dcf_end_year
    if now is True:
        dcf_end = dcf_end -1
    if general.last_result_month == 6:
        df1 = consolidated.annual_revenue_analysis(dic_data, input_dic).loc[dcf_start_year:dcf_end,:]
        df2 = consolidated.annual_costs_analysis(input_dic).loc[dcf_start_year:dcf_end,:]
        ebit = df1['Total revenue']+revenue.annual_revenue(dic_data, input_dic).loc[dcf_start_year:dcf_end,:]['interest_on_reserve'] + df2['Total operating costs'] + revenue.annual_revenue(dic_data, input_dic).loc[dcf_start_year:dcf_end,:]['currency_revenue'] 
        dcf = ebit.to_frame(name='EBIT').transpose()            
        dcf.loc['Depreciation',:] = -(costs.annual_costs(input_dic).loc[dcf_start_year:dcf_end,:]['depre_amort_financial'])
        dcf.loc['Capital Expenditure',:] = -(costs.annual_costs(input_dic).loc[dcf_start_year:dcf_end,:]['capital_expenditure'])
        if now is True:
            dcf.loc['EBIT',dcf_start_year-1] = consolidated.convert_report_revenue_data(False, year=dcf_start_year-1).loc['Total revenue',dcf_start_year-1] + report_reformat('revenue').loc['interest_on_reserve'] + consolidated.convert_report_costs_data(False, year=dcf_start_year-1).loc['Total operating costs',dcf_start_year-1] + report_reformat('revenue').loc['currency_revenue']
            dcf.loc['Depreciation', dcf_start_year-1] = -(report_reformat('costs').loc['depre_amort_financial'])
            dcf.loc['Capital Expenditure', dcf_start_year-1] = -(report_reformat('costs').loc['capital_expenditure'])
            dcf = dcf.sort_index(axis='columns')
        tax_rate = general.fillna_monthly(input_dic['tax rate']).reindex(index=dcf.columns).transpose()
        dcf.loc['Tax',:] = dcf.loc['EBIT',:] * tax_rate.loc['Tax',:]
        dcf.loc['EAT',:] = dcf.loc['EBIT',:] - dcf.loc['Tax',:]
        dcf.loc['Free cash flow',:] = dcf.loc['EAT',:] + dcf.loc['Depreciation',:] - dcf.loc['Capital Expenditure',:]
    else:
        df1 = consolidated.annual_revenue_analysis(dic_data, input_dic, cal_year=True).loc[dcf_start_year:dcf_end,:]
        df2 = consolidated.annual_costs_analysis(input_dic,cal_year=True).loc[dcf_start_year:dcf_end,:]
        ebit = df1['Total revenue']+revenue.annual_revenue(dic_data, input_dic,cal_year=True).loc[dcf_start_year:dcf_end,:]['interest_on_reserve'] + df2['Total operating costs'] + revenue.annual_revenue(dic_data, input_dic,cal_year=True).loc[dcf_start_year:dcf_end,:]['currency_revenue'] 
        dcf = ebit.to_frame(name='EBIT').transpose()
        dcf.loc['Depreciation',:] = -(costs.annual_costs(input_dic,cal_year=True).loc[dcf_start_year:dcf_end,:]['depre_amort_financial'])
        dcf.loc['Capital Expenditure',:] = -(costs.annual_costs(input_dic,cal_year=True).loc[dcf_start_year:dcf_end,:]['capital_expenditure'])
        if now is True:
            dcf.loc['EBIT',dcf_start_year-1] = consolidated.convert_report_revenue_data(False, year=dcf_start_year-1,cal_year=True).loc['Total revenue',dcf_start_year-1] + report_reformat('revenue',cal_year=True).loc['interest_on_reserve'] + consolidated.convert_report_costs_data(False, year=dcf_start_year-1,cal_year=True).loc['Total operating costs',dcf_start_year-1] + report_reformat('revenue',cal_year=True).loc['currency_revenue']
            dcf.loc['Depreciation', dcf_start_year-1] = -(report_reformat('costs',cal_year=True).loc['depre_amort_financial'])
            dcf.loc['Capital Expenditure', dcf_start_year-1] = -(report_reformat('costs',cal_year=True).loc['capital_expenditure'])
            dcf = dcf.sort_index(axis='columns')
        tax_rate = general.fillna_monthly(input_dic['tax rate']).reindex(index=dcf.columns).transpose()
        dcf.loc['Tax',:] = dcf.loc['EBIT',:] * tax_rate.loc['Tax',:]
        dcf.loc['EAT',:] = dcf.loc['EBIT',:] - dcf.loc['Tax',:]
        dcf.loc['Free cash flow',:] = dcf.loc['EAT',:] + dcf.loc['Depreciation',:] - dcf.loc['Capital Expenditure',:]
    
    s1 = pandas.Series(1, index=dcf.columns).cumsum()
    s2 = pandas.Series(1+discount_rate, index=dcf.columns)
    
    discount_factors = s2 ** s1
    dcf.loc['Discounted cash flow',:] = dcf.loc['Free cash flow',:] / (s2 ** s1)
    
    return dcf

def fair_value(dic_data, input_dic,now=False):
    df = disc_cash_flow(dic_data, input_dic, now=now)
    dcf_end = dcf_end_year
    if now is True:
        dcf_end = dcf_end -1
    hl = pandas.DataFrame(index=['Terminal value','Enterprise value', 'Net debt&cash','Fair value','No. of shares','Fair value per share'], columns=['HL'])
    terminal_value = (df.loc['Free cash flow',dcf_end]*(1+perpetuity_growth_rate)) / (discount_rate-perpetuity_growth_rate)
    hl.loc['Terminal value',:] = terminal_value
    enterprise_value = df.loc['Discounted cash flow',:].sum() + (terminal_value/((1+discount_rate)**dcf_period))
    hl.loc['Enterprise value',:] = enterprise_value
    hl.loc['Net debt&cash',:] = net_debt_cash
    hl.loc['Fair value',:] = enterprise_value + net_debt_cash
    hl.loc['No. of shares',:] = no_of_shares
    hl.loc['Fair value per share',:] = hl.loc['Fair value',:] / no_of_shares
    return hl
    
def report_reformat(typ, year=dcf_start_year-1,cal_year=False):
    '''
    typ is either "revenue" or "costs"
    '''
    df = general.convert_fy_quarter_half_index(data_accessing.report_data[typ],data_accessing.report_data[typ].index)
    if cal_year is False:
        return df.groupby('financial_year').sum().loc[year,:]
    else:
        return df.groupby('calendar_year').sum().loc[year,:]
    