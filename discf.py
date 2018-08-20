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
no_of_shares = 474429699 # 474965441
net_debt_cash = 125300000    
discount_rate = 0.0825 # WACC - cost of equity
#=============================================================================================================
def disc_cash_flow(dic_data, input_dic, now=False, fractional=False, dcf_p=dcf_period, disc_rate = discount_rate):
    dcf_end = dcf_start_year + dcf_p# - 1
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
    
    
    s1 = pandas.Series(1, index=dcf.columns)
    s2 = pandas.Series(1+disc_rate, index=dcf.columns)
    if fractional == True:
        first_y = general.recent_end_year + 1
        last_factor = (365.0 - (pandas.Timestamp(first_y,general.last_result_month + 1,1) - pandas.Timedelta(days=1) - pandas.Timestamp.today()).days) / 365
        first_factor = ((pandas.Timestamp(first_y,general.last_result_month + 1,1) - pandas.Timedelta(days=1) - pandas.Timestamp.today()).days) / 365
    #============== fractional discount factor, varies with time=========================
        s1.iloc[0] = first_factor
        s1.iloc[-1] = last_factor
        dcf.loc['Free cash flow', dcf_start_year] = dcf.loc['Free cash flow', general.recent_end_year+1] * first_factor
        dcf.loc['Free cash flow', dcf_end] = dcf.loc['Free cash flow', general.recent_end_year+1] * last_factor
    #====================================================================================

    s1 = s1.cumsum()

    
    discount_factors = s2 ** s1
    dcf.loc['Discounted cash flow',:] = dcf.loc['Free cash flow',:] / (s2 ** s1)
    
    return dcf

def fair_value(dic_data, input_dic,now=False, fractional=False,dcf_p=dcf_period,disc_rate = discount_rate, pep_rate = perpetuity_growth_rate):
    df = disc_cash_flow(dic_data, input_dic, now=now,fractional=fractional,dcf_p=dcf_p, disc_rate=disc_rate)
    dcf_end = dcf_start_year + dcf_p# - 1
    if now is True:
        dcf_end = dcf_end -1
    hl = pandas.DataFrame(index=['Terminal value','Enterprise value', 'Net debt&cash','Fair value','No. of shares','Fair value per share'], columns=['HL'])
    terminal_value = (df.loc['Free cash flow',dcf_end-1]*(1+pep_rate)) / (disc_rate-pep_rate)
    hl.loc['Terminal value',:] = terminal_value
    
    if fractional==True:
        first_y = general.recent_end_year + 1
        first_factor = ((pandas.Timestamp(first_y,general.last_result_month + 1,1) - pandas.Timedelta(days=1) - pandas.Timestamp.today()).days) / 365
        mod_dcf_period = dcf_period-1+first_factor
    else:
        mod_dcf_period = dcf_period
        
    enterprise_value = df.iloc[-1,:-1].sum(min_count=1) + (terminal_value/((1+disc_rate)**mod_dcf_period)) # adjusted for fraction fair value
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
        return df.groupby('financial_year').sum(min_count=1).loc[year,:]
    else:
        return df.groupby('calendar_year').sum(min_count=1).loc[year,:]
    