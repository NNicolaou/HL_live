import pandas
import general
import combined
import revenue
import costs
from data_access import data_accessing

idx = pandas.IndexSlice

revenue_cols = ['Platform fees','Net renewal income','Management fees','HL Fund AMC','Stockbroking income','Interest receivable','Adviser charges','Funds Library','Cash Service','Other income','On-going adviser charges']
costs_cols = ['Staff costs', 'Marketing and distribution spend','Depreciation, amortisation & financial costs','Office running costs','Other costs','FSCS levy costs']
aua_cols = ['Vantage AUA','PMS AUA','HLMM Funds AUA','Cash Service AUA']
nnb_cols = ['Vantage nnb','PMS nnb','HLMM Funds nnb','Cash Service nnb']

def revenue_analysis(dic_data, input_dic):
    
    # without currency revenue and interest on reserve
    _revenue = revenue.semi_revenue(dic_data, input_dic)
        
        
    df = pandas.DataFrame(index=_revenue.index, columns=revenue_cols)
    df.loc[:,'Cash Service'] = _revenue['cash_service']
    df.loc[:,'Platform fees'] = _revenue['platform_fee']
    df.loc[:,'Net renewal income'] = _revenue['renewal_income']
    df.loc[:,'Management fees'] = _revenue['management_fee']
    df.loc[:,'On-going adviser charges'] = _revenue['pms_advice']
    df.loc[:,'HL Fund AMC'] = _revenue['hlf_amc']
    df.loc[:,'Stockbroking income'] = _revenue['stockbroking_commission'] + _revenue['stockbroking_income']
    df.loc[:,'Interest receivable'] = _revenue['interest_on_cash']
    df.loc[:,'Adviser charges'] = _revenue['advice_fee']
    df.loc[:,'Funds Library'] = _revenue['funds_library']
    df.loc[:,'Other income'] = _revenue['paper_income'] + _revenue['other_income']
    df.loc[:,'Total net revenue'] = df.sum(axis='columns')
    
    return df

def annual_revenue_analysis(dic_data, input_dic, cal_year=False):
    df = revenue_analysis(dic_data, input_dic)
    if cal_year is False:
        if general.last_result_month == 6:
            return df.groupby('financial_year').sum(min_count=1).iloc[1:,:]
        else:
            return df.groupby('financial_year').sum(min_count=1)
    else:
        return df.groupby('calendar_year').sum(min_count=1).iloc[1:,:]


def costs_analysis(input_dic):
    
    _costs = costs.semi_costs(input_dic)
      
    df = pandas.DataFrame(index=_costs.index, columns=costs_cols)
    df.loc[:, 'Staff costs'] = _costs['staff_costs']
    df.loc[:, 'Marketing and distribution spend'] = _costs['marketing_distribution']
    df.loc[:, 'Depreciation, amortisation & financial costs'] = _costs['depre_amort_financial']
    df.loc[:, 'Office running costs'] = _costs['office_running']
    df.loc[:, 'Other costs'] = _costs['others']
    df.loc[:, 'FSCS levy costs'] = _costs['FSCS_levy']
    df.loc[:, 'Total operating costs'] = df.sum(axis='columns')
    return df

def annual_costs_analysis(input_dic,cal_year=False):
    df = costs_analysis(input_dic)
    if cal_year is False:
        if general.last_result_month == 6:
            return df.groupby('financial_year').sum(min_count=1).iloc[1:,:]
        else:
            return df.groupby('financial_year').sum(min_count=1)
    else:
        return df.groupby('calendar_year').sum(min_count=1).iloc[1:,:]

def get_revenue_compare(dic_data, input_dic, half, year=general.recent_end_year):
    actual_df = convert_report_revenue_data(half,year)
    if type(year)==int:
        year = [year]
    
    if half is True:
        df = revenue_analysis(dic_data, input_dic).groupby(['financial_year','half_no']).sum(min_count=1)
        df = df.loc[idx[year,1],:].transpose()
    else:
        df = annual_revenue_analysis(dic_data, input_dic)
        df = df.loc[year,:].transpose()
        
    dic = {'Horatio forecast':df, 'Actual': actual_df}
    result = pandas.concat(dic, axis='columns')
    return result



def get_costs_compare(input_dic, half, year=general.recent_end_year):
    actual_df = convert_report_costs_data(half,year)
    if type(year)==int:
        year = [year]
    
    if half is True:
        df = costs_analysis(input_dic).groupby(['financial_year','half_no']).sum(min_count=1)
        df = df.loc[idx[year,1],:].transpose()
    else:
        df = annual_costs_analysis(input_dic)
        df = df.loc[year,:].transpose()
    dic = {'Horatio forecast':df.abs(), 'Actual': actual_df.abs()}
    result = pandas.concat(dic, axis='columns')

    return result
        
def aua_analysis(dic_data, input_dic):
    df = combined.total_aua(dic_data, input_dic).reset_index().set_index('month_end').reindex(index=general.semi_annual_series)
    df.index.name='month_end'
    df = df.reset_index().set_index(['month_end','financial_year','quarter_no','half_no'])
    result = pandas.DataFrame(index= df.index, columns = aua_cols)
    result.loc[:,'Cash Service AUA'] = df['cash_service_aua']
    result.loc[:,'Vantage AUA'] = df['vantage_aua']
    result.loc[:,'PMS AUA'] = df['pms_aua']
    result.loc[:,'HLMM Funds AUA'] = df['discretionary_aua']
    result.loc[:, 'Total AUA']= result.loc[:,'Vantage AUA'] + result.loc[:,'PMS AUA'] + result.loc[:,'Cash Service AUA']
    
    
    return result.groupby(['financial_year','quarter_no','half_no']).sum(min_count=1)
    
def nnb_analysis(dic_data, input_dic):
    df = combined.nnb_distribution(dic_data, input_dic, idx=dic_data['total nnb'].index)   # needs update
    df = general.convert_fy_quarter_half_index(df, index=df.index).groupby(['financial_year','quarter_no','half_no']).sum(min_count=1)
    result = pandas.DataFrame(index=df.index, columns=nnb_cols)
    result.loc[:,'Vantage nnb'] = df.loc[:,'vantage_hl_shares_aua'] + df.loc[:,'vantage_other_shares_aua'] + df.loc[:,'vantage_other_funds_aua'] + df.loc[:,'vantage_hlf_aua'] + df.loc[:,'thirdparty_hlf_aua'] + df.loc[:,'vantage_cash_aua']
    result.loc[:,'PMS nnb'] = df['pms_hlf_aua'] + df['pms_others_aua']
    result.loc[:,'HLMM Funds nnb'] = df.loc[:,'vantage_hlf_aua'] + df.loc[:,'thirdparty_hlf_aua'] + df['pms_hlf_aua'] + df['pms_others_aua']
    result.loc[:,'Cash Service nnb'] = df['cash_service_aua']
    result.loc[:,'Total nnb'] = result.loc[:,'Vantage nnb'] + result.loc[:,'PMS nnb'] + result.loc[:,'Cash Service nnb']

    return result

def get_nnb_compare(dic_data, input_dic, year=general.recent_end_year):
    if type(year)==int:
        if general.last_result_month == 6:
            year=year+1
        year = [year]
        
    df = nnb_analysis(dic_data, input_dic).groupby(['financial_year','quarter_no','half_no']).sum(min_count=1)
    return df.loc[idx[year,:],:]

def get_aua_compare(dic_data, input_dic, year=general.recent_end_year):
    actual_df = convert_report_aua_data(year)
    if type(year)==int:
        year = [year]
        
    df = aua_analysis(dic_data, input_dic).groupby(['financial_year','half_no']).sum(min_count=1)
    dic = {'Horatio forecast':df.loc[idx[year,:],:], 'Actual': actual_df}
    result = pandas.concat(dic, axis='columns')

    return result
    
def convert_report_revenue_data(half,year=general.recent_end_year,cal_year=False):
    _revenue = data_accessing.report_data['revenue']
    
    df = pandas.DataFrame(index=_revenue.index, columns=revenue_cols)
    df.loc[:,'Cash Service'] = _revenue['cash_service']
    df.loc[:,'Platform fees'] = _revenue['platform_fee']
    df.loc[:,'Net renewal income'] = _revenue['renewal_income']
    df.loc[:,'Management fees'] = _revenue['management_fee']
    df.loc[:,'On-going adviser charges'] = _revenue['pms_advice']
    df.loc[:,'HL Fund AMC'] = _revenue['hlf_amc']
    df.loc[:,'Stockbroking income'] = _revenue['stockbroking_commission'] + _revenue['stockbroking_income']
    df.loc[:,'Interest receivable'] = _revenue['interest_on_cash']
    df.loc[:,'Adviser charges'] = _revenue['advice_fee']
    df.loc[:,'Funds Library'] = _revenue['funds_library']
    df.loc[:,'Other income'] = _revenue['paper_income'] + _revenue['other_income']
    df.loc[:,'Total net revenue'] = df.sum(axis='columns')
    
    df = general.convert_fy_quarter_half_index(df,_revenue.index)
    if type(year)==int:
            year = [year]
    if cal_year is False:
        result = df.groupby(['financial_year','half_no']).sum(min_count=1) 

        if half is True:
            result = result.loc[idx[year,1],:].transpose()
        else:
            result = result.groupby('financial_year').sum(min_count=1).loc[year,:].transpose()

        return result
    else:
        result = df.groupby(['calendar_year','half_no']).sum(min_count=1) 

        if half is True:
            result = result.loc[idx[year,1],:].transpose()
        else:
            result = result.groupby('calendar_year').sum(min_count=1).loc[year,:].transpose()

        return result

def convert_report_costs_data(half, year=general.recent_end_year, cal_year=False):
    _costs = data_accessing.report_data['costs']
    
    df = pandas.DataFrame(index=_costs.index, columns=costs_cols)
    df.loc[:, 'Staff costs'] = _costs['staff_costs']
    df.loc[:, 'Marketing and distribution spend'] = _costs['marketing_distribution']
    df.loc[:, 'Depreciation, amortisation & financial costs'] = _costs['depre_amort_financial']
    df.loc[:, 'Office running costs'] = _costs['office_running']
    df.loc[:, 'Other costs'] = _costs['others']
    df.loc[:, 'FSCS levy costs'] = _costs['FSCS_levy']
    df.loc[:, 'Total operating costs'] = df.sum(axis='columns')
    
    df = general.convert_fy_quarter_half_index(df,_costs.index)
    
    if type(year)==int:
        year = [year]
        
        
    if cal_year is False:     
        result = df.groupby(['financial_year','half_no']).sum(min_count=1)
        if half is True:
            result = result.loc[idx[year,1],:].transpose()
        else:
            result = result.groupby('financial_year').sum(min_count=1).loc[year,:].transpose()
        return result
    else:
        result = df.groupby(['calendar_year','half_no']).sum(min_count=1)
        if half is True:
            result = result.loc[idx[year,1],:].transpose()
        else:
            result = result.groupby('calendar_year').sum(min_count=1).loc[year,:].transpose()
        return result

def convert_report_aua_data(year=general.recent_end_year):
    final_aua = data_accessing.report_data['aua']
    df = pandas.DataFrame(index= final_aua.index, columns = aua_cols)
    df.loc[:,'Vantage AUA'] = final_aua.loc[:,'vantage_hl_shares_aua'] + final_aua.loc[:,'vantage_other_shares_aua'] + final_aua.loc[:,'vantage_other_funds_aua'] + final_aua.loc[:,'vantage_hlf_aua'] + final_aua.loc[:,'thirdparty_hlf_aua'] + final_aua.loc[:,'vantage_cash_aua']
    df.loc[:,'PMS AUA'] = final_aua.loc[:,'pms_others_aua'] + final_aua.loc[:,'pms_hlf_aua']
    df.loc[:,'HLMM Funds AUA'] = final_aua.loc[:,'vantage_hlf_aua'] + final_aua.loc[:,'thirdparty_hlf_aua'] + final_aua.loc[:,'pms_others_aua'] + final_aua.loc[:,'pms_hlf_aua']
    df.loc[:,'Cash Service AUA'] = final_aua.loc[:,'cash_service_aua']
    df.loc[:, 'Total AUA']= df.loc[:,'Vantage AUA'] + df.loc[:,'PMS AUA'] + df.loc[:,'Cash Service AUA']
    df.index.name = 'month_end'
    
    df = general.convert_fy_quarter_half_index(df,final_aua.index)
    
    result = df.groupby(['financial_year','half_no']).sum(min_count=1)
    
    if type(year)==int:
        year = [year]
    

    result = result.loc[idx[year,:],:]
    return result