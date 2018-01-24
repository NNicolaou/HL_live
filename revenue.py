import pandas
import numpy
import datetime
idx = pandas.IndexSlice
import general
import discretionary_aua
import vantage_aua
import combined
import data_accessing

aua_frame = general.report_dic['revenue'].loc[:,general.revenue_known_cols]
#general.set_values(col_names=general.revenue_known_cols,values=general.revenue_known_values,date = general.prev_financial_year_end,df=aua_frame)

def cash_service(dic_data, input_dic, period='half_no'):
    aua = combined.total_aua(dic_data, input_dic)
    aua_margins = general.fillna_monthly(input_dic['aua margin']).reindex(index=general.month_end_series)
    return (aua['cash_service_aua'] * (aua_margins['cash_service']/12)).groupby(['financial_year',period]).sum()#.map(general.compound_growth_rate))

def platform_fee(dic_data, input_dic, period='half_no'):
    aua = combined.total_aua(dic_data, input_dic)
    aua_margins = general.fillna_monthly(input_dic['aua margin']).reindex(index=general.month_end_series)
    return (aua['total_funds_aua'] * (aua_margins['platform_fee']/12)).groupby(['financial_year',period]).sum()#.map(general.compound_growth_rate))

def hlf_amc(dic_data, input_dic, period='half_no'):
    aua = combined.total_aua(dic_data, input_dic)
    aua_margins = general.fillna_monthly(input_dic['aua margin']).reindex(index=general.month_end_series)
    return (aua['discretionary_aua'] * (aua_margins['hlf_amc']/12)).groupby(['financial_year',period]).sum()#.map(general.compound_growth_rate))

def hlf_amc_daily(dic_data, input_dic, period='half_no'):
    test = dic_data['fund size']
    daily_fund_size = test.reindex(index=pandas.date_range(test.index.min(),general.month_end_series.max()))
    daily_fund_size = daily_fund_size.fillna(method='ffill')
    daily_fund_size[(daily_fund_size.index>=datetime.datetime.today())] = numpy.nan
    compound_rate = general.fillna_monthly(input_dic['compound growth'].reindex(index=daily_fund_size.index)).apply(general.compound_growth_rate,freq='Daily')
    sliced_compound_rate = compound_rate.loc[pandas.to_datetime('today'):,:]
    sliced_fund_size = daily_fund_size[pandas.to_datetime('today'):].fillna(method='ffill')
    working_compound= (1+sliced_compound_rate).pow((sliced_compound_rate.count(axis='columns').cumsum()-1),axis='index')
    sliced_fund_size = sliced_fund_size.multiply(working_compound['compound growth rate'], axis='index')    
    b = combined.future_nnb_distribution(dic_data, input_dic).reindex(sliced_fund_size.index)
    temp = b.loc[:,'vantage_hlf_aua'] + b.loc[:,'thirdparty_hlf_aua'] + b.loc[:,'pms_others_aua'] + b.loc[:,'pms_hlf_aua']
    temp.name='nnb'
    temp = temp.to_frame()
    temp['key'] = pandas.to_numeric(temp.index.year.astype(str) + temp.index.month.astype(str))
    temp = temp.fillna(method='bfill')              
    count_map = temp.groupby('key').count()
    count_map.iloc[0,:] = 30              
    temp['count'] = temp['key'].map(count_map.to_dict()['nnb'])
    temp['nnb'] = temp['nnb'] / temp['count']          
    portion = daily_fund_size.loc[pandas.to_datetime('today'),:] / daily_fund_size.loc[pandas.to_datetime('today'),:].sum()
    nnb = temp['nnb']         
    temp2 = pandas.DataFrame(columns=portion.index,index=nnb.index)
    temp2.loc[:,:] = portion.values
    nnb = temp2.multiply(nnb,axis='index')
    sliced_fund_size=sliced_fund_size+nnb.cumsum(axis='index')
    final_fund_size = daily_fund_size.combine_first(sliced_fund_size)              
    select_revenue = final_fund_size[['Select UK Growth Shares','Select UK Income Shares']].sum(axis='columns')
    select_revenue = select_revenue*(0.006/365)
    hlf_revenue = final_fund_size.drop(['Select UK Growth Shares','Select UK Income Shares'], axis='columns').sum(axis='columns')
    hlf_revenue = hlf_revenue*(0.0075/365)              
    total_hlf = select_revenue+hlf_revenue
    total_hlf.name='hlf_revenue'              
    result = general.convert_fy_quarter_half_index(total_hlf,total_hlf.index)              
    final_result = result.groupby(['financial_year',period]).sum().loc[idx[general.recent_end_year:,:],:]
    if general.last_result_month == 6:
        final_result = final_result.drop((general.recent_end_year,1),axis='index')
    final_result = final_result.stack()      
    final_result.index = final_result.index.droplevel(2)
    return final_result
                  
                  
                  
def pms_advice_fee(dic_data, input_dic, period='half_no'):
    aua = combined.total_aua(dic_data, input_dic)
    aua_margins = general.fillna_monthly(input_dic['aua margin']).reindex(index=general.month_end_series)
    return (aua['pms_aua'] * (aua_margins['pms_advice']/12)).groupby(['financial_year',period]).sum()#.map(general.compound_growth_rate))

def cash_interest(dic_data, input_dic, period='half_no'):
    aua = combined.total_aua(dic_data, input_dic)
    annual_libor_revenue = aua['vantage_cash_aua']*general.account_cash_dist['sipp']*0.8*(general.annual_libor_mean(dic_data)/12)#.map(general.compound_growth_rate)
    overnight_libor = dic_data['Index price'].loc[:, 'Overnight LIBOR'].fillna(method='ffill').reindex(index=general.month_end_series).fillna(method='ffill')
    overnight_libor_revenue = aua['vantage_cash_aua']*(general.account_cash_dist['sipp'] * 0.2 + (1 - general.account_cash_dist['sipp']))*(overnight_libor/12)#.map(general.compound_growth_rate)
    return (annual_libor_revenue + overnight_libor_revenue).groupby(['financial_year',period]).sum()

def cash_interest_margin(dic_data):
    annual = general.annual_libor_mean(dic_data)
    overnight = dic_data['Index price'].loc[:, 'Overnight LIBOR'].fillna(method='ffill').reindex(index=general.month_end_series).fillna(method='ffill')
    margin = general.account_cash_dist['sipp']*0.8*annual+(general.account_cash_dist['sipp']*0.2+(1-general.account_cash_dist['sipp']))*overnight
    return margin

def paper_statement_revenue(dic_data):
    df = general.fillna_monthly(dic_data['clients']).reindex(index=general.semi_annual_series)
    df2 = pandas.DataFrame(general.client_number_growth_semi, index=df.index, columns=df.columns)
    df2.iloc[0,:] = 0
    result = ((df2+1).cumprod()).multiply(df,axis='index') * general.paper_client_pcent * general.paper_charge_semi
    result.columns=['paper_income']
    return result['paper_income']


def growth_revenues(input_dic):
    df = aua_frame.copy()
    df = df.loc[:,general.growth_revenue_cols]
    revenue_growth = general.fillna_monthly(input_dic['growth rate']).loc[:,general.growth_revenue_cols].reindex(index=general.semi_annual_series)
    revenue_growth.iloc[0,:]=0
    result = df.fillna(method='ffill').multiply(((revenue_growth+1).cumprod()),axis='index')
    return result

def semi_revenue(dic_data, input_dic):
    df = aua_frame.copy()

    df.loc[:,general.growth_revenue_cols] = growth_revenues(input_dic)
    
    series = cash_service(dic_data, input_dic)
    series[0] = 0
    df.loc[:,'cash_service'] = df.loc[:,'cash_service'].fillna(0) + series.values 
    
    series = hlf_amc_daily(dic_data, input_dic)
    series[0] = 0
    df.loc[:,'hlf_amc'] = df.loc[:,'hlf_amc'].fillna(0) + series.values
    
    series = platform_fee(dic_data, input_dic)
    series[0] = 0
    df.loc[:,'platform_fee'] = df.loc[:,'platform_fee'].fillna(0) + series.values
    
    series = pms_advice_fee(dic_data, input_dic)
    series[0] = 0
    df.loc[:,'pms_advice'] = df.loc[:,'pms_advice'].fillna(0) + series.values
    
    series = cash_interest(dic_data, input_dic)
    series[0] = 0
    df.loc[:,'interest_on_cash'] = df.loc[:,'interest_on_cash'].fillna(0) + series.values
    
    series = paper_statement_revenue(dic_data)
    series[0] = 0
    df.loc[:,'paper_income'] = df.loc[:,'paper_income'].fillna(0) + series.values
    
    result = general.convert_fy_quarter_half_index(df, general.semi_annual_series)
    return result

def annual_revenue(dic_data, input_dic, cal_year=False):
    df = semi_revenue(dic_data, input_dic)
    if cal_year is False:
        if general.last_result_month == 6:
            return df.groupby('financial_year').sum().iloc[1:,:]
        else:
            return df.groupby('financial_year').sum()
    else:
        return df.groupby('calendar_year').sum().iloc[1:,:]
    
