import pandas
import numpy
import datetime
idx = pandas.IndexSlice
import data_accessing
import general
import discretionary_aua

aua_frame = general.report_dic['aua'].loc[:,general.vantage_known_cols]
# general.set_values(col_names=general.vantage_known_cols, values=general.vantage_known_values, date=general.prev_financial_year_end,df=aua_frame)


def hl_shares_aua(dic_data, df=aua_frame):
    hl_price = dic_data['HL price'].reindex(general.month_end_series)['Adj Close']
    hl_return = hl_price / hl_price.shift(1).fillna(method='bfill') - 1
    return aua_frame['vantage_hl_shares_aua'].fillna(method='ffill').multiply((hl_return+1).cumprod())
    
    



def other_shares_aua(dic_data, df=aua_frame):
    ftse_all_shares = dic_data['Index price'].loc[:,['FTSE All Share','FTSE All Share TR']].reindex(index=general.month_end_series)
    returns = (ftse_all_shares / ftse_all_shares.shift(1).fillna(method='bfill') - 1).mean(axis='columns')
    return aua_frame['vantage_other_shares_aua'].fillna(method='ffill').multiply((returns+1).cumprod())

def other_funds_fx_return(dic_data):
    df = pandas.DataFrame(columns=general.fund_distribution_cols, index=general.month_end_series)
    df.sort_index(axis='columns',inplace=True)
    df.loc[:,'Cash'] =1
    df.loc[:,'FTSE All Share'] =1
    df.loc[:,'FTSE All Share TR'] = 1
    df.loc[:,'FTSE UK Property'] =1
    df.loc[:,'Eurostoxx 50'] = dic_data['fx_rates']['GBPEUR'].reindex(index=df.index) * 0.97
    df.loc[:,'S&P500'] = dic_data['fx_rates']['GBPUSD'].reindex(index=df.index) * 0.96
    df.loc[:,'Nikkei 225'] = dic_data['fx_rates']['GBPJPY'].reindex(index=df.index) * 0.92
    df.loc[:,'Emerging Europe'] = dic_data['fx_rates']['GBPUSD'].reindex(index=df.index)
    df.loc[:,'Emerging Asia'] = dic_data['fx_rates']['GBPUSD'].reindex(index=df.index)
    df.loc[:,'China'] = dic_data['fx_rates']['GBPCNY'].reindex(index=df.index)
    df.loc[:,'South Korea'] = dic_data['fx_rates']['GBPKRW'].reindex(index=df.index)
    df.loc[:,'South America'] = dic_data['fx_rates']['GBPUSD'].reindex(index=df.index)
    df.loc[:,'New Zealand'] = dic_data['fx_rates']['GBPNZD'].reindex(index=df.index)
    df.loc[:,'Australia'] = dic_data['fx_rates']['GBPAUD'].reindex(index=df.index)
    df.loc[:,'Developed Asia'] = dic_data['fx_rates']['GBPUSD'].reindex(index=df.index)
    
    df = (df/df.shift(1) - 1).fillna(0)
    
    return df

def other_funds_composite_return(dic_data):
    df = pandas.DataFrame(columns=general.fund_distribution_cols, index=general.month_end_series)
    df.iloc[0,:]=general.fund_distribution_values
    df.sort_index(axis='columns',inplace=True)
    df = df.fillna(method='ffill')
    index_price = dic_data['Index price'].reindex(general.fund_distribution_cols,axis='columns').reindex(index=general.month_end_series).sort_index(axis='columns')
    index_return = index_price / index_price.shift(1).fillna(method='bfill')-1
    index_return.loc[:,'Cash']=0.0
    
    df2 = other_funds_fx_return(dic_data)
    index_return = index_return - df2
    
    composite_returns = (index_return * df).sum(axis='columns')
    composite_returns = composite_returns.where(composite_returns.index <= pandas.to_datetime(general.last_day_prev_month))
    return composite_returns

def other_funds_aua(dic_data, df=aua_frame):
    composite_returns = other_funds_composite_return(dic_data)
    return aua_frame['vantage_other_funds_aua'].fillna(method='ffill').multiply((composite_returns+1).cumprod())
   
    
def compute_historic_aua(dic_data, input_dic, df=aua_frame):
    final_aua = aua_frame.copy()
    final_aua.loc[:,'vantage_hl_shares_aua'] = hl_shares_aua(dic_data)
    final_aua.loc[:,'vantage_other_shares_aua'] = other_shares_aua(dic_data)
    final_aua.loc[:,'vantage_other_funds_aua'] = other_funds_aua(dic_data)
    final_aua = final_aua.loc[:general.last_day_prev_month,:].fillna(method='ffill').reindex(index=general.month_end_series)    
    return final_aua
    
    

