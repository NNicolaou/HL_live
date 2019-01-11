import general

aua_frame = general.report_dic['costs'].loc[:,general.growth_costs_cols]
#general.set_values(col_names=general.growth_costs_cols,values=general.costs_known_values,date = general.prev_financial_year_end,df=aua_frame)

def total_costs(input_dic):

    df = aua_frame.copy()

    df = df.loc[:,general.growth_costs_cols]
    costs = general.fillna_monthly(input_dic['growth rate']).loc[:,general.growth_costs_cols].reindex(index=general.semi_annual_series)
    costs.iloc[0,:]=0
    result = df.fillna(method='ffill').multiply(((costs+1).cumprod()),axis='index')
    return result

def semi_costs(input_dic):
    df = total_costs(input_dic)
    
    result = general.convert_fy_quarter_half_index(df, general.semi_annual_series)
    return result

def annual_costs(input_dic, cal_year=False):
    df = semi_costs(input_dic)
    if cal_year is False:
        if general.last_result_month == 6:
            return df.groupby('financial_year').sum(min_count=1).iloc[1:,:]
        else:
            return df.groupby('financial_year').sum(min_count=1)
    else:
        return df.groupby('calendar_year').sum(min_count=1).iloc[1:,:]
