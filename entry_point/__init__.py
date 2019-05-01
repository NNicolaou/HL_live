import pandas
import general
import discretionary_aua
import vantage_aua
from data_access.data_accessing import pull_data_for_model, read_assumptions_from_google_sheet
import revenue
import costs
import consolidated
import discf
import combined
import stats
idx = pandas.IndexSlice

if __name__ =='__main__':
    print('Pulling data...')
    data_dic = pull_data_for_model()
    input_dic = read_assumptions_from_google_sheet()
    print('Data pulled successfully.')
    result = stats.summary_avg_aua_dist(data_dic, input_dic, period='month_no')
