import pandas as pd
import glob,os
import numpy as np

import datetime
from functools import reduce
pd.set_option('display.expand_frame_repr', False)



import numpy.ma as ma
from itertools import groupby


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


##process the extracted matrix for cancellation (toAvaialble), cancelled turnover, and visits
##this is the further analysis for section 4.2

filePath=r'..\resultMatrix\updates'

os.chdir(filePath)
files=glob.glob('*.txt')

dfs_toAvaialble_money= {'p1': [],
              'p2': [],
              'p3': [],
              'p4': [],
              'p5': []
              }

dfs_toAvailable= {'p1': [],
              'p2': [],
              'p3': [],
              'p4': [],
              'p5': []
              }

lst_policyVisits_allCols={}

cnt=0
for file in files:
    print(file)
    m=np.genfromtxt(file, delimiter=",", filling_values=99)

    ###read scrape days - columns
    f = open(file)
    header = f.readline()
    scrapeDates = header.replace('# ','')[:-1].split(',')

    ###read calendar days -rows
    with open(file, "r") as fl:
        last_line = fl.readlines()[-1]
    # print(last_line)
    calendarDates = last_line.replace('# ','')[:-1].split(',')

    m_avaialbility_update=m

    #status switch to Unavailable meaning -potential cancallation
    m_avaialbility_update_toAvailable=ma.masked_where((m_avaialbility_update!=1),  m_avaialbility_update)
    m_avaialbility_update_toAvailable=m_avaialbility_update_toAvailable.filled(np.nan)

    ##read price matrix
    m_price=np.genfromtxt(r'..\resultMatrix\price\%s'%(str(file)), delimiter=",", filling_values=99)
    m_price=m_price[:,1:]

    ##money-wise manipulation
    m_toAvaialble_money=(m_avaialbility_update_toAvailable*m_price).copy()


    ###slice to policies
    Lst_policyVisits_cols={}
    for pk,[[P1_scrapeDates_start,P1_scrapeDates_end],[P1_cal_start,P1_cal_end]] in \
            {'p1': [['2020-03-16','2020-04-14'],['2020-03-16','2020-04-14']],
              'p2': [['2020-03-30', '2020-05-31'],['2020-04-15','2020-05-31']],
              'p3': [['2020-05-01', '2020-06-15'],['2020-06-01','2020-06-15']],
              'p4': [['2020-06-01', '2020-07-15'],['2020-06-16','2020-07-15']],
              'p5': [['2020-06-15', '2020-07-31'],['2020-07-16','2020-07-31']]
              }.items():
        P1_scrapeDates_start_index= scrapeDates.index(P1_scrapeDates_start)
        P1_scrapeDates_end_index= scrapeDates.index(P1_scrapeDates_end)

        P1_cal_start_index= calendarDates.index(P1_cal_start)
        P1_cal_end_index= calendarDates.index(P1_cal_end)

        ## extrac policy cancellations
        extract_p=m_avaialbility_update_toAvailable[P1_cal_start_index:P1_cal_end_index+1, P1_scrapeDates_start_index:P1_scrapeDates_end_index+1]

        lst=''
        if len(np.unique(extract_p[~np.isnan(extract_p)]))>0:

            where_are_NaNs = np.isnan(extract_p)
            extract_p[where_are_NaNs] = 0
            extract_p_ScrapeDateSum=extract_p.sum(axis=0)

            lst=file.split('.')[0].split('_')[0]
            df_extract_p_ScrapeDateSum=pd.DataFrame(extract_p_ScrapeDateSum,index=pd.date_range(P1_scrapeDates_start,P1_scrapeDates_end),columns=[lst])
            dfs_toAvailable[pk].append(df_extract_p_ScrapeDateSum)

            # ##How many dates were updated together as visits
            df_extract_p=pd.DataFrame(extract_p,index=pd.date_range(P1_cal_start,P1_cal_end),columns=pd.date_range(P1_scrapeDates_start,P1_scrapeDates_end))
            m_columns=list(extract_p.T)

            lst_policyVisits=[]
            for m_c in m_columns:
                list1 = m_c
                count_dups = [sum(1 for _ in group) for _, group in groupby(list1)]
                ind = [x[0] for x in groupby(list1)]
                ##find visits
                for e in list(zip(ind, count_dups)):
                    if e[0]==1:
                        lst_policyVisits.append(e[1])
            Lst_policyVisits_cols[pk]=lst_policyVisits
        if lst!='':
            lst_policyVisits_allCols[lst]=Lst_policyVisits_cols

        ## extrac  cancellations money
        extract_p = m_toAvaialble_money[P1_cal_start_index:P1_cal_end_index + 1,
                    P1_scrapeDates_start_index:P1_scrapeDates_end_index + 1]

        lst = ''
        if len(np.unique(extract_p[~np.isnan(extract_p)])) > 0:
            where_are_NaNs = np.isnan(extract_p)
            extract_p[where_are_NaNs] = 0
            extract_p_ScrapeDateSum = extract_p.sum(axis=0)

            lst = file.split('.')[0].split('_')[0]
            df_extract_p_ScrapeDateSum = pd.DataFrame(extract_p_ScrapeDateSum,
                                                      index=pd.date_range(P1_scrapeDates_start, P1_scrapeDates_end),
                                                      columns=[lst])
            dfs_toAvaialble_money[pk].append(df_extract_p_ScrapeDateSum)

    cnt+=1


for pk in ['p1', 'p2', 'p3', 'p4', 'p5']:
    df_merged = reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True,
                                                    how='outer'), dfs_toAvailable[pk])
    df_merged.to_csv(
            r'..\results\matrix_toAvailability_%s.csv' % (
                pk))


###output money
for pk in ['p1', 'p2', 'p3', 'p4', 'p5']:
    df_merged = reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True,
                                                    how='outer'), dfs_toAvaialble_money[pk])
    df_merged.to_csv(
            r'..\results\matrix_toAvailability_money_%s.csv' % (
                pk))


###output visit
import pickle

with open(r"..\results\visits_collected.pickle", "wb") as output_file:
    pickle.dump(lst_policyVisits_allCols, output_file)

