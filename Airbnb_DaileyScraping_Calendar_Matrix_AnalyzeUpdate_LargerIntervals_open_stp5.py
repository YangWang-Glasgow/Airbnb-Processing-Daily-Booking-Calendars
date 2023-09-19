import pandas as pd
import glob,os
import numpy as np

import datetime
from functools import reduce

import numpy.ma as ma
from itertools import groupby


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


##This script mainly follow the same logic as collectin daily udpates
##set up larger intervals by tuning the date_range viarable
##refer to paper section 4.4

# filePath=r'U:\Projects\AirBnB-Analysis\calendars\airbnb_edinburgh_calendars_start_to_2022-07-15\resultMatrx\availability'
filePath=r'..\resultMatrx\availability'

os.chdir(filePath)
files=glob.glob('*.txt')
# print(files)
dfs_toUnavaialble= {'p1': [],
              'p2': [],
              'p3': [],
              'p4': [],
              'p5': []
              }

dfs_toUnavaialble_money= {'p1': [],
              'p2': [],
              'p3': [],
              'p4': [],
              'p5': []
              }

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
    lst=file.split('.')[0].split('_')[0]
    m=np.genfromtxt(file, delimiter=",", filling_values=99)


    ###read scrape days - columns
    f = open(file)
    header = f.readline()
    scrapeDates = header.replace('# ','')[:-1].split(',')


    ###read calendar days -rows
    with open(file, "r") as fl:
        last_line = fl.readlines()[-1]
    calendarDates = last_line.replace('# ','')[:-1].split(',')


    start_date = "2020-03-04"
    end_date = "2022-07-15"

    ##date_range variable can be set to monthly weekly (monday, tuesday, etc)
    d1 = pd.date_range(start_date, end_date, freq="W-MON")

    d1_str=[d.strftime('%Y-%m-%d') for d in d1]
    df_m_sel=df_m[d1_str]


    m_avaialbility_update = np.diff(df_m_sel.values, axis=1)


    m_avaialbility_update_toUnavailable=ma.masked_where((m_avaialbility_update!=-1),  m_avaialbility_update)
    m_avaialbility_update_toUnavailable=m_avaialbility_update_toUnavailable.filled(np.nan)


    df_m_avaialbility_update_toUnavailable=pd.DataFrame(data=m_avaialbility_update_toUnavailable,  # values
    index = calendarDates,  # 1st column as index
    columns = d1_str[1:])



    m_avaialbility_update_toAvailable=ma.masked_where((m_avaialbility_update!=1),  m_avaialbility_update)
    m_avaialbility_update_toAvailable=m_avaialbility_update_toAvailable.filled(np.nan)


    df_m_avaialbility_update_toAvailable=pd.DataFrame(data=m_avaialbility_update_toAvailable,  # values
    index = calendarDates,  # 1st column as index
    columns = d1_str[1:])


    m_price=np.genfromtxt(r'..\resultMatrx\price\%s'%(str(file)), delimiter=",", filling_values=99)


    df_price=pd.DataFrame(data=m_price,  # values
    index = calendarDates,  # 1st column as index
    columns = scrapeDates)


    df_price_sel=df_price[d1_str]
    m_price=df_price_sel.to_numpy()
    ##prepare for updates
    m_price=m_price[:,1:]
    # ##money wise cancellation
    m_toAvaialble_monoey=m_avaialbility_update_toAvailable*m_price


    df_m_toAvaialble_monoey=pd.DataFrame(data=m_toAvaialble_monoey,  # values
    index = calendarDates,  # 1st column as index
    columns = d1_str[1:])


    ###slice to policies
    Lst_policyVisits_cols={}
    for pk,[[P1_scrapeDates_start,P1_scrapeDates_end],[P1_cal_start,P1_cal_end]] in \
            {'p1': [['2020-03-16','2020-04-14'],['2020-03-16','2020-04-14']],
              'p2': [['2020-03-30', '2020-05-31'],['2020-04-15','2020-05-31']],
              'p3': [['2020-05-01', '2020-06-15'],['2020-06-01','2020-06-15']],
              'p4': [['2020-06-01', '2020-07-15'],['2020-06-16','2020-07-15']],
              'p5': [['2020-06-15', '2020-07-31'],['2020-07-16','2020-07-31']]
              }.items():

        ## extrac policy cancellation
        df_m_avaialbility_update_toAvailable.index=pd.to_datetime(df_m_avaialbility_update_toAvailable.index)
        df_m_avaialbility_update_toAvailable.columns = pd.to_datetime(df_m_avaialbility_update_toAvailable.columns)
        df_extract_p=df_m_avaialbility_update_toAvailable[(df_m_avaialbility_update_toAvailable.index>=pd.to_datetime(P1_cal_start)) & (df_m_avaialbility_update_toAvailable.index<=pd.to_datetime(P1_cal_end))][[c for c in df_m_avaialbility_update_toAvailable.columns if c in pd.date_range(P1_scrapeDates_start,P1_scrapeDates_end)]]


        df_extract_p_ScrapeDateSum=pd.DataFrame(df_extract_p.sum(axis=0),index=df_extract_p.columns.tolist(),columns=[lst])

        dfs_toAvailable[pk].append(df_extract_p_ScrapeDateSum)


        lst_policyVisits={}
        for m_c in [c.strftime('%Y-%m-%d') for c in df_extract_p.columns]:
            list1 = df_extract_p[m_c].tolist()
            count_dups = [sum(1 for _ in group) for _, group in groupby(list1)]
            ind = [x[0] for x in groupby(list1)]

            ##find visits
            visits=[]
            for e in list(zip(ind, count_dups)):
                if e[0]==1:
                    visits.append(e[1])
            lst_policyVisits[m_c]=visits
        Lst_policyVisits_cols[pk]=lst_policyVisits


    lst_policyVisits_allCols[lst]=Lst_policyVisits_cols



for pk in ['p1','p2','p3','p4','p5']:
    df_merged = reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True,
                                                        how='outer'), dfs_toUnavaialble[pk])

    print(df_merged)
    df_merged.to_csv(r'..\resultMatrx\results\weekly\matrix_toUnavailability_%s.csv'%(pk))

for pk in ['p1', 'p2', 'p3', 'p4', 'p5']:
    df_merged = reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True,
                                                    how='outer'), dfs_toAvailable[pk])

    print(df_merged)
    df_merged.to_csv(
            r'..\resultMatrx\results\weekly\matrix_toAvailability_%s.csv' % (
                pk))

###output money
for pk in ['p1', 'p2', 'p3', 'p4', 'p5']:
    df_merged = reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True,
                                                    how='outer'), dfs_toAvaialble_money[pk])

    print(df_merged)
    df_merged.to_csv(
            r'..\resultMatrx\results\weekly\matrix_cancellationMoney_%s.csv' % (
                pk))



###output visit
import pickle

with open(r"..\resultMatrx\results\weekly\visits_collected.pickle", "wb") as output_file:
    pickle.dump(lst_policyVisits_allCols, output_file)

