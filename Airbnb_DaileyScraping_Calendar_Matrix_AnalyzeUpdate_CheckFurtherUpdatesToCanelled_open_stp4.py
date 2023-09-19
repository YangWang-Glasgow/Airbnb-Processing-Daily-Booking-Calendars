import pandas as pd
import glob,os
import numpy as np

import datetime
from functools import reduce
import pickle

import numpy.ma as ma
from itertools import groupby


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)



##this script has the function to check further updates after a date has been cancelled/booked
##refer to paper secton 4.3

##this script can be adepted to two aspects
##update to either dates being cancelled or booked
##to specify this, change the input files accordingly

##step 1  collect
filePath=r'..\resultMatrx\updates'

os.chdir(filePath)
files=glob.glob('*.txt')
# print(files)

Lst_notUpdated_allcols={}
Lst_updatedOnce_allcols={}
Lst_updatedMultiple_allcols={}
cnt=0
for file in files:
    # print(file)
    lst_name=file.split('.')[0].split('_')[0]

    m=np.genfromtxt(file, delimiter=",", filling_values=99)


    ###read scrape days - columns
    f = open(file)
    header = f.readline()
    scrapeDates = header.replace('# ','')[:-1].split(',')

    ###read calendar days -rows
    with open(file, "r") as fl:
        last_line = fl.readlines()[-1]

    calendarDates = last_line.replace('# ','')[:-1].split(',')

    m_avaialbility_update=m


    ###slice to policies
    Lst_notUpdated_cols={}
    Lst_updatedOnce_cols = {}
    Lst_updatedMultiple_cols = {}

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



        ## extract further updated to policies
        extract_p=m_avaialbility_update[P1_cal_start_index:P1_cal_end_index+1, P1_scrapeDates_start_index:P1_scrapeDates_end_index+1]


        if (1 in np.unique(extract_p)):

            extract_p_calendarDates=[d.strftime('%Y-%m-%d') for d in pd.date_range(P1_cal_start,P1_cal_end)]
            extract_p_scrapeDates = [d.strftime('%Y-%m-%d') for d in pd.date_range(P1_scrapeDates_start,P1_scrapeDates_end)]

            ##focus on noupdated, update once and update multiple times
            calDate_notUpdated={}
            calDate_updatedOnce = {}
            calDate_updatedMultipleTimes={}

            unqiue_scrapeDateWithCancellations=list(set([scrp_date for [cal_date, scrp_date] in np.argwhere(extract_p == 1)]))

            for scrapeDate_index in unqiue_scrapeDateWithCancellations:
                notUpdated_indicator = 0
                updatedOnce_indicator = 0
                updatedMuti_indicator = 0

                notUpdated = []
                updatedOnce = []
                updatedMultipleTimes = []
                for [chk_loc_row, chk_loc_column] in [[sel_row, sel_column] for [sel_row, sel_column] in np.argwhere(extract_p == 1) if sel_column==scrapeDate_index]:

                    extract_checkPeriod=extract_p[chk_loc_row,chk_loc_column:extract_p_scrapeDates.index(extract_p_calendarDates[chk_loc_row])+1]
                    extract_checkPeriod_lst=extract_checkPeriod.tolist()

                    if (extract_checkPeriod_lst.count(1)==1) & (extract_checkPeriod_lst.count(-1)==0) :
                        notUpdated_indicator+=1
                        notUpdated.append([notUpdated_indicator,extract_p_calendarDates[chk_loc_row],len(extract_checkPeriod)])

                    if (extract_checkPeriod_lst.count(1)==1) & (extract_checkPeriod_lst.count(-1)==1):
                        updatedOnce_indicator+=1
                        furtherUpdated_position = extract_checkPeriod_lst.index(-1)
                        updatedOnce.append([updatedOnce_indicator,extract_p_calendarDates[chk_loc_row],furtherUpdated_position,len(extract_checkPeriod)])
                    lastUpdateStatue=np.nan
                    toAvilableTimes=0
                    toUnavilableTimes=0
                    if (extract_checkPeriod_lst.count(1)>1) | (extract_checkPeriod_lst.count(-1)>1):
                        updatedMuti_indicator+=1
                        lastUpdateStatue=extract_checkPeriod[~np.isnan(extract_checkPeriod)][np.max(np.nonzero(extract_checkPeriod[~np.isnan(extract_checkPeriod)]))]
                        toAvilableTimes=extract_checkPeriod_lst.count(1)
                        toUnavilableTimes = extract_checkPeriod_lst.count(-1)
                        updatedMultipleTimes.append([updatedMuti_indicator,lastUpdateStatue,toAvilableTimes,toUnavilableTimes,len(extract_checkPeriod)])
                calDate_notUpdated[extract_p_scrapeDates[chk_loc_column]]=notUpdated
                calDate_updatedOnce[extract_p_scrapeDates[chk_loc_column]]=updatedOnce
                calDate_updatedMultipleTimes[extract_p_scrapeDates[chk_loc_column]]=updatedMultipleTimes

            Lst_notUpdated_cols[pk]=calDate_notUpdated
            Lst_updatedOnce_cols[pk]=calDate_updatedOnce
            Lst_updatedMultiple_cols[pk]=calDate_updatedMultipleTimes

    Lst_notUpdated_allcols[lst_name]=Lst_notUpdated_cols
    Lst_updatedOnce_allcols[lst_name] = Lst_updatedOnce_cols
    Lst_updatedMultiple_allcols[lst_name] = Lst_updatedMultiple_cols


with open(r"..\resultMatrx\summary\furtherUpdatesAndCancellation\UpdateToAvailable1\noUpdates.pickle", "wb") as output_file:
    pickle.dump(Lst_notUpdated_allcols, output_file)



with open(r"..\resultMatrx\summary\furtherUpdatesAndCancellation\UpdateToAvailable1\updatesOnce.pickle", "wb") as output_file:
    pickle.dump(Lst_updatedOnce_allcols, output_file)



with open(r"..\resultMatrx\summary\furtherUpdatesAndCancellation\UpdateToAvailable1\updatesMultiple.pickle", "wb") as output_file:
    pickle.dump(Lst_updatedMultiple_allcols, output_file)




##step 2
#summarize the total by accessing the output of step1
with open(
            r"..\resultMatrx\summary\furtherUpdatesAndCancellation\UpdateToAvailable1\noUpdates.pickle",
            "rb") as input_file:
        e_noUpdates = pickle.load(input_file)



with open(
            r"..\resultMatrx\summary\furtherUpdatesAndCancellation\UpdateToAvailable1\updatesOnce.pickle",
            "rb") as input_file:
        e_updatesOnce = pickle.load(input_file)





with open(
            r"..\resultMatrx\summaryfurtherUpdatesAndCancellation\UpdateToAvailable1\updatesMultiple.pickle",
            "rb") as input_file:
        e_updatesMultiple = pickle.load(input_file)



for pk,[[P1_scrapeDates_start,P1_scrapeDates_end],[P1_cal_start,P1_cal_end]] in \
        {'p1': [['2020-03-16','2020-04-14'],['2020-03-16','2020-04-14']],
         'p2': [['2020-03-30', '2020-05-31'],['2020-04-15','2020-05-31']],
         'p3': [['2020-05-01', '2020-06-15'],['2020-06-01','2020-06-15']],
         'p4': [['2020-06-01', '2020-07-15'],['2020-06-16','2020-07-15']],
         'p5': [['2020-06-15', '2020-07-31'],['2020-07-16','2020-07-31']]
         }.items():

    lst_noUpdate_total={}
    lst_noUpdate_length={}
    for lst,cols in e_noUpdates.items():
        noUpdate_total = {}
        noUpdate_length={}
        if pk in cols.keys():
            col_pk=cols[pk]
            if len(col_pk)>0:
                for col_pk_k, col_pk_v in col_pk.items():
                    if len(col_pk_v):
                        noUpdate_total[col_pk_k]=col_pk_v[-1:][0][:-1][0]
                        noUpdate_length[col_pk_k] = [c[-1:] for c in col_pk_v]
        lst_noUpdate_total[lst]=noUpdate_total
        lst_noUpdate_length[lst]=noUpdate_length


    lst_UpdateOnce_total={}
    lst_UpdateOnce_length={}
    for lst,cols in e_updatesOnce.items():
        UpdateOnce_total = {}
        UpdateOnce_length={}
        if pk in cols.keys():
            col_pk=cols[pk]
            if len(col_pk)>0:
                for col_pk_k, col_pk_v in col_pk.items():
                    if len(col_pk_v):
                        UpdateOnce_total[col_pk_k]=col_pk_v[-1:][0][:-1][0]
                        UpdateOnce_length[col_pk_k] = [c[-1:][0] for c in col_pk_v]
        lst_UpdateOnce_total[lst]=UpdateOnce_total

        lst_UpdateOnce_length[lst]=UpdateOnce_length


    lst_UpdateMultiple_total={}
    lst_UpdateMultiple_length={}
    for lst,cols in e_updatesMultiple.items():
        UpdateMultiple_total = {}
        UpdateMultiple_length={}
        if pk in cols.keys():
            col_pk=cols[pk]
            if len(col_pk)>0:
                for col_pk_k, col_pk_v in col_pk.items():
                    if len(col_pk_v):
                        UpdateMultiple_total[col_pk_k]=col_pk_v[-1:][0][:-1][0]
                        UpdateMultiple_length[col_pk_k] = [c[-1:][0] for c in col_pk_v]
        lst_UpdateMultiple_total[lst]=UpdateMultiple_total

        UpdateMultiple_length[lst]=UpdateMultiple_length

    dfs=[]
    for k,v in lst_UpdateMultiple_total.items():

        if len(v):
            df=pd.DataFrame(v.values(), index=v.keys(),columns=[k])
            dfs.append(df)

    df_merged_lst_UpdateMultiple_total = reduce(lambda left, right: pd.merge(left, right, right_index=True,left_index=True,
                                                    how='outer'), dfs)

    print(df_merged_lst_UpdateMultiple_total.index)
    print(df_merged_lst_UpdateMultiple_total.shape)

    df_merged_lst_UpdateMultiple_total.to_csv(r'..\resultMatrx\summary\furtherUpdatesAndCancellation\UpdateToAvailable1\summary\UpdateMultiple_total_%s.csv'%pk)

    dfs=[]
    for k,v in lst_UpdateOnce_total.items():

        if len(v):
            df=pd.DataFrame(v.values(), index=v.keys(),columns=[k])

            dfs.append(df)

    df_merged_lst_UpdateOnce_total = reduce(lambda left, right: pd.merge(left, right, right_index=True,left_index=True,
                                                    how='outer'), dfs)

    print(df_merged_lst_UpdateOnce_total.index)
    print(df_merged_lst_UpdateOnce_total.shape)
    df_merged_lst_UpdateOnce_total.to_csv(r'..\resultMatrx\summary\furtherUpdatesAndCancellation\UpdateToAvailable1\summary\UpdateOnce_total_%s.csv'%pk)


    dfs = []
    for k, v in lst_noUpdate_total.items():

        if len(v):
            df = pd.DataFrame(v.values(), index=v.keys(), columns=[k])

            dfs.append(df)

    df_merged_lst_noUpdate_total = reduce(lambda left, right: pd.merge(left, right, right_index=True, left_index=True,
                                                                         how='outer'), dfs)

    print(df_merged_lst_noUpdate_total.index)
    print(df_merged_lst_noUpdate_total.shape)
    df_merged_lst_noUpdate_total.to_csv(r'..\resultMatrx\summary\furtherUpdatesAndCancellation\UpdateToAvailable1\summary\noUpdate_total_%s.csv'%pk)
