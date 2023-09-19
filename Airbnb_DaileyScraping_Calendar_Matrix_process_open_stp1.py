import pandas as pd
import glob,os
import numpy as np

import datetime
from functools import reduce

from pyarrow import csv
import pyarrow.parquet as pq
import pyarrow as pa

pd.set_option('display.expand_frame_repr', False)


##Process the scraped calendars
##to get the update, avaibility and price matrix

##optional - to specify the focused listings
##the paper focuses on entire-home listings
##for privacy reasons, we cannot sure the csv.gz file, but it should be obtainable from the scraping exercises: https://github.com/urbanbigdatacentre/ubdc-airbnb/tree/master/README
##this is the foundation for case study 4.2

listingPath=r'..\data\ListingDetails-241-GlasgowCCounsil.csv.gz'
lst_new=pd.read_csv(listingPath, compression='gzip', dtype={'listing_id': 'Int64'})#,nrows=1000)#,nrows=1000)
lst_edin_et=lst_new[(lst_new['room_type_category']=='entire_home') & (lst_new['room_type_category'].notnull()) & (lst_new['listing_id'].notnull()) ]['listing_id'].unique().tolist()
lst_edin_et=[str(lst) for lst in lst_edin_et]


##specify study period
start_date="2020-03-04"
update_start_date=pd.to_datetime(start_date)+datetime.timedelta(days=1)
end_date="2020-07-31"
dateRange=pd.date_range(start_date, end_date, freq="1d").map(lambda x: x.strftime('%Y-%m-%d'))



def tuneDfDate(indf):
    ##deal with excaptions where more than one record per listings in one day
    indf_gp = indf.groupby('cal_date').agg({'cal_available': np.mean})
    indf_gp.loc[indf_gp['cal_available'] > 0, 'cal_available'] = 1
    indf_gp.loc[indf_gp['cal_available'] == 0, 'cal_available'] = 0
    return (indf_gp.reset_index())

def tuneDfPrice(indf):
    ##deal with excaptions where more than one price per listings in one day
    indf_gp = indf.groupby('cal_date').agg({'cal_native_price': np.mean})
    return (indf_gp.reset_index())

##get activities
from itertools import groupby
import datetime

##The whole scrapes were split into small job chunks
##This allows the calculation of matrix run in parallel threads
##We use parqeust.gzip for fast read/writing
filePath=r'..\data\processed_pqtest'
fileId=1
files_inds=range(fileId,fileId+5)

##threshold that is used to retreive calendar activities
calendarThrs=360

for file_ind in files_inds:
    file=filePath+'\combined_chunk%s_100.parquet.gzip'%file_ind

    table = pq.read_table(file)
    df=table.to_pandas()
    df['listing_id']=df['listing_id'].astype('long').astype('str')

    df['cal_date']=pd.to_datetime(df['cal_date'],format='%Y-%m-%d')
    df['scrapingDate']=pd.to_datetime(df['scrapingDate'],format='%Y-%m-%d')


    for lst in df['listing_id'].unique():
        if (lst in lst_edin_et) :
                dfs = []
                dfs_price = []
                for spr_date in dateRange:

                    df_sel=df[(df['listing_id']==lst) & (df['scrapingDate']==spr_date) & (df['cal_date']>=pd.to_datetime(spr_date)) & (df['cal_date']<=pd.to_datetime(spr_date)+datetime.timedelta(days=calendarThrs)) ]

                    ### transform the availability to numerical values
                    df_sel_availability=df_sel[['listing_id','cal_date','cal_available']].copy()
                    df_sel_availability.loc[df_sel['cal_available'] == 'No', 'cal_available'] = 0
                    df_sel_availability.loc[df_sel_availability['cal_available'] == 'Yes', 'cal_available'] = 1
                    df_sel_availability['cal_available'] = df_sel_availability['cal_available'].astype(int)

                    ##deal with exceptions when more than one records per lising at one day
                    df_sel_availability=tuneDfDate(df_sel_availability)
                    df_sel_availability=df_sel_availability.set_index('cal_date')
                    df_sel_availability.columns=[spr_date]
                    dfs.append(df_sel_availability)

                    ###similar process to price but using means
                    df_sel_price=df_sel[['listing_id','cal_date','cal_native_price']].copy()
                    df_sel_price['cal_native_price'] = df_sel_price['cal_native_price'].astype(int)
                    df_sel_price=tuneDfPrice(df_sel_price)
                    df_sel_price=df_sel_price.set_index('cal_date')
                    df_sel_price.columns=[spr_date]
                    dfs_price.append(df_sel_price)
                ##merge collected availabilities and save as matrix, with header as scrape dates, footer as calendar dates
                df_merged_availability = reduce(lambda left, right: pd.merge(left, right, right_index=True, left_index=True,
                                                                how='outer'), dfs)
                df_merged_availability=df_merged_availability.reindex(pd.date_range(start_date,  pd.to_datetime(end_date)+datetime.timedelta(days=360)), fill_value=np.nan)

                path=r'..\resultMatrx\availability\%s_%s.txt'%(lst,file_ind)
                np.savetxt(path, df_merged_availability.to_numpy(), fmt='%.0f', delimiter=',', newline='\n', header=','.join(pd.date_range(start_date,  pd.to_datetime(end_date), freq="1d").map(lambda x: x.strftime('%Y-%m-%d'))), footer=','.join(pd.date_range(start_date,  pd.to_datetime(end_date)+datetime.timedelta(days=360), freq="1d").map(lambda x: x.strftime('%Y-%m-%d'))), comments='# ')

                ##calcualte updates from availabilty matrix
                m_avaialbility = df_merged_availability.to_numpy()
                m_avaialbility_update = np.diff(m_avaialbility, axis=1)
                ##save as matrix, with header as scrape dates, footer as calendar dates
                path = r'..\resultMatrx\updates\%s_%s.txt' % (lst, file_ind)
                np.savetxt(path, m_avaialbility_update, fmt='%.0f', delimiter=',', newline='\n', header=','.join(pd.date_range(update_start_date,  pd.to_datetime(end_date), freq="1d").map(lambda x: x.strftime('%Y-%m-%d'))), footer=','.join(pd.date_range(start_date,  pd.to_datetime(end_date)+datetime.timedelta(days=360), freq="1d").map(lambda x: x.strftime('%Y-%m-%d'))), comments='# ')

                ##merge collected price and save as matrix, with header as scrape dates, footer as calendar dates
                df_merged_price = reduce(lambda left, right: pd.merge(left, right, right_index=True, left_index=True,
                                                                how='outer'), dfs_price)
                df_merged_price=df_merged_price.reindex(pd.date_range(start_date,  pd.to_datetime(end_date)+datetime.timedelta(days=360)), fill_value=np.nan)

                path = r'..\resultMatrx\price\%s_%s.txt' % (lst, file_ind)

                np.savetxt(path, df_merged_price.to_numpy(), fmt='%.0f', delimiter=',', newline='\n', header=','.join(pd.date_range(start_date,  pd.to_datetime(end_date), freq="1d").map(lambda x: x.strftime('%Y-%m-%d'))), footer=','.join(pd.date_range(start_date,  pd.to_datetime(end_date)+datetime.timedelta(days=360), freq="1d").map(lambda x: x.strftime('%Y-%m-%d'))), comments='# ')
