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


###summarizing the indicators into a summary table for plot
##this produce the data for section 4.2

dfs=[]
dfs_listings=[]
for pk in ['p1','p2','p3','p4','p5']:
    ##summarize policies
    ##configure the input file e.g.toAvailability, toUnAvailability, or Money matrix
    df=pd.read_csv(r'..\resultMatrix\matrix_toAvailability_%s.csv'%(pk),index_col=[0])
    print(df.head())
    df.index=pd.to_datetime(df.index)
    print(df.shape)

    ###count listings with updates
    df_listings=pd.DataFrame((df != 0).astype(int).sum(axis=1), columns=['listings'])
    print(df_listings)
    dfs_listings.append(df_listings)

    df_sum=pd.DataFrame(df.sum(axis=1),columns=[pk])
    print(df_sum)
    dfs.append(df_sum)

from functools import reduce

df_merged = reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True,
                                                    how='outer'),     dfs)

print(df_merged)

df_merged = df_merged.reindex(
    pd.date_range("2020-03-04", pd.to_datetime("2020-07-15") ), fill_value=np.nan)

df_merged.to_csv(r'..\resultMatrix\summary\toAvailable.csv')


df_merged = reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True,
                                                    how='outer'),     dfs_listings)

print(df_merged)

df_merged = df_merged.reindex(
    pd.date_range("2020-03-04", pd.to_datetime("2020-07-15") ), fill_value=np.nan)


df_merged.to_csv(r'..\resultMatrix\summary\toAvailable_listings.csv')