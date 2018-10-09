#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct  5 14:56:50 2018

@author: natalie
"""

import pandas as pd
import numpy as np

df=pd.read_csv("/home/natalie/Documents/Manifold/df_test.csv")
from iter_apriori import freq
from iter_apriori import order_count
from iter_apriori import get_item_pairs
from iter_apriori import merge_item_stats
from iter_apriori import merge_item_name
from itertools import combinations, groupby
from collections import Counter


df=df.iloc[:,[0,1,3,4,5,6,7,8]]
df.columns=['Date', 'Duration', 'Src_IP', 'Src_pt', 'Dst_IP', 'Dst_pt','Packets', 'Bytes']
#add an date column that is rounded to nearest hour, so we can use this as a timestep to see how frequently IP pairs occur in each timestep
df['Date']=pd.to_datetime(df['Date'], format="%Y-%m-%d %H:%M:%S.%f", errors = 'coerce')
df['date_hr']=pd.Series(df['Date']).dt.round("H")

df['pairs']=list(zip(df.Src_IP, df.Dst_IP))
df['pairs']=df['pairs'].apply(sorted)
df['pairs2']=tuple(df['pairs'])

data=df[['Date', 'Src_IP', 'Dst_IP']]
#melt 
data2=data
data2=pd.melt(data2, id_vars=['Date'])

#necessary for assocation mining
data2=data2[['Date', 'value']]
data2.columns=['Date', 'IP']
data_series = data2.set_index('Date')['IP'].rename('IP')


#group the dataset by unique pairs, then count how frequently each pair occurs, and also get the average normalized latency time for that pair. The latency times vary because the servers they are assigned are random.
pairs_count=(df.groupby('pairs2').agg({'Date':'count', 'Duration': 'sum', 'Packets':'sum'}).reset_index())

pairs_count.columns=['pairs','frequency', 'total_duration', 'total_packets']
#pairs_count['norm_latency']=(pairs_count['total_duration']/pairs_count['total_packets'].sum())*100 #sum of all duration time divided by sum of all packets transfered for that pair


per_90=np.percentile(pairs_count['frequency'], [90])

min_support=per_90/len(df)
min_support=min_support[0]

order_item=data_series
item_stats             = freq(data_series).to_frame("freq")
item_stats['support']  = item_stats['freq'] / order_count(data_series) * 100

 # Filter from order_item items below min support 
qualifying_items       = item_stats[item_stats['support'] >= min_support].index #only one entry per item b/c getting freq of that item
order_item             = order_item[order_item.isin(qualifying_items)] #the series of each transaction can have repeated IPs

order_size             = freq(order_item.index)
qualifying_orders      = order_size[order_size >= 2].index
order_item             = order_item[order_item.index.isin(qualifying_orders)]

# Recalculate item frequency and support
item_stats2             = freq(order_item).to_frame("freq")
item_stats2['support']  = item_stats2['freq'] / order_count(order_item) * 100


    # Get item pairs generator
item_pair_gen          = get_item_pairs(order_item)


item_pairs              = freq(item_pair_gen).to_frame("freqAB")
item_pairs['supportAB'] = item_pairs['freqAB'] / len(qualifying_orders) * 100

for order_id, order_object in groupby(order_item, lambda x: x[0]):
    item_list = [item[1] for item in order_object]             
    for item_pair in combinations(item_list, 2):
        yield item_pair
        
def get_item_pairs(order_item):
    order_item = order_item.reset_index().as_matrix()
    for order_id, order_object in groupby(order_item, lambda x: x[0]):
        item_list = [item[1] for item in order_object]
              
        for item_pair in combinations(item_list, 2):
            yield item_pair
            
            
def get_item_pairs(order_item):
    order_item = order_item.reset_index().as_matrix()
    for order_id, order_object in groupby(order_item, lambda x: x[0]):
        item_list = [item[1] for item in order_object]
              
        for item_pair in combinations(item_list, 2):
            yield item_pair