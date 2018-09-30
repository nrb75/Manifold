#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep 30 08:45:30 2018

@author: natalie
"""

from format_rules import format_rules
from server_association import server_association
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from mlxtend.preprocessing import TransactionEncoder
import random
from mlxtend.frequent_patterns import apriori
import pyfpgrowth

#load data
df=pd.read_csv('/home/natalie/Documents/Manifold/df_test.csv')

#df=df.iloc[:,[0,1,3,4,5,6,7,8]]
#df.columns=['Date', 'Duration', 'Src_IP', 'Src_pt', 'Dst_IP', 'Dst_pt','Packets', 'Bytes']
#add an date column that is rounded to nearest hour, so we can use this as a timestep to see how frequently IP pairs occur in each timestep
df['Date']=pd.to_datetime(df['Date'], format="%Y-%m-%d %H:%M:%S.%f", errors = 'coerce')
df['date_hr']=pd.Series(df['Date']).dt.round("H")

#create a pair column, which is a touple of the src and dst IP, sorted. 
#It does not matter which call came first, we simply want to know which pair occurs most frequently.

df['pairs']=list(zip(df.Src_IP, df.Dst_IP))
df['pairs']=df['pairs'].apply(sorted)
df['pairs2']=tuple(df['pairs'])

def convert_si_to_number(x):
    total_stars = 0
    if 'k' in x:
        if len(x) > 1:
            total_stars = float(x.replace('k', '')) * 1000 # convert k to a thousand
    elif 'M' in x:
        if len(x) > 1:
            total_stars = float(x.replace('M', '')) * 1000000 # convert M to a million
    elif 'B' in x:
        total_stars = float(x.replace('B', '')) * 1000000000 # convert B to a Billion
    else:
        total_stars = int(x) # Less than 1000
    return int(total_stars)


df.Bytes=df.Bytes.astype('str')
test_list=df.Bytes

[i for i, s in enumerate(test_list) if 'M' in s]#show where the M errors are happening

test_list= [convert_si_to_number(x) for x in test_list]
df.Bytes=test_list #bring it back into the dataframe

#create a normalized latency column = duration/packets
df['norm_latency']=df['Duration']/df['Packets']

#group the dataset by unique pairs, then count how frequently each pair occurs, and also get the average normalized latency time for that pair. The latency times vary because the servers they are assigned are random.
pairs_count=(df.groupby('pairs2').agg({'Date':'count', 'norm_latency': 'mean', 'Duration': 'sum', 'Packets':'sum'}).reset_index())

pairs_count.columns=['pairs','frequency', 'avg_norm_latency', 'total_duration', 'total_packets']
pairs_count['norm_latency']=(pairs_count['total_duration']/pairs_count['total_packets'].sum())*100 #sum of all duration time divided by sum of all packets transfered for that pair

#we only want a list of all the individual pairs at each timestamp. Think of this where each timestamp is a 'transaction' and we chose to buy which 2 items (IP addresses)
data_l=list(df['pairs'])

per_40=np.percentile(pairs_count['frequency'], [40])[0]
per_80=np.percentile(pairs_count['frequency'], [80])[0]
patterns40 = pyfpgrowth.find_frequent_patterns(data_l, per_40) 
patterns80 = pyfpgrowth.find_frequent_patterns(data_l, per_80) #generates less patterns b/c the min threshold is set higher

confidence=0.7 #this means the rule is likely to be true 70% of the time, it is a high threshold, used for testing
rules40 = pyfpgrowth.generate_association_rules(patterns40, confidence)
rules80 = pyfpgrowth.generate_association_rules(patterns80, confidence)
#the input 'rules' is the result of the pyfgrowth function run above. You must have specified the thresholds you want to use
#the orig_df is the original d

rules_df_80per_70con=format_rules(rules80, df, 20) #this function was loaded above from the format_rules.py file

server_assignments80, total_latency, total_latency80, avg_latency, avg_latency80 = server_association(rules_df_80per_70con, df, 20) #this function loaded from server_assocation.py file

