#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep 30 12:01:52 2018

@author: natalie
"""


#import boto3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from mlxtend.preprocessing import TransactionEncoder
import random
from mlxtend.frequent_patterns import apriori
import pyfpgrowth
#from io import BytesIO

#load data
#df=pd.read_csv('/home/natalie/Documents/Manifold/df_test.csv')
#client = boto3.client('s3')
#obj = client.get_object(Bucket='manifolddata', Key='week1.csv')
#df = pd.read_csv(BytesIO(obj['Body'].read()), low_memory=False)


#df=df.iloc[:,[0,1,3,4,5,6,7,8]]
#df.columns=['Date', 'Duration', 'Src_IP', 'Src_pt', 'Dst_IP', 'Dst_pt','Packets', 'Bytes']
#add an date column that is rounded to nearest hour, so we can use this as a timestep to see how frequently IP pairs occur in each timestep

df=pd.read_csv('/home/natalie/Documents/Manifold/df_test.csv')
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
#patterns40 = pyfpgrowth.find_frequent_patterns(data_l, per_40) 
patterns80 = pyfpgrowth.find_frequent_patterns(data_l, per_80) #generates less patterns b/c the min threshold is set higher

confidence=0.7 #this means the rule is likely to be true 70% of the time, it is a high threshold, used for testing
#rules40 = pyfpgrowth.generate_association_rules(patterns40, confidence)
rules80 = pyfpgrowth.generate_association_rules(patterns80, confidence)
#the input 'rules' is the result of the pyfgrowth function run above. You must have specified the thresholds you want to use
#the orig_df is the original data, with 
#the apps_server is the number of apps that can fit on a server
def format_rules(rules, orig_df,apps_server): 
    #Convert the Dictionary format into a dataframe
    rules_df=pd.DataFrame(list(rules.items()), columns=['IP_A', 'confidence'])
    rules_df['confidence']=rules_df['confidence'].astype(str)

    rules_df['IP_B'], rules_df['B'] = rules_df['confidence'].str.split(', ', 1).str
    rules_df=rules_df.drop('confidence', axis=1)
    rules_df.IP_B=rules_df.IP_B.astype(str)
    rules_df.IP_A=rules_df.IP_A.astype(str)
    rules_df.columns=['IP_A', 'IP_B', 'confidence']
    rules_df[['IP_A', 'IP_B']]=rules_df[['IP_A', 'IP_B']].replace({',':''}, regex=True)

    rules_df['IP_A'] = rules_df['IP_A'].map(lambda x: x.lstrip('(').rstrip(')'))
    rules_df['IP_B'] = rules_df['IP_B'].map(lambda x: x.lstrip('((').rstrip(')'))
    rules_df['confidence'] = rules_df['confidence'].map(lambda x: x.rstrip(')'))
    rules_df['IP_A'] = rules_df['IP_A'].map(lambda x: x.lstrip("'").rstrip("'"))
    rules_df['IP_B'] = rules_df['IP_B'].map(lambda x: x.lstrip("'").rstrip("'"))
    
    #add back in a pairs column, this is important because the order of IP_A and IP_B does not matter, 
    rules_df['pairs']=list(zip(rules_df.IP_A, rules_df.IP_B))
    rules_df['pairs']=rules_df['pairs'].apply(sorted)
    rules_df['pairs2']=tuple(rules_df['pairs'])
    
    pairs_count=(orig_df.groupby('pairs2').agg({'Date':'count', 'norm_latency': 'mean', 'Duration': 'sum', 'Packets':'sum'}).reset_index())
    pairs_count.columns=['pairs','frequency', 'avg_norm_latency', 'total_duration', 'total_packets']
    pairs_count['norm_latency']=(pairs_count['total_duration']/pairs_count['total_packets'].sum())*100 #sum of all duration time divided by sum of all packets transfered for that pair
    
    rules_df=rules_df.merge(pairs_count, left_on='pairs2', right_on='pairs')
    rules_df=rules_df.drop('pairs_y', axis=1)
    rules_df=rules_df.rename(columns={'pairs_x':'pairs'})
    rules_df['latency_rank']=rules_df['frequency']*rules_df['norm_latency']
    
    rules_df=rules_df.sort_values(by='latency_rank', ascending=False)
    
    #1. Start by filling in the servers on the pairs until the server is full
    import math
    pairs_server=apps_server/2 #pairs of IP addresses that can fit on each server

#how many servers do we need for our rules, which are in pairs?
    servers_rule=math.ceil(len(rules_df)/pairs_server)
    servers_rule_list=list(range(0,servers_rule+1))
    servers_rule_list=np.repeat(servers_rule_list,pairs_server)

#remove the extra items
    servers_rule_list=servers_rule_list[0:len(rules_df)]
    
    # add a pair_ID column so we can keep track of how frequently IP addresses repeat in different pairs
    rules_df['pair_ID']=range(0, len(rules_df))
    
    #start by assigning the most important pair to a server
    rules_df['server_A']=None
    rules_df['server_B']=None
    rules_df.loc[rules_df['pair_ID']==0, 'server_A'] = 0
    rules_df.loc[rules_df['pair_ID']==0, 'server_B'] = 0
    
    #assign these servers to the pairs in our rules dataframe. Again this is stupid as we are not considering individual IPs that may repeat in different pairs. but it's a start
    rules_df['server']=servers_rule_list
    #export rules_df
    rules_df.to_csv('rules_df.csv')
    rules_df=pd.DataFrame(rules_df)
    return(rules_df)


#rules_df_40per_70con_all=format_rules(rules40, df, 20)
rules_df_80per_70con=format_rules(rules80, df, 20)

#test that the rule lenght is as expected
print(len(rules_df_80per_70con))
#head=rules_df_80per_70con.head()
print(rules_df_80per_70con)