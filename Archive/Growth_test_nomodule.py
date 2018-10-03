#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep 29 16:23:33 2018

@author: natalie
"""

import boto3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from mlxtend.preprocessing import TransactionEncoder
import random
from mlxtend.frequent_patterns import apriori
import pyfpgrowth
from io import BytesIO

#load data
#df=pd.read_csv('/home/natalie/Documents/Manifold/df_test.csv')
client = boto3.client('s3')
obj = client.get_object(Bucket='manifolddata', Key='week1.csv')
df = pd.read_csv(BytesIO(obj['Body'].read()), low_memory=False)

df=df.iloc[:,[0,1,3,4,5,6,7,8]]
df.columns=['Date', 'Duration', 'Src_IP', 'Src_pt', 'Dst_IP', 'Dst_pt','Packets', 'Bytes']
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
#patterns40 = pyfpgrowth.find_frequent_patterns(data_l, per_40) 
patterns80 = pyfpgrowth.find_frequent_patterns(data_l, per_80) #generates less patterns b/c the min threshold is set higher

confidence=0.7 #this means the rule is likely to be true 70% of the time, it is a high threshold, used for testing
#rules40 = pyfpgrowth.generate_association_rules(patterns40, confidence)
rules80 = pyfpgrowth.generate_association_rules(patterns80, confidence)
#the input 'rules' is the result of the pyfgrowth function run above. You must have specified the thresholds you want to use
#the orig_df is the original data, with 
#the apps_server is the number of apps that can fit on a server

apps_server=20

    #Convert the Dictionary format into a dataframe
rules_df=pd.DataFrame(list(rules80.items()), columns=['IP_A', 'confidence'])
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
    
pairs_count=(df.groupby('pairs2').agg({'Date':'count', 'norm_latency': 'mean', 'Duration': 'sum', 'Packets':'sum'}).reset_index())
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
#rules_df.to_csv('rules_df.csv')


#rules_df_40per_70con=format_rules(rules40, df, 20)
rules_df_80per_70con=rules_df

#test that the rule lenght is as expected


from collections import defaultdict
serverlist=[]
server={}
ips={}
apps_server=20

serverid=0
for i in range(0,len(rules_df)):
    if len(server)==apps_server:
        serverlist.append(server)
            #server={}
        server = defaultdict(list)
        serverid=serverid+1 #change the serverid when the previous one is full
    #if IP_B is in this server, it is a duplicate, so we only want to add in the IP_A which has not been added to the server yet
    if (rules_df['IP_B'][i] in server) or rules_df['IP_B'][i] in ips and (len(server)<=(apps_server-1)) and (rules_df['IP_A'][i] not in ips):
        server[rules_df['IP_A'][i]]=serverid
        ips[rules_df['IP_A'][i]]=1
    #if IP_B is not in the server, and the server has room for 2 more, and it's matching IP_A is also not in the ip list we know IP_A hasn't been added yet.
    #Thus, we need to add both the IP_A and IP_B in this row to this server and the ip list.
    if (rules_df['IP_B'][i] not in server) and len(server)<=(apps_server-2) and (rules_df['IP_A'][i] not in ips) and (rules_df['IP_B'][i] not in ips):
        server[rules_df['IP_A'][i]]=serverid
        server[rules_df['IP_B'][i]]=serverid
        ips[rules_df['IP_A'][i]]=1
        ips[rules_df['IP_B'][i]]=1                                                                                                                                                                                                                          
   ##if IP_B is not in the server, and the server has room for only 1 more, and it's matching IP_A is also not in the ip list we know IP_A hasn't been added yet.
    #we need to create a new server, and add both the IP_A and IP_B in this row to this new server and the ip list.
    if (rules_df['IP_B'][i] not in server) and len(server)==(apps_server-1) and (rules_df['IP_A'][i] not in ips) and (rules_df['IP_B'][i] not in ips): #if there is not enough room for the pair, we need to start a new server even if it is not full
        serverlist.append(server)
        server={}
        serverid=serverid+1
        server[rules_df['IP_A'][i]]=serverid
        server[rules_df['IP_B'][i]]=serverid
        ips[rules_df['IP_A'][i]]=1
        ips[rules_df['IP_B'][i]]=1
    if server not in serverlist:
        serverlist.append(server)
     #if last server is not full, we still want to append it to the serverlist           
        

server_df=pd.DataFrame.from_records(serverlist) 
server_dft=server_df.transpose()
server_dft['serverid']=server_dft.min(axis=1)
server_dft['IP']=server_dft.index
server_dft=server_dft[['IP', 'serverid']]
server_rules=server_dft #something is wrong with server_rules, the serverids do not match jupyter

#merge in the serverid
df_servers=df.merge(server_rules, left_on='Src_IP', right_on='IP', how='left')
df_servers=df_servers.rename(columns={'serverid': 'Src_Server'})
df_servers=df_servers.merge(server_rules, left_on='Dst_IP', right_on='IP', how='left')
df_servers=df_servers.rename(columns={'serverid': 'Dst_Server'})
df_servers=df_servers.drop(['IP_x', 'IP_y'], axis=1)

df_servers['duration_pred']=df_servers['Duration']
df_servers.loc[df_servers['Src_Server']==df_servers['Dst_Server'], 'duration_pred']=0

 
server_assignments80=server_rules
total_latency=df_servers['Duration'].sum()
total_latency80=df_servers['duration_pred'].sum()
avg_latency=df_servers['Duration'].mean()
avg_latency80 = df_servers['duration_pred'].mean()

print(rules_df[['IP_A', 'IP_B', 'latency_rank']].head(15)) #this matches jupyter
print('len of rules_df_80per_70con is ' + str(len(rules_df_80per_70con)))#should get same length as the rules 351
print('len of server rules is ' + str(len(server_rules))) #should get same 351
print('len of df_servers ' + str(len(df_servers)))#should be dataframe length

print(server_rules.head(20))

print(df_servers.head(20))

print('total latency is ' + str(total_latency))
print('pred total latency is ' + str(total_latency80))

#server_assignments40, total_latency40 = server_association(rules_df_40per_70con, 20)