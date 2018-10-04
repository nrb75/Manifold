#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  2 18:02:38 2018

@author: natalie
"""


import pandas as pd
import numpy as np
from iter_apriori import freq
from iter_apriori import order_count
from iter_apriori import get_item_pairs
from iter_apriori import merge_item_stats
from iter_apriori import merge_item_name
from apriori_rules import association_rules
from Server_Assign_apriori import server_association_apriori
from format_rules_apriori import format_rules_apriori

#df=pd.read_csv('/home/natalie/Documents/Manifold/df_test.csv')

#percentile as a whole number ex 90 = 90th percentile

def hourly_step_test_apriori_output(df_train, df_test, percentile, apps_server):
    df_train['hour']=None
    df_train['hour']=pd.DatetimeIndex(df_train['Date']).hour
    df_test['hour']=None
    df_test['hour']=pd.DatetimeIndex(df_test['Date']).hour
    
    data=df_train[['hour', 'Src_IP', 'Dst_IP']]
    #melt 
    data_series=data
    data_series=pd.melt(data_series, id_vars=['hour'])

    #data_series['hour']=None
    #data_series['hour']=pd.DatetimeIndex(data_series['Date']).hour
    #data_series=data_series.drop('variable', axis=1)
    data_series=data_series[['hour', 'value']]
    data_series.columns=['hour', 'IP']
    data_series = data_series.set_index('hour')['IP'].rename('IP')
    
    data_groups=[]
    for i in range(0, data_series.index.nunique()):
        data=data_series[data_series.index==i]
        data_groups.append(data)
        
    pair_groups=[]
    for i in range(0, df_train['hour'].nunique()):
        data=df_train[df_train['hour']==i]
        pair_groups.append(data)
    
    pairs_list=[]
    patterns_list=[]
    rules_list=[]
    hours=[]

    for i ,j in zip(data_groups, pair_groups):    
        pairs_count=(j.groupby('pairs2').agg({'Date':'count', 'norm_latency': 'mean', 'Duration': 'sum', 'Packets':'sum'}).reset_index())
        pairs_count.columns=['pairs','frequency', 'avg_norm_latency', 'total_duration', 'total_packets']
        pairs_count['norm_latency']=(pairs_count['total_duration']/pairs_count['total_packets'].sum())*100 #sum of all duration time divided by sum of all packets transfered for that pair
        pairs_list.append(pairs_count)
        per_n=(pairs_count['frequency'].quantile(percentile))
        min_support=per_n/len(df_train) #wants this as a percentage
        hour_group=i.index[0]
        hours.append(hour_group)
        #data_series=i.drop('hour', axis=1)
        rules = association_rules(i, min_support)  
        rules_list.append(rules)
    
    #format the rules, bring back in the other info on latency rank
    while {} in rules_list:
        rules_list.remove({})#lremove empty items if they have less than 24 hours
    formated_rules=[]

    for i in rules_list:
        formatrule=format_rules_apriori(i, df_train, apps_server)
        formated_rules.append(formatrule)

    
    #assign IPs to the servers at each hour
    server_df_list=[]
    server_assign_list=[]
    total_latency_list=[]
    total_latency_model_list=[]
    avg_latency_list=[]
    avg_latency_model_list=[]

#now we use the training model on the unseen test data
    data_groups_test=[]
    for i in range(0,df_test['hour'].nunique()):
        data=df_test[df_test['hour']==i]
        data_groups_test.append(data)
    
    
    for i, j in zip(formated_rules, data_groups_test) :
        server_df, server_assignments, total_latency, total_latency_model, avg_latency, avg_latency_model = server_association_apriori(i, j, apps_server) #this function loaded fr
        server_df_list.append(server_df)
        server_assign_list.append(server_assignments)
        total_latency_list.append(total_latency)
        total_latency_model_list.append(total_latency_model)
        avg_latency_list.append(avg_latency)
        avg_latency_model_list.append(avg_latency_model)


#bring together all the durations for the actual data and the model
    hours=range(0, df_train['hour'].nunique())
    model_output=pd.DataFrame({'hours':hours,'total_latency_list': total_latency_list, 'total_latency_model_list': total_latency_model_list, 'avg_latency_list': avg_latency_list, 'avg_latency_model_list': avg_latency_model_list})
    model_output.columns=['hours', 'total_latency', 'total_latency_model', 'avg_latency', 'avg_latency_model']
    model_output['avg_latency_per_reduction']=((model_output['avg_latency']-model_output['avg_latency_model'])/model_output['avg_latency'])*100

    #return(formated_rules)
    return(server_df_list, server_assign_list, model_output , model_output['total_latency'].sum(), model_output['total_latency_model'].sum(), model_output['avg_latency'].mean(), model_output['avg_latency_model'].mean())  
  