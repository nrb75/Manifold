#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  2 18:02:38 2018

@author: natalie
"""


import pandas as pd
import pyfpgrowth
from Server_Assign import server_association
from format_rules import format_rules

#this function assigns servers to apps that are important and have rules created for them.
#rules were created with frequent pattern algorithm, then ranked.


def hourly_step_output(df, percentile, confidence, apps_server):
    df['hour']=None
    df['hour']=pd.DatetimeIndex(df['Date']).hour

    data_groups=[]

    for i in range(0, df['hour'].nunique()):
        data=df[df['hour']==i]
        data_groups.append(data)
    
    pairs_list=[]
    patterns_list=[]
    rules_list=[]

    for i in data_groups:    
        data_l=list(i['pairs'])
        pairs_count=(i.groupby('pairs2').agg({'Date':'count', 'norm_latency': 'mean', 'Duration': 'sum', 'Packets':'sum'}).reset_index())
        pairs_count.columns=['pairs','frequency', 'avg_norm_latency', 'total_duration', 'total_packets']
        pairs_count['norm_latency']=(pairs_count['total_duration']/pairs_count['total_packets'].sum())*100 #sum of all duration time divided by sum of all packets transfered for that pair
        pairs_list.append(pairs_count)
        per_n=(pairs_count['frequency'].quantile(percentile))
        patterns = pyfpgrowth.find_frequent_patterns(data_l, per_n) 
        patterns_list.append(patterns)
        rules = pyfpgrowth.generate_association_rules(patterns, confidence)
        rules_list.append(rules)
    
    #format the rules, bring back in the other info on latency rank
    while {} in rules_list:
        rules_list.remove({})#lremove empty items if they have less than 24 hours
    formated_rules=[]

    for i in rules_list:
        formatrule=format_rules(i, df, apps_server)
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
    for i in range(0,df['hour'].nunique()):
        data=df[df['hour']==i]
        data_groups_test.append(data)
    
    
    for i, j in zip(formated_rules, data_groups_test) :
        server_df, server_assignments, total_latency, total_latency_model, avg_latency, avg_latency_model = server_association(i, j, apps_server) #this function loaded fr
        server_df_list.append(server_df)
        server_assign_list.append(server_assignments)
        total_latency_list.append(total_latency)
        total_latency_model_list.append(total_latency_model)
        avg_latency_list.append(avg_latency)
        avg_latency_model_list.append(avg_latency_model)


#bring together all the durations for the actual data and the model
    hours=range(0, df['hour'].nunique())
    model_output=pd.DataFrame({'hours':hours,'total_latency_list': total_latency_list, 'total_latency_model_list': total_latency_model_list, 'avg_latency_list': avg_latency_list, 'avg_latency_model_list': avg_latency_model_list})
    model_output.columns=['hours', 'total_latency', 'total_latency_model', 'avg_latency', 'avg_latency_model']
    model_output['avg_latency_per_reduction']=((model_output['avg_latency']-model_output['avg_latency_model'])/model_output['avg_latency'])*100

    #return(formated_rules)
    return(server_df_list, server_assign_list, model_output , model_output['total_latency'].sum(), model_output['total_latency_model'].sum(), model_output['avg_latency'].mean(), model_output['avg_latency_model'].mean())  
  