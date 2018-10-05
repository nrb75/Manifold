#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  2 18:02:38 2018

@author: natalie
"""

#this function takes a training dataset and a test dataset, defined by the user, and performs the frequent pattern algorithm.
#the output is the actual data contrasted with the modeled data for the testing period.

import pandas as pd
import pyfpgrowth
from Server_Assign import server_association
from format_rules import format_rules

#df=pd.read_csv('/home/natalie/Documents/Manifold/df_test.csv')


def assign_servers_test_output(df_train, df_test, percentile, confidence, apps_server):
    df_train['hour']=None
    df_train['hour']=pd.DatetimeIndex(df_train['Date']).hour

      
    data_l=list(df_train['pairs'])
    pairs_count=(df_train.groupby('pairs2').agg({'Date':'count', 'norm_latency': 'mean', 'Duration': 'sum', 'Packets':'sum'}).reset_index())
    pairs_count.columns=['pairs','frequency', 'avg_norm_latency', 'total_duration', 'total_packets']
    pairs_count['norm_latency']=(pairs_count['total_duration']/pairs_count['total_packets'].sum())*100 #sum of all duration time divided by sum of all packets transfered for that pair
        
    per_n=(pairs_count['frequency'].quantile(percentile))
    patterns = pyfpgrowth.find_frequent_patterns(data_l, per_n) 
    rules = pyfpgrowth.generate_association_rules(patterns, confidence)
        
    
    #format the rules, bring back in the other info on latency rank

    formated_rules=format_rules(rules, df_train, apps_server)
       
    #now we make the server assignments based on the training rules applied to the test data
    server_df, server_assignments, total_latency, total_latency_model, avg_latency, avg_latency_model = server_association(formated_rules, df_test, apps_server) #this function loaded fr

    #return(formated_rules)
    return(server_df, server_assignments, total_latency, total_latency_model, avg_latency, avg_latency_model)  
  