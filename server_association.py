#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep 30 08:50:26 2018

@author: natalie
"""

#after you run the patterns and rules generation from pypf

#patterns=pyfpgrowth.find_frequent_patterns(data_l, minthreshold) 
#rules=pyfpgrowth.generate_association_rules(patterns, confidence)

#and then run this trhough the 'format_rules' function

#now we assign the servers to each IP address

def server_association(rules_df, df_orig, apps_server):
    import pandas as pd
    import numpy as np
    from collections import defaultdict
    serverlist=[]
    server={}
    ips={}
#serverlist.append(server) #put a new server dictionary into our list

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

    server_df=pd.DataFrame.from_records(serverlist)        
    server_rules=server_df.transpose()
    server_rules['serverid']=server_rules.min(axis=1) #makes a new column with the serverid, which is the only non na value in the row
    server_rules['IP']=server_rules.index
    server_rules=server_rules[['IP', 'serverid']]

#merge in the serverid
    df_servers=df_orig.merge(server_rules, left_on='Src_IP', right_on='IP', how='left')
    df_servers=df_servers.rename(columns={'serverid': 'Src_Server'})
    df_servers=df_servers.merge(server_rules, left_on='Dst_IP', right_on='IP', how='left')
    df_servers=df_servers.rename(columns={'serverid': 'Dst_Server'})
    df_servers=df_servers.drop(['IP_x', 'IP_y'], axis=1)

    df_servers['duration_pred']=df_servers['Duration']
    df_servers['duration_pred'][df_servers['Src_Server']==df_servers['Dst_Server']]=0

    return [server_rules, df_servers['Duration'].sum(), df_servers['duration_pred'].sum(), df_servers['Duration'].mean(), df_servers['duration_pred'].mean()]
    #the outputs are: server_assignments, total latency time origninal data, total latency time predicted after the model, average latency time per transaction in this period, avg latency time predicted per transaction