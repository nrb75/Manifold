#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct  1 07:32:51 2018

@author: natalie
"""

#after the rules are generated and formtted, we now want to assign apps to specific servers
#this function is a simple way to assign high priority apps to specific servers. Rank is determined based on frequency and normalized latency time.
#normalized latency time was calculated as sum of duration for that pair in the entire dataset / total packets for that pair in the dataset


def server_association_apriori(rules_df, df_orig, apps_server):
    import pandas as pd
    from collections import defaultdict
    serverlist=[]
    server={}
    ips={}
    last=len(rules_df)-1
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
        if server not in serverlist and i==last:
            serverlist.append(server)
     #if last server is not full, we still want to append it to the serverlist   

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
    df_servers.loc[df_servers['Src_Server']==df_servers['Dst_Server'], 'duration_pred']=0

    return [df_servers, server_rules, df_servers['Duration'].sum(), df_servers['duration_pred'].sum(), df_servers['Duration'].mean(), df_servers['duration_pred'].mean()]
    #the total time based on our new model
    #dataframe with servers assigned
    #each app with the assigned server that had a rule
    #total latency time for the data
    #total latency time predicted by model
    #avg transaction time for the data
    #avg transaction time for the model
    
