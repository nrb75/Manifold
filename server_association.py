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
#the inputs are: data=rules_df output from earlier, df_orig=original dataframe formattted correctly, apps_server = number of apps that can fit on a server
def server_association(data, df_orig, apps_server):
    import pandas as pd
    import numpy as np
    from collections import defaultdict

#server 0 is a bucket, want to count how many go into bucket
    serversize=20
    serverlist=[]
    server={}
    ips={}
#serverlist.append(server) #put a new server dictionary into our list

    serverid=0
    for i in range(0,len(data)):
        if len(server)==serversize:
           serverlist.append(server)
           server={}
           serverid=serverid+1
    #not appending last server b/c it is not full
#if IP_B is in this server, it is a duplicate, so we only want to add in the IP_A which has not been added to the server yet
        if (data['IP_B'][i] in server) or data['IP_B'][i] in ips and (len(server)<=(serversize-1)) and (data['IP_A'][i] not in ips):
            server[data['IP_A'][i]]=serverid
            ips[data['IP_A'][i]]=1
    #if IP_B is not in the server, and the server has room for 2 more, and it's matching IP_A is also not in the ip list we know IP_A hasn't been added yet.
    #Thus, we need to add both the IP_A and IP_B in this row to this server and the ip list.
        if (data['IP_B'][i] not in server) and len(server)<=(serversize-2) and (data['IP_A'][i] not in ips) and (data['IP_B'][i] not in ips):
            server[data['IP_A'][i]]=serverid
            server[data['IP_B'][i]]=serverid
            ips[data['IP_A'][i]]=1
            ips[data['IP_B'][i]]=1                                                                                                                                                                                                                          
   ##if IP_B is not in the server, and the server has room for only 1 more, and it's matching IP_A is also not in the ip list we know IP_A hasn't been added yet.
    #we need to create a new server, and add both the IP_A and IP_B in this row to this new server and the ip list.
        if (data['IP_B'][i] not in server) and len(server)==(serversize-1) and (data['IP_A'][i] not in ips) and (data['IP_B'][i] not in ips): #if there is not enough room for the pair, we need to start a new server even if it is not full
            serverlist.append(server)
            server={}
            serverid=serverid+1
            server[data['IP_A'][i]]=serverid
            server[data['IP_B'][i]]=serverid
            ips[data['IP_A'][i]]=1
            ips[data['IP_B'][i]]=1
        if server not in serverlist:
            serverlist.append(server)
     #if last server is not full, we still want to append it to the serverlist   

    server_df=pd.DataFrame.from_records(serverlist)
    server_rules=server_df.transpose()
    server_rules['serverid']=server_rules.min(axis=1)#makes a new column with the serverid, which is the only non na value in the row
    server_rules['IP']=server_rules.index
    server_rules=server_rules[['IP', 'serverid']]
    
    #merge in the serverid

    df_servers=df_orig.merge(server_rules.iloc[:,1:3], left_on='Src_IP', right_on='IP', how='left')
    df_servers=df_servers.rename(columns={'serverid': 'Src_Server'})
    
    
    df_servers=df_servers.merge(server_rules.iloc[:,1:3], left_on='Dst_IP', right_on='IP', how='left')
    df_servers=df_servers.rename(columns={'serverid': 'Dst_Server'})
    df_servers=df_servers.drop(['IP_x', 'IP_y'], axis=1)

#set the duration equal to 0 if the serverd match
    df_servers['duration_pred']=df_servers['Duration']
    df_servers.loc[df_servers['Src_Server']==df_servers['Dst_Server'], 'duration_pred']=0
    
    
    return [server_rules, df_servers['Duration'].sum(), df_servers['duration_pred'].sum(), df_servers['Duration'].mean(), df_servers['duration_pred'].mean()]
    #the outputs are: server_assignments, total latency time origninal data, total latency time predicted after the model, average latency time per transaction in this period, avg latency time predicted per transaction