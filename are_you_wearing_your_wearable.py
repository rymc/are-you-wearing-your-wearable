import pandas as pd
import sys
import numpy as np
import gc

def mag(df):
    return np.linalg.norm(df[['x','y','z']])

def is_active(res):
    return res['mag_diff']['var'] > 1e-07

f = sys.argv[1]
df_o = pd.read_csv(f)

df_o.set_index(pd.to_datetime(df_o['timestamp']), inplace=True)
df_o.sort_values(by='timestamp', inplace=True)

dfg = df_o.groupby(pd.TimeGrouper('D'))
not_wearing_times = []
for df in dfg:
    df = df[1]
    df = df.rolling('480s').mean()
    df.dropna(inplace=True)
    df.columns = ['x', 'y','z']
    df=df.resample('480s').mean()
    try:
        df['mag'] =df.apply(mag, axis=1)
        df['mag_diff'] = df['mag'].diff()
        df.dropna(inplace=True)
        res = df.groupby(pd.Grouper(freq='24Min')).agg(['var'])
        res['wearing'] =res.apply(is_active, axis=1)
        res.dropna(inplace=True)
    except Exception as e:
        print str(e)
        continue

    wear_c = len(res.loc[res['wearing'] == True])

    if len(res) == 0:
        print "res is empty"
        continue
    print len(res)
    print "compliance: "
    print (float(wear_c) / len(res) * 100),
    print "%"
    not_wearing_times.append(res.loc[res['wearing'] == False].index.values)

del df
del dfg
gc.collect()

import datetime
for day_time in not_wearing_times:
    for time in day_time:
        time = pd.to_datetime(time)
        endtime = time+datetime.timedelta(minutes = 24)
        df_o.drop(df_o[(df_o.index >= time) & (df_o.index <= endtime)].index, inplace=True)
        df_o.dropna(inplace=True)
df_o.to_csv(f+'-when-wearing.csv')

with open(f+'-not-wearing-times-list.csv', 'w') as f:
    for day_time in not_wearing_times:
        for time in day_time:
            time = pd.to_datetime(time)
            endtime = time+datetime.timedelta(minutes = 24)
            f.write(str(time)+","+str(endtime)+"\n")
