## Simple Exponential Smoothing
## This project uses a simple expoential smoothing method to predict the cost of key commodities in a subnational location and compare trends to national average
##

import pandas as pd
import datetime as dt
import numpy as np
import os
import matplotlib.pyplot as plt
from statsmodels.tsa.api import ExponentialSmoothing, SimpleExpSmoothing, Holt
%matplotlib inline

project=os.getcwd()
df=pd.read_csv(project+"file.csv")
df.head()

items=list(set(df.item_id))

int_df=pd.DataFrame()
for item in items:
    item_frame=df.loc[df.item_id==item]
    item_frame.price_location=item_frame.price_location.interpolate(method='linear')
    item_frame.price_nationalavg=item_frame.price_nationalavg.interpolate(method='linear')
    int_df=pd.concat([int_df, item_frame])

for i in items:
    print("Analyzing:", i)
    item=int_df.loc[int_df.item_id==i].dropna(subset=['price_location'])
    item_location=list(item['price_location'])
    item_natl=list(item['price_nationalavg'])
    item.date=[x.strftime('%Y-%m-%d') for x in item.date]
    dates=pd.date_range(start=min(item.date), end=max(item.date), freq='MS')
    location=pd.Series(item_location, dates)
    natl=pd.Series(item_natl, dates)

    locfit = Holt(location,exponential=True, initialization_method="estimated").fit(smoothing_level=0.9, smoothing_trend=0.2)
    loccast = locfit.forecast(6).rename("location (Holt's Method)")
    nfit = Holt(natl, exponential=True, initialization_method="estimated").fit(smoothing_level=0.9, smoothing_trend=0.2)
    nfcast = nfit.forecast(6).rename("National Avg (Holt's Method)")

    fig=plt.figure(figsize=(10,6))
    plt.plot(location, marker='o', color='blue')
    plt.plot(natl, marker='o', color='red')
    fig.suptitle('Price of %s in Location vs National Avg, with 90 Day Forecast' % i)
    plt.ylabel('Price')
    plt.xlabel('Month')

    #plt.plot(locfit.fittedvalues, marker='o', color='lightblue')
    line1, = plt.plot(loccast,marker='o', color='lightblue')
    #plt.plot(nfit.fittedvalues, marker='o', color='pink')
    line2, = plt.plot(nfcast, marker='o', color='pink')
    plt.legend([line1, line2], [loccast.name, nfcast.name])
    fig.show()
    plt.savefig('initresults_6mos_%s' %i)
