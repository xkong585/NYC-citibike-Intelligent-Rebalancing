"""Portable January 2023 flow analysis and chronological forecasting baseline.

Run with a real monthly trip ZIP. No inventory capacities or simulated benefits
are inferred from trip records alone. Raw archives stay outside the repository.
"""
from pathlib import Path
import argparse, hashlib, json, platform, zipfile
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
import folium

ROOT=Path(__file__).resolve().parents[1];OUT=ROOT/'results'/'refreshed'
INDIGO,TEAL,AMBER='#3E4C8A','#148A88','#D99336'
plt.rcParams.update({'font.family':'DejaVu Sans','font.size':10,'axes.spines.top':False,'axes.spines.right':False,'axes.titleweight':'bold','text.color':'#24364B','axes.labelcolor':'#24364B','figure.facecolor':'#F6F7FB','axes.facecolor':'white','savefig.facecolor':'#F6F7FB'})
CMAP=LinearSegmentedColormap.from_list('indigo',['#F3F4FB','#B9BFE1','#737FBC',INDIGO])

def save(fig,name,title,subtitle):
    fig.suptitle(title,x=.06,ha='left',fontsize=18,fontweight='bold',y=.99)
    fig.text(.06,.93,subtitle,fontsize=10,color='#58657B');fig.tight_layout(rect=[.02,.05,.98,.88],h_pad=2.5,w_pad=2.5)
    fig.text(.06,.015,'Source: January 2023 trip archive | Complete month, local reproduction | No measured operational uplift',fontsize=8,color='#64748B')
    fig.savefig(OUT/name,dpi=170);plt.close(fig)

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--data',type=Path,required=True);args=ap.parse_args();OUT.mkdir(parents=True,exist_ok=True)
    cols=['started_at','ended_at','start_station_id','start_station_name','end_station_id','end_station_name','start_lat','start_lng','end_lat','end_lng','member_casual']
    departures=[];arrivals=[];stations=[];riders=[];raw_count=0;clean_count=0
    with zipfile.ZipFile(args.data) as z:
        members=sorted(n for n in z.namelist() if n.endswith('.csv') and '__MACOSX' not in n)
        for name in members:
            for df in pd.read_csv(z.open(name),usecols=cols,dtype={'start_station_id':'string','end_station_id':'string'},chunksize=200000):
                raw_count+=len(df)
                for col in ['started_at','ended_at']:df[col]=pd.to_datetime(df[col],format='mixed',errors='coerce')
                duration=(df.ended_at-df.started_at).dt.total_seconds()/60
                valid=duration.between(1,1440)&df.start_station_id.notna()&df.end_station_id.notna()&df.start_lat.between(40.4,41)&df.start_lng.between(-74.3,-73.7)
                df=df[valid].copy();clean_count+=len(df)
                dep=df.groupby(['start_station_id',df.started_at.dt.floor('h')]).size().rename('departures').reset_index().rename(columns={'start_station_id':'station_id','started_at':'hour'})
                arr=df.groupby(['end_station_id',df.ended_at.dt.floor('h')]).size().rename('arrivals').reset_index().rename(columns={'end_station_id':'station_id','ended_at':'hour'})
                departures.append(dep);arrivals.append(arr)
                stations.append(df.groupby('start_station_id').agg(station_name=('start_station_name','first'),lat_sum=('start_lat','sum'),lon_sum=('start_lng','sum'),trips=('start_lat','size')))
                riders.append(df.member_casual.value_counts())
    dep=pd.concat(departures).groupby(['station_id','hour']).departures.sum()
    arr=pd.concat(arrivals).groupby(['station_id','hour']).arrivals.sum()
    # Outer join preserves station-hours with arrivals but no departures.
    hourly=pd.concat([dep,arr],axis=1).fillna(0).astype(int).reset_index()
    hourly['net_flow']=hourly.arrivals-hourly.departures
    assert hourly.departures.sum()==clean_count and hourly.arrivals.sum()==clean_count
    jan=hourly[hourly.hour.between('2023-01-01','2023-01-31 23:00:00')].copy()
    s=pd.concat(stations).groupby(level=0).agg({'station_name':'first','lat_sum':'sum','lon_sum':'sum','trips':'sum'})
    s['latitude']=s.lat_sum/s.trips;s['longitude']=s.lon_sum/s.trips
    totals=jan.groupby('station_id')[['departures','arrivals','net_flow']].sum().join(s[['station_name','latitude','longitude']]);totals.to_csv(OUT/'station_totals.csv')
    hours=pd.date_range('2023-01-01','2023-01-31 23:00:00',freq='h')
    city=jan.groupby('hour')[['departures','arrivals']].sum().reindex(hours,fill_value=0);city.index.name='hour';city.to_csv(OUT/'city_hourly.csv')
    daily=city.resample('D').sum();daily.to_csv(OUT/'daily_flows.csv')
    riders=pd.concat(riders,axis=1).fillna(0).sum(axis=1).astype(int);riders.rename('trips').to_csv(OUT/'rider_counts.csv')
    fig,axs=plt.subplots(1,2,figsize=(12,5));axs[0].plot(daily.index,daily.departures,color=INDIGO,lw=2.3);axs[0].set(title='Daily departures',ylabel='Cleaned trips');axs[0].tick_params(axis='x',rotation=25);axs[0].grid(alpha=.15)
    axs[1].bar(riders.index,riders.values,color=[INDIGO,AMBER][:len(riders)]);axs[1].set(title='Rider mix',ylabel='Cleaned trips')
    save(fig,'monthly_overview.png','January riding patterns',f'{raw_count:,} source trips | {clean_count:,} pass duration, station-ID and start-coordinate checks')
    by=city.groupby([city.index.dayofweek,city.index.hour]).departures.mean().unstack().reindex(index=range(7),columns=range(24),fill_value=0)
    by.to_csv(OUT/'weekday_hour_mean.csv');fig,ax=plt.subplots(figsize=(12,5));im=ax.imshow(by,cmap=CMAP,aspect='auto');ax.set_yticks(range(7),['Mon','Tue','Wed','Thu','Fri','Sat','Sun']);ax.set_xticks(range(0,24,2));ax.set_xlabel('Hour');fig.colorbar(im,ax=ax,label='Mean departures per observed day')
    save(fig,'weekly_demand.png','How demand changes through the week','Averages use the number of actual observed weekdays in January, rather than pooled totals.')
    top=totals.reindex(totals.net_flow.abs().nlargest(12).index).sort_values('net_flow');labels=top.station_name.fillna(top.index.to_series()).str.slice(0,36)
    fig,ax=plt.subplots(figsize=(11,7));ax.barh(labels,top.net_flow,color=[TEAL if n>=0 else AMBER for n in top.net_flow]);ax.axvline(0,color='#738198',lw=.8);ax.set_xlabel('Monthly arrivals minus departures');ax.grid(axis='x',alpha=.15)
    save(fig,'station_imbalance.png','Where do bikes accumulate or leave?', 'Largest absolute trip-flow imbalances | Positive: more returns; negative: more departures | Not measured inventory')
    # Same held-out week and features for every station; no test outcomes are used
    # in the hour-of-week mean baseline. Daily naive uses information 24 h earlier.
    top_ids=jan[jan.hour<'2023-01-25'].groupby('station_id').departures.sum().nlargest(20).index;errors=[];example=None
    for sid in top_ids:
        series=jan[jan.station_id==sid].set_index('hour').departures.reindex(hours,fill_value=0).astype(float)
        train=series[series.index<'2023-01-25'];test=series[series.index>='2023-01-25']
        lookup=train.groupby([train.index.dayofweek,train.index.hour]).mean()
        pred=pd.Series([lookup.get((t.dayofweek,t.hour),train.mean()) for t in test.index],index=test.index)
        naive=series.shift(24).reindex(test.index)
        for name,values in [('Hour-of-week mean',pred),('Previous-day naive',naive)]:
            errors.append({'station_id':sid,'model':name,'mae':float(np.abs(test-values).mean()),'rmse':float(np.sqrt(np.mean((test-values)**2))),'test_hours':len(test)})
        if example is None:example=pd.DataFrame({'actual':test,'hour_of_week':pred,'previous_day':naive})
    metrics=pd.DataFrame(errors);metrics.to_csv(OUT/'forecast_metrics.csv',index=False);example.to_csv(OUT/'forecast_example.csv')
    means=metrics.groupby('model')[['mae','rmse']].mean();means.to_csv(OUT/'forecast_summary.csv')
    fig,axs=plt.subplots(2,1,figsize=(12,7));example.iloc[:72][['actual','hour_of_week','previous_day']].plot(ax=axs[0],color=[INDIGO,TEAL,AMBER],lw=1.5);axs[0].set(title='First 72 held-out hours at the busiest station',ylabel='Departures / hour',xlabel='');axs[0].legend(['Observed','Hour-of-week mean','Previous-day naive'],fontsize=8,ncol=3)
    axs[1].barh(means.index,means.mae,color=[TEAL,AMBER]);axs[1].set(xlabel='Mean station-level MAE (departures / hour)',title='Average across 20 stations selected using training data');axs[1].grid(axis='x',alpha=.15)
    save(fig,'forecast_baselines.png','Start with a measurable forecasting baseline','Train: Jan 1-24 | Test: Jan 25-31 | Demand-only baselines; no Prophet/ARIMA or rebalancing uplift claim')
    coords=totals.dropna(subset=['latitude','longitude']);fig,ax=plt.subplots(figsize=(9,8));cap=max(1,float(coords.net_flow.abs().quantile(.98)))
    points=ax.scatter(coords.longitude,coords.latitude,c=coords.net_flow.clip(-cap,cap),cmap=LinearSegmentedColormap.from_list('balance',[AMBER,'#F2F3F9',TEAL]),s=6+np.sqrt(coords.departures+coords.arrivals)/3,alpha=.8,vmin=-cap,vmax=cap,linewidths=0)
    ax.set(xlabel='Longitude',ylabel='Latitude');ax.set_aspect(1/np.cos(np.radians(40.73)));fig.colorbar(points,ax=ax,label='Net trip flow (98th-percentile color clipping)')
    save(fig,'station_balance_map.png','A network view of trip-flow imbalance','Each dot is a departure-observed station | Colors describe trip flows, not live bike availability.')
    m=folium.Map(location=[40.74,-73.98],zoom_start=12,tiles='OpenStreetMap')
    for row in coords.reindex(coords.net_flow.abs().nlargest(150).index).itertuples():
        folium.CircleMarker([row.latitude,row.longitude],radius=4+min(abs(row.net_flow)/150,9),color=TEAL if row.net_flow>=0 else AMBER,fill=True,fill_opacity=.65,weight=1,tooltip=f'{row.station_name}: net trip flow {row.net_flow:+,}').add_to(m)
    m.save(OUT/'station_balance_map.html')
    summary={'archive_sha256':hashlib.sha256(args.data.read_bytes()).hexdigest(),'csv_members':members,'raw_trips':raw_count,'cleaned_trips':clean_count,'cleaning_retention':clean_count/raw_count,'observed_station_ids':int(hourly.station_id.nunique()),'city_january_departures':int(city.departures.sum()),'city_january_arrivals':int(city.arrivals.sum()),'arrivals_after_january':int(hourly.loc[hourly.hour>'2023-01-31 23:00:00','arrivals'].sum()),'busiest_stations_evaluated':len(top_ids),'forecast_summary':means.reset_index().to_dict('records'),'python':platform.python_version(),'scope':'Full January 2023 archive, portable pandas flow aggregation and demand baselines. Not a rerun of a complete annual Databricks/VRP system; stages 2-5 in the supplied DBC files are empty.'}
    (OUT/'run_summary.json').write_text(json.dumps(summary,indent=2));print(json.dumps(summary,indent=2))

if __name__=='__main__':main()
