# Importing Libraries Start -----------------------------------------------------
import pandas as pd
import requests
import json
import pyodbc
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pyarrow.parquet as pq
import pyarrow as pa
import re
import urllib
from sqlalchemy import create_engine
import duckdb
import os
# Importing Libraries End -----------------------------------------------------

# Setting Directories Start ---------------------------------------------------
data_destination = 'I:\\COF\\COF\\_DA&E_\\Nikita\\R_Projects\\datahub\\data\\'

os.chdir('I:\\COF\\COF\\_DA&E_\\Nikita\\Python_Projects\\duckDB\\datahub_backend')
# Setting Directories End ---------------------------------------------------



# LEGACY CODE ########################################################
# oldTrips = pq.read_table('final_trips.parquet')
# oldTrips = oldTrips.to_pandas()
# oldTrips.id = oldTrips.id.astype(int)
# oldTrips['count'] = oldTrips['count'].astype(int)
# oldTrips['year_month'] = pd.to_datetime(oldTrips['year_month'], format='%Y%m')
# oldTrips = oldTrips.loc[oldTrips['year_month'] < '2022-01-01', ]
# print(oldTrips.head())

# oldPickUps = pq.read_table('final_pu.parquet')
# oldPickUps = oldPickUps.to_pandas()
# oldPickUps.id = oldPickUps.id.astype(int)
# oldPickUps['count'] = oldPickUps['count'].astype(int)
# oldPickUps['year_month'] = pd.to_datetime(oldPickUps['year_month'], format='%Y%m')
# oldPickUps = oldPickUps.loc[oldPickUps['year_month'] < '2022-01-01', ]
# print(oldPickUps.head())

# oldDropOffs = pq.read_table('final_do.parquet')
# oldDropOffs = oldDropOffs.to_pandas()
# oldDropOffs.id = oldDropOffs.id.astype(int)
# oldDropOffs['count'] = oldDropOffs['count'].astype(int)
# oldDropOffs['year_month'] = pd.to_datetime(oldDropOffs['year_month'], format='%Y%m')
# oldPickUps = oldPickUps.loc[oldPickUps['year_month'] < '2022-01-01', ]
# print(oldDropOffs.head())
# ##########################################################


# spin a duckdb virtual db
con = duckdb.connect(database=':memory:')
# Setting up DB Connections End --------------------------------------


# Setting up dynamic date ranges for querying Start ------------------
today = datetime.today().date()
today = pd.to_datetime(today).to_period('M').to_timestamp().date()
print(today)

last_month = today + relativedelta(months=-1)
last_month_fhv = today + relativedelta(months=-1)

end = int(last_month.strftime('%Y%m%d%H'))
end_fhv = int(last_month_fhv.strftime('%Y%m%d%H'))

start = pd.to_datetime('2021-06-01').to_period('M').to_timestamp().date()
start = int(start.strftime('%Y%m%d%H'))


print(end)
print(start)

end_test = pd.to_datetime('2014-01-01').to_period('M').to_timestamp().date()

# Setting up dynamic date ranges for querying End ------------------


# Pulling in location ids for further transformations
idall = pd.read_csv('idall.csv', index_col=None)
idall.reset_index(drop=True, inplace=True)


# Queries to pull trip data from the repo Start --------------------
query_yellow = f"""
    SELECT
         datetimeid/10000 as pu_year_month,
         pulocationid, dolocationid,
         count(datetimeid) as pu_count
         FROM 'I:\\COF\\COF\\_DA&E_\\DA&E_DATABASE_PROJECT\\trip_repository\\tpep_monthly_trip_record\\*.parquet'
         where
         datetimeid >= 2011010100 and datetimeid < 2022060100 and trip_distance > 0 and total_amount > 0
         group by
         datetimeid/10000,
         pulocationid,dolocationid

"""


query_green = f"""
    SELECT
         datetimeid/10000 as pu_year_month,
         pulocationid, dolocationid,
         count(datetimeid) as pu_count
         FROM 'I:\\COF\\COF\\_DA&E_\\DA&E_DATABASE_PROJECT\\trip_repository\\tpep_monthly_trip_record\\*.parquet'
         where
         datetimeid >= 2014010100 and datetimeid < 2022060100 and trip_distance > 0 and total_amount > 0
         group by
         datetimeid/10000,
         pulocationid,dolocationid

"""


query_fhv = f"""
    SELECT
         datetimeid/10000 as pu_year_month,
         pulocationid, dolocationid,
          count(datetimeid) as pu_count
         FROM 'I:\\COF\\COF\\_DA&E_\\DA&E_DATABASE_PROJECT\\trip_repository\\fhv_monthly_trip_record\\*.parquet' as trips
         			inner join 'I:\\COF\\COF\\_DA&E_\\DA&E_DATABASE_PROJECT\\license_repository\\fhv_base_list\\*.parquet' as bases
 			on
				trips.dispatching_base_num = bases.lic_no
         where company not in ('UBER', 'LYFT', 'JUNO', 'VIA') and
         datetimeid >= 2015010100 and datetimeid < 2022060100
         group by
         datetimeid/10000,
         pulocationid,dolocationid
   """


query_old_hvfhv = f"""
    SELECT
          datetimeid/10000 as pu_year_month,
          pulocationid, dolocationid,
          company as company,
          count(datetimeid) as pu_count
    FROM 'I:\\COF\\COF\\_DA&E_\\DA&E_DATABASE_PROJECT\\trip_repository\\fhv_monthly_trip_record\\*.parquet' as trips
         			inner join
				'I:\\COF\\COF\\_DA&E_\\DA&E_DATABASE_PROJECT\\license_repository\\fhv_base_list\\*.parquet' as bases
 			on
				trips.dispatching_base_num = bases.lic_no
          where company in ('UBER', 'LYFT', 'JUNO', 'VIA') and
          datetimeid >= 2017060100 and datetimeid < 2019020100
          group by
          datetimeid/10000,
          pulocationid,dolocationid,company

"""




query_hvfhv = f"""
    SELECT
         datetimeid/10000 as pu_year_month,
         pulocationid, dolocationid, hvfhs_license_num,
         count(datetimeid) as pu_count
          FROM 'I:\\COF\\COF\\_DA&E_\\DA&E_DATABASE_PROJECT\\trip_repository\\hvfhv_monthly_trip_record\\*.parquet'
         where
         datetimeid >= 2019020100 and datetimeid < 2022060100 and trip_miles > 0 and base_passenger_fare > 0
         group by
         datetimeid/10000,
         pulocationid,dolocationid,hvfhs_license_num
   
"""

query_shared = f"""
    SELECT
         datetimeid/10000 as pu_year_month,
         pulocationid, dolocationid,
         count(datetimeid) as pu_count
         FROM 'I:\\COF\\COF\\_DA&E_\\DA&E_DATABASE_PROJECT\\trip_repository\\hvfhv_monthly_trip_record\\*.parquet'
         where route_id is not NULL and
         datetimeid >= 2019020100 and datetimeid < 2022060100 and trip_miles > 0 and base_passenger_fare > 0
         group by
         datetimeid/10000,
         pulocationid,dolocationid
   
"""

# Queries to pull trip data from the repo End --------------------



# Function to excecute the queries and transform the data Start --------------------
def trips_loop(query_ind, industry):
    """

    Parameters
    ----------
    query_ind : CHAR
        SQL Query
    industry : CHAR
        TLC industry like yellow, green, fhv

    Returns
    -------
    dataframe with trip counts by location for each industry and service zone

    """
    

    # There are 4 industries. This Condition covers hvs
    if industry == 'hvfhv':

        # Running the query
        monthly_pickups_by_location = pd.read_sql_query(query_ind,con)
        # Renaming the columns
        monthly_pickups_by_location.columns = ['year_month','pulocationid','dolocationid','company','count']

        # looping through the data and renaming company codes into names
        for i in [{'HV0002':'JUNO'},{'HV0004':'VIA'},{'HV0003':'UBER'},{'HV0005':'LYFT'}]:
            for code,name in i.items():
              monthly_pickups_by_location.loc[monthly_pickups_by_location['company'].isin([code]), 'company'] = name

        

        # summing pickups by month location and company
        pu = monthly_pickups_by_location.groupby(['year_month','pulocationid','company'], as_index=False)['count'].sum()
        # summing dos by month location and company
        do = monthly_pickups_by_location.groupby(['year_month','dolocationid','company'], as_index=False)['count'].sum()

    
        # empty df to store results
        trips = pd.DataFrame([])

        # this loop goes over each column of the idall and calculates trip sums for taxi zones that are located in that column
        # It then stacks the results and we get the sums for each taxi zone in each service zone
        for i in range(2,len(idall.columns)):
            # take 1 column
            col = str(idall.columns[i])
            # keep only records where 'Yes'
            a = idall.loc[idall[col] == 'Yes',['ID']]
            # keep rows where zone in our extracted ids and then count 
            f = monthly_pickups_by_location.loc[~monthly_pickups_by_location['pulocationid'].isin(a['ID']) 
            & monthly_pickups_by_location['dolocationid'].isin(a['ID'])].groupby(['year_month','dolocationid','company'], as_index=False).agg(count1 = ('count', sum))
          
            e = pu.loc[pu['pulocationid'].isin(a['ID']),:]
            
            
            e = e.rename({'pulocationid':'id'}, axis=1)

            f = f.rename({'dolocationid':'id'}, axis=1)
            

            f = e.merge(f, on=['year_month', 'id','company'], how = 'left')

            f=f.fillna(0)

            f['volume'] = f['count'] + f['count1']
            f['zone'] = col
            
         

            trips = trips.append(f).reset_index(drop=True)
        return trips, pu, do 
    # same as above but for the rest of industries
    else:


        monthly_pickups_by_location = pd.read_sql_query(query_ind,con)


        monthly_pickups_by_location.columns = ['year_month','pulocationid','dolocationid','count']

        

        pu = monthly_pickups_by_location.groupby(['year_month','pulocationid'], as_index=False)['count'].sum()

        do = monthly_pickups_by_location.groupby(['year_month','dolocationid'], as_index=False)['count'].sum()

        pu['company'] = industry
        do['company'] = industry


        trips = pd.DataFrame([])

        for i in range(2,len(idall.columns)):
            col = str(idall.columns[i])
            
            a = idall.loc[idall[col] == 'Yes',['ID']]

            f = monthly_pickups_by_location.loc[~monthly_pickups_by_location['pulocationid'].isin(a['ID']) 
            & monthly_pickups_by_location['dolocationid'].isin(a['ID'])].groupby(['year_month','dolocationid'], as_index=False).agg(count1 = ('count', sum))
        
            e = pu.loc[pu['pulocationid'].isin(a['ID']),:]
            
            
            e = e.rename({'pulocationid':'id'}, axis=1)

            f = f.rename({'dolocationid':'id'}, axis=1)
            

            f = e.merge(f, on=['year_month', 'id'], how = 'left')

            f=f.fillna(0)

            f['volume'] = f['count'] + f['count1']
            f['zone'] = col
            
         

            trips = trips.append(f).reset_index(drop=True)
        return trips, pu, do
# Function to excecute the queries and transform the data End --------------------

# Running the function for each industry (Can be rewritten as loop)
yellow_trips, yellow_pu, yellow_do = trips_loop(query_yellow, 'yellow')
green_trips, green_pu, green_do = trips_loop(query_green, 'green')
no_hv_trips, no_hv_pu, no_hv_do = trips_loop(query_fhv, 'no_hv')
hvfhv_trips, hvfhv_pu, hvfhv_do = trips_loop(query_hvfhv, 'hvfhv')
shared_trips, shared_pu, shared_do = trips_loop(query_shared, 'shared')

# Final aggregations-----------------------------------------------------

final_trips = pd.concat([yellow_trips, green_trips, no_hv_trips, hvfhv_trips, shared_trips], ignore_index=True)
final_do = pd.concat([yellow_do, green_do, no_hv_do, hvfhv_do, shared_do], ignore_index=True)
final_pu = pd.concat([yellow_pu, green_pu, no_hv_pu, hvfhv_pu, shared_pu], ignore_index=True)

final_pu = final_pu.rename({'pulocationid':'id'}, axis=1)
final_do = final_do.rename({'dolocationid':'id'}, axis=1)

final_trips = final_trips.drop(final_trips.columns[[2,4]], axis = 1)

final_trips = final_trips.rename({'volume':'count'}, axis=1)


# Prep files to be converted to parquets -----------------------------------------------------

final_trips_for_parquet = pa.Table.from_pandas(final_trips, preserve_index=False)
final_do_for_parquet = pa.Table.from_pandas(final_do, preserve_index=False)
final_pu_for_parquet = pa.Table.from_pandas(final_pu, preserve_index=False)

# Writing parquets (this is to cache interim results) -----------------------------------------------------
pq.write_table(final_trips_for_parquet, 'final_trips.parquet')
pq.write_table(final_do_for_parquet, 'final_do.parquet')
pq.write_table(final_pu_for_parquet, 'final_pu.parquet')

##################################################


# Correcting the dateformat, taking out nas, renaming, and formatting Start ----------------------------------------------------
final_pu['year_month'] = pd.to_datetime(final_pu['year_month'], format='%Y%m')
#final_pu = pd.concat([oldPickUps,final_pu], ignore_index=True)
final_pu = final_pu.loc[~final_pu['id'].isna(),]
final_pu.loc[final_pu['company']=='no_hv', 'company'] = 'FHV'
final_pu['company'] = final_pu['company'].str.upper()

final_pu.company.unique()
#final_do.loc[final_do['company'].isin(['UBER', 'LYFT','VIA','JUNO']), 'company'] = 'HVFHV'
final_do['year_month'] = pd.to_datetime(final_do['year_month'], format='%Y%m')
#final_do = pd.concat([oldDropOffs,final_do], ignore_index=True)
final_do = final_do.loc[~final_do['id'].isna(),]
final_do.loc[final_do['company']=='no_hv', 'company'] = 'FHV'
final_do['company'] = final_do['company'].str.upper()

final_do.company.unique()
#final_trips.loc[final_trips['company'].isin(['UBER', 'LYFT','VIA','JUNO']), 'company'] = 'HVFHV'
final_trips['year_month'] = pd.to_datetime(final_trips['year_month'], format='%Y%m')
#final_trips = pd.concat([oldTrips,final_trips], ignore_index=True)
final_trips = final_trips.loc[~final_trips['id'].isna(),]
final_trips.loc[final_trips['company']=='no_hv', 'company'] = 'FHV'
final_trips['company'] = final_trips['company'].str.upper()

final_trips.company.unique()
# Correcting the dateformat, taking out nas, renaming, and formatting End ----------------------------------------------------



#########################################################

# Setting up DB Connections Start --------------------------------------
params = urllib.parse.quote_plus("DRIVER={SQL Server};SERVER=TLCV-Analytic-P.csc.nycnet;DATABASE=TLC_PLCPRG_PRD;Trusted_Connection=yes")
engine= create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
# Setting up DB Connections End --------------------------------------


# Queries to pull total trad fhv trips Start ----------------------------
fhv_subindustry_query= f"""
    SELECT
DATEADD(MONTH, DATEDIFF(MONTH, 0, [metric_day]), 0) as year_month,
999 as id,
     sum([count_trips]) as count
      ,[industry]
  FROM [TLC_PLCPRG_PRD].[dbo].[industry_indicators_daily_trips]
                                  where industry in ('fhv_black_car', 'fhv_livery', 'fhv_lux_limo') and period_start < {end_fhv}
                                  group by DATEADD(MONTH, DATEDIFF(MONTH, 0, [metric_day]), 0), [industry]
                                  order by DATEADD(MONTH, DATEDIFF(MONTH, 0, [metric_day]), 0) desc

"""

fhv_all_query = f"""
    SELECT 
DATEADD(MONTH, DATEDIFF(MONTH, 0, [metric_day]), 0) as year_month,
999 as id,
sum([count_trips]) as count
,'fhv' as industry
FROM [TLC_PLCPRG_PRD].[dbo].[industry_indicators_daily_trips]
where industry in ('fhv_black_car', 'fhv_livery', 'fhv_lux_limo') and period_start < {end_fhv}
group by DATEADD(MONTH, DATEDIFF(MONTH, 0, [metric_day]), 0) 
order by DATEADD(MONTH, DATEDIFF(MONTH, 0, [metric_day]), 0) desc
   
"""
fhv_subindustry = pd.read_sql_query(fhv_subindustry_query,engine)
fhv_all = pd.read_sql_query(fhv_all_query,engine)



fhv3 = pd.concat([fhv_subindustry, fhv_all], ignore_index=True)


fhv3['year_month'] = fhv3['year_month'].dt.date


fhv3.to_csv(data_destination + 'fhv3.csv')


# Queries to pull total trad fhv trips End ----------------------------   


# Merging the results to the location lookup table Start ------------
lookup = pq.read_table('lookup.parquet')
lookup = lookup.to_pandas()

lok = lookup.loc[:,['LocationID','Zone']]

lok = lok.rename({'LocationID':'id', 'Zone':'zone'}, axis=1)

lok.info()

#lok['id'] = str(int['id'])

pu = final_pu.merge(lok, on=['id'], how = 'left')
do = final_do.merge(lok, on=['id'], how = 'left')

# Merging the results to the location lookup table End ------------



# This loop does that same things as the one before but just for Pickups or Dropoffs
def zoning_loop(dataframe, metrics):


    trips = pd.DataFrame([])

    for i in range(1,len(idall.columns)):
        col = str(idall.columns[i])
        a = idall.loc[idall[col] == 'Yes',['ID']]

        data = dataframe.loc[dataframe['id'].isin(a['ID'])]
    
        data['metric'] = metrics
        data['Zone'] = col
        
        trips = trips.append(data).reset_index(drop=True)
    return trips

pickUps = zoning_loop(pu, 'PickUps')
dropOffs = zoning_loop(do, 'DropOffs')
#-----------------------------------------------------------------------------------



# Calculating Citywide trips (we do not need loop for that) Start -------------------
a = idall.loc[idall['Citywide'] == 'Yes',['ID']]

final_trips = final_trips.rename({'zone':'Zone'}, axis=1)
final_trips = final_trips.merge(lok, on=['id'], how = 'left')
final_trips['metric'] = 'Trips'

data = pu.loc[pickUps['id'].isin(a['ID'])]

data['Zone'] = 'Citywide'
data['metric'] = 'Trips'

# Calculating Citywide trips (we do not need loop for that) End -------------------



# Combining Trips, Pickups, and DropOffs Start -------------------
Trips = pd.concat([final_trips, data], ignore_index=True)

combined = pd.concat([Trips,pickUps,dropOffs], ignore_index=True)

combined = combined.sort_values(by = ['year_month'], ascending=False)

combined_comb = combined.loc[(~combined['company'].isin(['SHARED','NOHV'])) & (combined['year_month'] >= pd.to_datetime('2017-06-01')),].reset_index(drop=True)


# Combining Trips, Pickups, and DropOffs End -------------------


# Final touches and aggregations Start ------------------------

del combined_comb['company']

combined_comb.info()
combined_comb['zone'] = combined_comb['zone'].astype('string')


combined_comb = combined_comb.groupby(['year_month','id','Zone','zone','metric']).agg(count = ('count', sum)).reset_index()
combined_comb['company'] = 'ALL'

combined = pd.concat([combined, combined_comb], ignore_index=True)



combined['company'] = combined['company'].str.replace(' ','')

combined = combined.rename({'Zone':'Z', 'zone':'Zone'}, axis=1)

combined  = combined.loc[combined['year_month'] < pd.to_datetime(last_month),]

combined.loc[combined['company'] == 'NOHV', 'company'] = 'FHV'


combined['year_month'] = combined['year_month'].dt.date


combined.info()

combined.loc[combined['company'].isin(['UBER', 'LYFT','VIA','JUNO']), 'company'] = 'HVFHV'
combined = combined.groupby(['year_month','id','company', 'Z','Zone','metric']).agg(count = ('count', sum)).reset_index()

combined = combined.loc[combined['company'] != 'FHV',]

# combined.to_feather('trips_updated.feather')
combined.to_csv(data_destination +'trips_updated.csv')

# Final touches and aggregations End ------------------------




############################################################################
#FARES, TRIPTIME, TRIPMIL


# Queries to calculate average fares, trip time and trip mileages (not used in the DataHub at the moment) Start ---------------
query_yellow = f"""
    SELECT
         datetimeid/10000 as pu_year_month,
         pulocationid, avg(total_amount) as avg_total_fare, sum(COALESCE(total_amount,0)) as sum_total_fare, avg(trip_time_in_secs)/60 as avg_trip_time, avg(trip_distance) as avg_trip_distance
         
         FROM 'I:\\COF\\COF\\_DA&E_\\DA&E_DATABASE_PROJECT\\trip_repository\\tpep_monthly_trip_record\\*.parquet'
         where
         datetimeid >= 2011010100 and datetimeid < 2011020100 and trip_distance > 0 and total_amount > 0 and total_amount < 250 and trip_distance < 100 and trip_time_in_secs > 0 and trip_time_in_secs < 7200
         group by
         datetimeid/10000,
         pulocationid

"""


query_green = f"""
    SELECT
         datetimeid/10000 as pu_year_month,
         pulocationid, avg(total_amount) as avg_total_fare, sum(COALESCE(total_amount,0)) as sum_total_fare, avg(trip_time_in_secs)/60 as avg_trip_time, avg(trip_distance) as avg_trip_distance
         FROM 'I:\\COF\\COF\\_DA&E_\\DA&E_DATABASE_PROJECT\\trip_repository\\lpep_monthly_trip_record\\*.parquet'
         where
         datetimeid >= 2014010100 and datetimeid < 2014020100 and trip_distance > 0 and total_amount > 0 and total_amount < 250 and trip_distance < 100 and trip_time_in_secs > 0 and trip_time_in_secs < 7200
         group by
         datetimeid/10000,
         pulocationid

"""





query_hvfhv = f"""
    SELECT
         datetimeid/10000 as pu_year_month,
         pulocationid, avg(COALESCE(base_passenger_fare,0) + COALESCE(tolls,0)  + COALESCE(bcf,0)  + COALESCE(sales_tax,0)  + COALESCE(congestion_surcharge,0)  + COALESCE(airport_fee,0)  + COALESCE(tips,0) ) as avg_total_fare, sum(COALESCE(base_passenger_fare,0)  + COALESCE(tolls,0)  + COALESCE(bcf,0)  + COALESCE(sales_tax,0)  + COALESCE(congestion_surcharge,0)  + COALESCE(airport_fee,0) + COALESCE(tips,0) ) as sum_total_fare, avg(trip_time)/60 as avg_trip_time, avg(trip_miles) as avg_trip_distance
         
         FROM 'I:\\COF\\COF\\_DA&E_\\DA&E_DATABASE_PROJECT\\trip_repository\\hvfhv_monthly_trip_record\\*.parquet'
         where
         datetimeid >= 2019020100 and datetimeid < 2019030100 and trip_miles > 0 and base_passenger_fare > 0 and base_passenger_fare < 250 and trip_miles < 100 and trip_time > 0 and trip_time < 7200
         group by
         datetimeid/10000,
         pulocationid

"""

query_shared = f"""
    SELECT
         datetimeid/10000 as pu_year_month,
 pulocationid, avg(COALESCE(base_passenger_fare,0) + COALESCE(tolls,0)  + COALESCE(bcf,0)  + COALESCE(sales_tax,0)  + COALESCE(congestion_surcharge,0)  + COALESCE(airport_fee,0)  + COALESCE(tips,0) ) as avg_total_fare, sum(COALESCE(base_passenger_fare,0)  + COALESCE(tolls,0)  + COALESCE(bcf,0)  + COALESCE(sales_tax,0)  + COALESCE(congestion_surcharge,0)  + COALESCE(airport_fee,0) + COALESCE(tips,0) ) as sum_total_fare, avg(trip_time)/60 as avg_trip_time, avg(trip_miles) as avg_trip_distance
         FROM 'I:\\COF\\COF\\_DA&E_\\DA&E_DATABASE_PROJECT\\trip_repository\\hvfhv_monthly_trip_record\\*.parquet'
         where route_id is not NULL and
         datetimeid >= 2019020100 and datetimeid < 2019030100 and trip_miles > 0 and base_passenger_fare > 0 and base_passenger_fare < 250 and trip_miles < 100 and trip_time > 0 and trip_time < 7200
         group by
         datetimeid/10000,
         pulocationid

"""

# Queries to calculate average fares, trip time and trip mileages (not used in the DataHub at the moment) End ---------------



# The function below is the same as before but covers fares, time and mileage Start ---------------

def trips_loop(query_ind, industry):

    monthly_pickups_by_location = pd.read_sql_query(query_ind,con)
    
    monthly_pickups_by_location.columns = ['year_month','pulocationid', 'avg_total_fare','sum_total_fare','avg_trip_time', 'avg_trip_distance' ]
    
    monthly_pickups_by_location['company'] = industry
    
    trips = pd.DataFrame([])
    
    for i in range(1,len(idall.columns)):
        col = str(idall.columns[i])
        
        a = idall.loc[idall[col] == 'Yes',['ID']]
    
        #f = monthly_pickups_by_location.loc[~monthly_pickups_by_location['pulocationid'].isin(a['ID'])].groupby(['pu_year_month','pulocationid','company'], as_index=False).agg(count1 = ('count', sum))
    
        e = monthly_pickups_by_location.loc[monthly_pickups_by_location['pulocationid'].isin(a['ID']),:]
        
        
        e = e.rename({'pulocationid':'id'}, axis=1)
    
        #f = f.rename({'dolocationid':'id'}, axis=1)
        
    
        #f = e.merge(f, on=['year_month', 'id','company'], how = 'left')
    
        #f=f.fillna(0)
    
        #f['volume'] = f['count'] + f['count1']
        e['zone'] = col
        
     
    
        trips = trips.append(e).reset_index(drop=True)
        
    return trips
    
# The function below is the same as before but covers fares, time and mileage End ---------------    
    

# Applying the function and stacking --------------- 
yellow_trips = trips_loop(query_yellow, 'yellow')
green_trips = trips_loop(query_green, 'green')
hvfhv_trips = trips_loop(query_hvfhv, 'hvfhv')
shared_trips = trips_loop(query_shared, 'shared')   
    
final_financials = pd.concat([yellow_trips, green_trips, hvfhv_trips, shared_trips], ignore_index=True)


# Caching the result ---------------------------
final_financials_parq = pa.Table.from_pandas(final_financials, preserve_index=False)
pq.write_table(final_financials_parq, 'financials_old.parquet')


# The final steps in this sections are the same as with trips but cover fares, time and mileage Start------------

final_financials['year_month'] = pd.to_datetime(final_financials['year_month'], format='%Y%m')
#final_trips = pd.concat([oldTrips,final_trips], ignore_index=True)
final_financials = final_financials.loc[~final_financials['id'].isna(),]
#final_trips.loc[final_trips['company']=='no_hv', 'company'] = 'FHV'
final_financials['company'] = final_financials['company'].str.upper()

final_financials.company.unique()


lookup = pq.read_table('lookup.parquet')
lookup = lookup.to_pandas()

lok = lookup.loc[:,['LocationID','Zone']]

lok = lok.rename({'LocationID':'id', 'Zone':'zone'}, axis=1)

lok.info()

#lok['id'] = str(int['id'])

final_financials = final_financials.merge(lok, on=['id'], how = 'left')


final_financials = final_financials.sort_values(by = ['year_month', 'company', 'zone_x'], ascending=False)



final_financials['company'] = final_financials['company'].str.replace(' ','')

final_financials = final_financials.rename({'zone_x':'Z', 'zone_y':'Zone'}, axis=1)

final_financials = final_financials.loc[final_financials['year_month'] < pd.to_datetime(last_month),]



final_financials['year_month'] = final_financials['year_month'].dt.date


#combined = combined.loc[combined['year_month'] >= end_test, ]
final_financials.info()

final_financials = final_financials.reset_index(drop=True)

final_financials = final_financials.rename({'avg_total_fare':'Average Fare', 'sum_total_fare':'Total Fare','avg_trip_time':'Average Trip Time','avg_trip_distance':'Average Trip Distance'}, axis=1)


fin = pd.melt(final_financials, id_vars=['year_month', 'id','company','Z','Zone' ], value_vars=['Average Fare', 'Total Fare','Average Trip Time','Average Trip Distance'])
fin = fin.rename({'variable':'metric', 'value':'count'}, axis=1)

#trips_updated = pd.read_csv('https://github.com/analytics-tlc/datahub/blob/main/data/trips_updated.csv?raw=true')
#trips_updated = trips_updated.drop(trips_updated.columns[0], axis=1)
# combined.to_feather('trips_updated.feather')
trips_updated = pd.read_csv(data_destination +'trips_updated.csv')
trips_updated = pd.concat([trips_updated, fin], ignore_index=True)


trips_updated.to_csv(data_destination +'trips_updated.csv')


# The final steps in this sections are the same as with trips but cover fares, time and mileage End------------


############################################################################


# The queries to pull the data that will feed the financial part of the DataHub Start ----------------------

query_yellow = f"""
    SELECT
         datetimeid/10000 as year_month,
         avg(total_amount) as avg_total_fare, sum(COALESCE(total_amount,0)) as sum_total_fare,
         avg(fare_amount) as avg_fare_amount, sum(COALESCE(fare_amount,0)) as sum_fare_amount,
         avg(COALESCE(extra,0)) as avg_extra, sum(COALESCE(extra,0)) as sum_extra,
         0.50 as mta_tax, sum(COALESCE(mta_tax,0)) as total_mta_tax,
         avg(NULLIF(tip_amount,0)) as avg_tip_amount, sum(COALESCE(tip_amount,0)) as sum_tip_amount,
         avg(NULLIF(tolls_amount,0)) as avg_tolls_amount, sum(COALESCE(tolls_amount,0)) as sum_tolls_amount,
         0.30 as improvement_surcharge, sum(COALESCE(improvement_surcharge,0)) as sum_improvement_surcharge,
         avg(NULLIF(congestion_surcharge,0)) as avg_congestion_surcharge, sum(COALESCE(congestion_surcharge,0)) as sum_congestion_surcharge,
         avg(NULLIF(airport_fee,0)) as avg_airport_fee, sum(COALESCE(airport_fee,0)) as sum_airport_fee,
         avg(trip_distance) as avg_distance,
         count(distinct hack_number) as count_drivers,
         avg(fare_amount) + avg(COALESCE(extra,0)) as driver_pay,
         sum(COALESCE(fare_amount,0)) + sum(COALESCE(extra,0)) as total_driver_pay,
         (sum(COALESCE(fare_amount,0)) + sum(COALESCE(extra,0)))/ count(distinct hack_number) as pay_per_driver,
         sum(COALESCE(tip_amount,0))/count(distinct hack_number) as tips_per_driver,
         sum(COALESCE(extra,0))/count(distinct hack_number) as extra_per_driver,
         sum(COALESCE(fare_amount,0))/count(distinct hack_number) as fare_per_driver
         FROM 'I:\\COF\\COF\\_DA&E_\\DA&E_DATABASE_PROJECT\\trip_repository\\tpep_monthly_trip_record\\*.parquet'
         where
         datetimeid >= 2011010100 and datetimeid < {end} and trip_distance > 0 and total_amount > 0 and total_amount < 250 and trip_distance < 100
         group by
         datetimeid/10000

"""


query_green = f"""
    SELECT
         datetimeid/10000 as year_month,
         avg(total_amount) as avg_total_fare, sum(COALESCE(total_amount,0)) as sum_total_fare,
         avg(fare_amount) as avg_fare_amount, sum(COALESCE(fare_amount,0)) as sum_fare_amount,
         AVG(NULLIF(extra,0)) as avg_extra, sum(COALESCE(extra,0)) as sum_extra,
         0.50 as mta_tax, sum(COALESCE(mta_tax,0)) as total_mta_tax,
         avg(NULLIF(tip_amount,0)) as avg_tip_amount, sum(COALESCE(tip_amount,0)) as sum_tip_amount,
         AVG(NULLIF(tolls_amount,0)) as avg_tolls_amount, sum(COALESCE(tolls_amount,0)) as sum_tolls_amount,
         0.30 as improvement_surcharge, sum(COALESCE(improvement_surcharge,0)) as sum_improvement_surcharge,
         AVG(NULLIF(congestion_surcharge,0)) as avg_congestion_surcharge, sum(COALESCE(congestion_surcharge,0)) as sum_congestion_surcharge,
         avg(trip_distance) as avg_distance,
        count(distinct hack_number) as count_drivers,
         avg(fare_amount) + avg(COALESCE(extra,0)) as driver_pay,
         sum(COALESCE(fare_amount,0)) + sum(COALESCE(extra,0)) as total_driver_pay,
         (sum(COALESCE(fare_amount,0)) + sum(COALESCE(extra,0)))/ count(distinct hack_number) as pay_per_driver,
         sum(COALESCE(tip_amount,0))/count(distinct hack_number) as tips_per_driver,
         sum(COALESCE(extra,0))/count(distinct hack_number) as extra_per_driver,
         sum(COALESCE(fare_amount,0))/count(distinct hack_number) as fare_per_driver
         FROM 'I:\\COF\\COF\\_DA&E_\\DA&E_DATABASE_PROJECT\\trip_repository\\lpep_monthly_trip_record\\*.parquet'
         where
         datetimeid >= 2014010100 and datetimeid < {end} and trip_distance > 0 and total_amount > 0 and total_amount < 250 and trip_distance < 100
         group by
         datetimeid/10000

"""





query_hvfhv = f"""
    SELECT
         datetimeid/10000 as year_month,
         avg(base_passenger_fare) as avg_base_passenger_fare, sum(COALESCE(base_passenger_fare,0)) as sum_base_passenger_fare,
          avg(tolls) as avg_tolls, sum(COALESCE(tolls,0)) as sum_tolls,
          avg(bcf) as avg_bcf, sum(COALESCE(bcf,0)) as sum_bcf,
        avg(sales_tax) as avg_sales_tax, sum(COALESCE(sales_tax,0)) as sum_sales_tax,
        avg(congestion_surcharge) as avg_congestion_surcharge, sum(COALESCE(congestion_surcharge,0)) as sum_congestion_surcharge,
        avg(tips) as avg_tips, sum(COALESCE(tips,0)) as sum_tips,
        avg(driver_pay) as avg_driver_pay, sum(COALESCE(driver_pay,0)) as sum_driver_pay,
        avg(airport_fee) as avg_airport_fee, sum(COALESCE(airport_fee,0)) as sum_airport_fee,
        avg(trip_miles) as avg_distance,
        count(distinct tlc_driver_license_num) as count_drivers,
        avg(driver_pay) as driver_pay,
        sum(driver_pay) as total_driver_pay,
        sum(driver_pay)/count(distinct tlc_driver_license_num) as pay_per_driver,
        sum(COALESCE(tips,0))/count(distinct tlc_driver_license_num) as tips_per_driver,
        sum(COALESCE(tips,0))/count(distinct tlc_driver_license_num) as tips_per_driver,
        sum(COALESCE(base_passenger_fare,0)) + sum(COALESCE(tolls,0)) + sum(COALESCE(bcf,0)) + sum(COALESCE(sales_tax,0)) + sum(COALESCE(congestion_surcharge,0)) + sum(COALESCE(tips,0)) + sum(COALESCE(airport_fee,0)) as sum_total_fare,
        avg(base_passenger_fare) + avg(tolls) + avg(bcf) + avg(sales_tax) + avg(congestion_surcharge) + avg(tips) + avg(airport_fee) as avg_total_fare
         FROM 'I:\\COF\\COF\\_DA&E_\\DA&E_DATABASE_PROJECT\\trip_repository\\hvfhv_monthly_trip_record\\*.parquet'
         where
         datetimeid >= 2019020100 and datetimeid < {end} and trip_miles > 0 and base_passenger_fare > 0 and base_passenger_fare < 250 and trip_miles < 100
         group by
         datetimeid/10000

"""

query_shared = f"""
    SELECT
         datetimeid/10000 as year_month,
         avg(base_passenger_fare) as avg_base_passenger_fare, sum(COALESCE(base_passenger_fare,0)) as sum_base_passenger_fare,
          avg(tolls) as avg_tolls, sum(COALESCE(tolls,0)) as sum_tolls,
          avg(bcf) as avg_bcf, sum(COALESCE(bcf,0)) as sum_bcf,
        avg(sales_tax) as avg_sales_tax, sum(COALESCE(sales_tax,0)) as sum_sales_tax,
        avg(congestion_surcharge) as avg_congestion_surcharge, sum(COALESCE(congestion_surcharge,0)) as sum_congestion_surcharge,
        avg(tips) as avg_tips, sum(COALESCE(tips,0)) as sum_tips,
        avg(driver_pay) as avg_driver_pay, sum(COALESCE(driver_pay,0)) as sum_driver_pay,
        avg(airport_fee) as avg_airport_fee, sum(COALESCE(airport_fee,0)) as sum_airport_fee,
        avg(trip_miles) as avg_distance,
        count(distinct tlc_driver_license_num) as count_drivers,
        avg(driver_pay) as driver_pay,
        sum(driver_pay) as total_driver_pay,
        sum(driver_pay)/count(distinct tlc_driver_license_num) as pay_per_driver,
        sum(COALESCE(tips,0))/count(distinct tlc_driver_license_num) as tips_per_driver,
        sum(COALESCE(tips,0))/count(distinct tlc_driver_license_num) as tips_per_driver,
        sum(COALESCE(base_passenger_fare,0)) + sum(COALESCE(tolls,0)) + sum(COALESCE(bcf,0)) + sum(COALESCE(sales_tax,0)) + sum(COALESCE(congestion_surcharge,0)) + sum(COALESCE(tips,0)) + sum(COALESCE(airport_fee,0)) as sum_total_fare,
        avg(base_passenger_fare) + avg(tolls) + avg(bcf) + avg(sales_tax) + avg(congestion_surcharge) + avg(tips) + avg(airport_fee) as avg_total_fare
 FROM 'I:\\COF\\COF\\_DA&E_\\DA&E_DATABASE_PROJECT\\trip_repository\\hvfhv_monthly_trip_record\\*.parquet'
         where route_id is not NULL and
         datetimeid >= 2019020100 and datetimeid < {end} and trip_miles > 0 and base_passenger_fare > 0 and base_passenger_fare < 250 and trip_miles < 100
         group by
         datetimeid/10000

"""


# The queries to pull the data that will feed the financial part of the DataHub End ----------------------


# Reading the data, basic formating, reshaping, and saving Start ----------------------------------
yellow_fare = pd.read_sql_query(query_yellow,con)
green_fare = pd.read_sql_query(query_green,con)
hvfhv_fare = pd.read_sql_query(query_hvfhv,con)
#shared_fare = pd.read_sql_query(query_shared,mydb)

yellow_fare['year_month'] = pd.to_datetime(yellow_fare['year_month'], format='%Y%m')
yellow_fare['year_month'] = yellow_fare['year_month'].dt.date

green_fare['year_month'] = pd.to_datetime(green_fare['year_month'], format='%Y%m')
green_fare['year_month'] = green_fare['year_month'].dt.date

hvfhv_fare['year_month'] = pd.to_datetime(hvfhv_fare['year_month'], format='%Y%m')
hvfhv_fare['year_month'] = hvfhv_fare['year_month'].dt.date

yellow_fare.to_csv(data_destination +'yellow_fare.csv')
green_fare.to_csv(data_destination +'green_fare.csv')
hvfhv_fare.to_csv(data_destination +'hvfhv_fare.csv')


yellow_fare_melt = pd.melt(yellow_fare, id_vars=['year_month'])
#yellow_fare_melt = fin.rename({'variable':'metric', 'value':'count'}, axis=1)
green_fare_melt = pd.melt(green_fare, id_vars=['year_month'])
hvfhv_fare_melt = pd.melt(hvfhv_fare, id_vars=['year_month'])

yellow_fare_melt.to_csv(data_destination +'yellow_fare_melt.csv')
green_fare_melt.to_csv(data_destination +'green_fare_melt.csv')
hvfhv_fare_melt.to_csv(data_destination +'hvfhv_fare_melt.csv')

# Reading the data, basic formating, reshaping, and saving End ----------------------------------
