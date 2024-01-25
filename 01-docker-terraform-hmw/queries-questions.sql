-- Question 3. Count records
-- How many taxi trips were totally made on September 18th 2019?
  select count(*)
  from green_taxi_data 
  where cast(lpep_pickup_datetime as date)='2019-09-18'
  and cast(lpep_dropoff_datetime as date)='2019-09-18'
  
-- Question 4. Largest trip for each day
  -- Which was the pick up day with the largest trip distance Use the pick up time for your calculations.
  SELECT date(lpep_pickup_datetime) pick_up_date
  from green_taxi_data 
  where trip_distance  = (select max(trip_distance) from green_taxi_data )


-- Question 5. Three biggest pick up Boroughs
--Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?
  select z."Borough" 
  	  ,sum(t.total_amount) as total_amount 
  from
  public.green_taxi_data as t
  inner join 
  zones as z 
  on t."PULocationID"  = z."LocationID" 
  where z."Borough"  <> 'Unknown'
  group by z."Borough" 
  having sum(t.total_amount) > 50000
  order by sum(t.total_amount)  desc 
  limit 3


--Question 6. Largest tip
/*
   For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone
that had the largest tip? We want the name of the zone, not the id.

*/
    select zdo."Zone" 
    		,max(t.tip_amount)
    from
    	public.green_taxi_data as t
    inner join 
    	zones zpu
    on  t."PULocationID"  = zpu."LocationID" 
    inner join 
    zones zdo
    on t."DOLocationID"  = zdo."LocationID" 
    where to_char(t.lpep_pickup_datetime, 'YYYY-MM') = '2019-09'
    and zpu."Zone"  = 'Astoria'
    group by zdo."Zone" 
    order by 2 desc







