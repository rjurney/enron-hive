create external table from_to(from_address string, to_address string, dt string) 
    row format delimited fields terminated by '\t' stored as textfile location 's3://enron.data/from.to.date';

// Get this from https://github.com/rjurney/timeseriesserde
add jar /home/hadoop/timeseriesserde.jar;
create temporary function TimeSeries as 'com.example.hive.udf.TimeSeries';

/* Sample 10 records */
select * from from_to limit 10;

/* Get 100 counts of emails sent between pairs of email addresses */
select from_address, 
       to_address, 
       count(*) as total 
       from from_to 
       group by from_address, to_address 
       order by total 
       desc limit 100;

/* Select 10 totals of emails sent per day overall */
select to_date(dt), count(*) as total from from_to group by to_date(dt) limit 10;

/* Use a custom Serde to create a time series of all emails sent each day.
   Get the Serde here: https://github.com/rjurney/timeseriesserde */
select to_date(dt) as dt,
       TimeSeries(CAST(count(*) AS INT)) as stars,
       count(*) as total
       from from_to
       group by to_date(dt)
       order by to_date(dt);

