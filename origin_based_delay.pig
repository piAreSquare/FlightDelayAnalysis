--load data from local file to pig in fdata var.
fdata = load '/home/hduser/Desktop/flight.csv' USING PigStorage(',') as (Month:int,DayofMonth:int,DayOfWeek:int,DepTime:int,CRSDepTime:int,ArrTime:int,CRSArrTime:int,FlightNum:int,ActualElapsedTime:int,CRSElapsedTime:int,AirTime:int,ArrDelay:int,DepDelay:int,Origin:chararray,Dest:chararray,Distance:int,TaxiIn:int,TaxiOut:int,CarrierDelay:int,WeatherDelay:int,NASDelay:int,LateAircraftDelay:int,TotalDelay:int);



--choose columns Origin,CRSDepTime and DepDelay in var delay
delay  = DISTINCT( foreach fdata generate Origin,CRSDepTime,DepDelay);



--now filter delay for DepDelay>0
delay  = filter delay by (DepDelay >0);


--remove DepDelay col
delay  = DISTINCT( foreach delay generate Origin,CRSDepTime);

--day flights
day = FILTER delay BY CRSDepTime>600 and CRSDepTime < 1800;

--night flights
night = FILTER delay BY CRSDepTime>0 and CRSDepTime<=600 or CRSDepTime>=1800 and CRSDepTime <= 2400;

--group day by origin
grp_day = group day by Origin;

--group night by origin
grp_night = group night by Origin;

--count no. of flights in each group of grp_day
count_day = foreach grp_day generate FLATTEN(group), COUNT($1);

--count no. of flights in each group of grp_night
count_night = foreach grp_night generate FLATTEN(group), COUNT($1);

--rename fields of count_day
count_day  = foreach count_day generate $0 as origin ,$1 as day_count;

--rename fields of count_night
count_night  = foreach count_night generate $0 as origin ,$1 as night_count;

--join count day,count night
count = join count_day by origin, count_night by origin;

-- formatting count as origin,day_count,night_count
count  = foreach count generate count_day::origin as origin,count_day::day_count as day_count,count_night::night_count as night_count;

--display results
dump count;

--store results in file
STORE count INTO '/home/hduser/Documents/pig/results' USING org.apache.pig.piggybank.storage.CSVExcelStorage('\t', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');




