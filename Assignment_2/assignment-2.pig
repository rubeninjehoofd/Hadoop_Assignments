-- use the CSV module
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage; 

-- load the csv file 
orderCSV = LOAD '/user/maria_dev/lec3/orders.csv' 
USING CSVExcelStorage() AS
(game_id:int,
unit_id:int,
unit_order:chararray,
location:chararray,
target:chararray,
target_dest:chararray,
success:int,
reason:int,
turn_num:int);

-- Get rows where Holland is the target, group it then by their locations. Count the how many times Holland was the target, then order the locations ascending.
filtered = FILTER orderCSV BY target == 'Holland';
grouped = GROUP filtered BY (location, target);
countGroupedRows = FOREACH grouped GENERATE group, COUNT(filtered);
orderedList = ORDER countGroupedRows BY $0 ASC;

-- print rows
DUMP orderedList;