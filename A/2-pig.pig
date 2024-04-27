-- step 1: load the tago dataset
yago_dataset = LOAD '../data/yago.tsv' USING PigStorage(' ') AS (subject:chararray, predicate:chararray, object:chararray);
-- yago_dataset = LIMIT yago_dataset 100;

-- step 2: group all the data together
yago_all = GROUP yago_dataset ALL;

-- step 3: get the user defined function function
REGISTER 2.jar;
count = FOREACH yago_all GENERATE group as yago_all, MyUDF(yago_dataset, '<Ron_Mann>', '<directed>') AS count;
STORE count INTO 'group-all-udf-trial-16';

-- counts_2 = FOREACH yago_all GENERATE group as yago_all, MyUDF(yago_dataset, '<Dev_Anand>', '<directed>') AS count;
-- STORE counts_2 INTO 'group-all-udf-trial-15-b';;

-- 

