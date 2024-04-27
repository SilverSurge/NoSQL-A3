-- Yago Problem 1b

-- step 1: load the tago dataset
yago_dataset = LOAD '../../data/yago.tsv' USING PigStorage(' ') AS (subject:chararray, predicate:chararray, object:chararray);

-- step 2: filter the dataset for livesin
filter_livesin = FILTER yago_dataset BY predicate == '<livesIn>';
grouped_livesin = GROUP filter_livesin BY subject;
count_livesin = FOREACH grouped_livesin GENERATE group AS subject, COUNT(filter_livesin) as count;
more_than_one_livesin = FILTER count_livesin BY count > 1;

-- step 3: filter the dataset for hasgivenname
filter_hasgivenname = FILTER yago_dataset BY predicate == '<hasGivenName>';

-- step 4: join analysis
join_result = JOIN filter_hasgivenname BY subject, more_than_one_livesin BY subject;
final_result = FOREACH join_result GENERATE filter_hasgivenname::object;

-- step 4: store result
STORE final_result INTO 'part-b-result';;
