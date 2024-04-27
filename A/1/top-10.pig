-- YAGO Top Ten Predicates Analysis Script

-- step 1: load the tago dataset
yago_dataset = LOAD '../../data/yago.tsv' USING PigStorage(' ') AS (subject:chararray, predicate:chararray, object:chararray);

-- step 2: group the data by predicate
grouped_by_predicate = GROUP yago_dataset BY predicate;

-- step 3: count the occurences of each predicate
predicate_counts = FOREACH grouped_by_predicate GENERATE group AS predicate, COUNT(yago_dataset) AS count;

-- step 4: order by count and limit to top 10
ordered_predicates = ORDER predicate_counts by count DESC;
top_10_predicates = LIMIT ordered_predicates 10;

-- step 5: the results in a file
STORE top_10_predicates INTO 'part-a-result';;