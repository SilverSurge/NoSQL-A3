CREATE TABLE yago_table (
    subject STRING,
    predicate STRING,
    object STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
LOCATION '/my_hive_tables/yago_table'; 

