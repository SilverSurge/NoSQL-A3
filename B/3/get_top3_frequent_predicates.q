SELECT predicate, COUNT(*)
FROM yago_table
GROUP BY predicate 
ORDER BY COUNT(*) DESC
LIMIT 3;
