SELECT names.object
FROM (SELECT object FROM yago_table WHERE predicate="<hasGivenName>") as names
JOIN (SELECT subject from yago_table WHERE predicate="<livesIn>" GROUP BY subject HAVING count(*)>1) as temp
ON(temp.subject == names.object);
