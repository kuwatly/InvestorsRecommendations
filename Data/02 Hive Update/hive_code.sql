
CREATE TABLE trades (
  investor_id INT ,
  trade_id INT ,
  rating INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE ;
 
SHOW TABLES;
DESCRIBE trades;


LOAD DATA 
  LOCAL INPATH '/root/trades.txt'
  OVERWRITE INTO TABLE trades;
  
select * from trades;

CREATE TABLE cooccurrence AS
SELECT
  a.trade_id ,
  b.trade_id AS trade_id_2 ,
  COUNT(*) AS tradescount 
FROM
  trades a
  JOIN trades b ON a.investor_id = b.investor_id
GROUP BY 
  a.trade_id ,
  b.trade_id;

select * from cooccurrence;


CREATE TABLE product_matrix AS
SELECT
  a.trade_id ,
  a.trade_id_2 ,
 sum(b.rating*a.tradescount) as cooccurrencecount
FROM
cooccurrence a,
trades b
where
a.trade_id_2= b.trade_id
AND b.investor_id=3
group by 
a.trade_id,
a.trade_id_2;


select * from product_matrix;


CREATE TABLE result_Vector AS
select trade_id , sum(cooccurrencecount) as recommended
from product_matrix
group by trade_id
order by recommended DESC;


select * from result_Vector;

CREATE TABLE User3_recommendations AS
SELECT  DISTINCT a.trade_id, a.recommended
FROM    result_vector a
        LEFT JOIN trades b 
          ON a.trade_id = b.trade_id AND b.investor_id = 3
WHERE   b.rating IS NULL;


select * from User3_recommendations;
