trades = LOAD 's3://iyadamazon/data/tradesbigdata.txt' USING PigStorage('\t') AS (investor_id:int, trade_id:int, rating:int) ;

trades_2 = FOREACH trades GENERATE investor_id AS investor_id_2, trade_id AS trade_id_2, rating AS rating_2 ;
joinedtradeds = JOIN trades BY investor_id, trades_2 BY investor_id_2 ;
groupedtrades = group  joinedtradeds  by (trade_id,trade_id_2);
cooccurrence = FOREACH groupedtrades GENERATE 
		group.trade_id as trade_id,
		group.trade_id_2 as trade_id_2,
		COUNT($1) as tradecount;

filteredtradesforuser3 = filter trades BY investor_id == 3;
pre_product_matrix = JOIN cooccurrence BY trade_id_2, filteredtradesforuser3 BY trade_id ;
product_matrix = FOREACH pre_product_matrix GENERATE 
		$0 as trade_id,
		$1 as trade_id_2,
		(int)$2*$5 as user3ratingproduct;
grouped_product_matrix = group  product_matrix  by trade_id;	
	

result_Vector = FOREACH grouped_product_matrix GENERATE 
		$0 as trade_id,
		SUM(product_matrix.user3ratingproduct) as user3ratingtotal;
		
joinedrecommendations = JOIN result_Vector by trade_id LEFT, filteredtradesforuser3 BY trade_id;
filteredrecommendations = filter joinedrecommendations BY $2 is null;

user3recommendation = FOREACH filteredrecommendations GENERATE
$0 as trade_id,
$1 as recommendation;

user3recommendationsorted = order user3recommendation by recommendation desc;

store user3recommendationsorted into 's3://iyadamazon/results/tradesrecommendations.txt' using PigStorage('\t');