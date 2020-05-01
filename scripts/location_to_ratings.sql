disable 'yelp:business_location'
drop 'yelp:business_location'
create 'yelp:business_location', 'rating', 'isopen', 'reviewcount','categories'


REGISTER /usr/hdp/current/pig-client/piggybank.jar;

DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;

RAW_BUSINESS_TABLE = LOAD '/user/root/yelp_data/yelp_business.csv' USING CSVExcelStorage() AS (
    businessid:chararray,
    name:chararray,
    neighborhood:chararray,
    address:chararray,
    city:chararray,
    state:chararray,
    postalcode:chararray,
    latitude:float,wq
    longitude:float,
    stars:float,
    reviewcount:float,
    isopen:boolean,
    categories:chararray
);

C_LOCATION_VARIABLE = FOREACH RAW_BUSINESS_TABLE GENERATE
CONCAT(city, '_', state) AS rowkey:chararray
, TOMAP(businessid, stars) as rating:map[]
, TOMAP(businessid, isopen) as isopen:map[]
, TOMAP(businessid, reviewcount) as reviewcount:map[]
, TOMAP(businessid, categories) as categories:map[];


STORE C_LOCATION_VARIABLE INTO 'hbase://yelp:business_location'
USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('rating:* isopen:* reviewcount:* categories:*');
