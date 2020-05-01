disable 'yelp:business_name'
drop 'yelp:business_name'
create 'yelp:business_name','rating','isOpen','reviewCount','categoryString'

REGISTER /usr/hdp/current/pig-client/piggybank.jar;

DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;

RAW_BUSINESS = LOAD '/user/root/yelp_data/yelp_business.csv' USING CSVExcelStorage() AS (
    businessid:chararray,
    name:chararray,
    neighborhood:chararray,
    address:chararray,
    city:chararray,
    state:chararray,
    postalcode:chararray,
    latitude:float,
    longitude:float,
    stars:float,
    reviewcount:float,
    isopen:boolean,
    categories:chararray
);

C_BUSINESS = FOREACH RAW_BUSINESS GENERATE 
name AS rowkey:chararray
, TOMAP(businessid, stars) as rating:map[]
, TOMAP(businessid, isopen) as isOpen:map[]
, TOMAP(businessid, reviewcount) as reviewCount:map[]
, TOMAP(businessid, categories) as categoryString:map[];


STORE C_BUSINESS INTO 'hbase://yelp:business_name'
USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('rating:* isOpen:* reviewCount:* categoryString:*');
