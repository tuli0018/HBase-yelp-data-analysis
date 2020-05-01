disable 'yelp:business_cities'
drop 'yelp:business_cities'
create 'yelp:business_cities', 'business_name'



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


C_BUSINESS_CITIES = FOREACH RAW_BUSINESS GENERATE CONCAT(city, '_', state) AS rowkey:chararray, TOMAP(businessid, name) as business_name:map[];


STORE C_BUSINESS_CITIES INTO 'hbase://yelp:business_cities' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('business_name:*');

scan 'yelp:business_cities', {'LIMIT' => 10}

scan 'yelp:business_cities', {FILTER => "PrefixFilter('Las Vegas_NV')"}
