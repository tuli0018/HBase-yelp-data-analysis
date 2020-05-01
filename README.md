# HbaseAnalytics

### Team ADVanced - Daniel, Vansh, Ahmed


## Script (for video)
Hbase is difficult to perform analytics with standalone. We found that we needed to introduce an application layer to analyze code. 

We transform our data from CSV data sources through PIG (through grunt). From this mechanism, we can bulk insert data.

We insert the data based on group keys. What is meant by group keys is that we find some metric that a group within the data shares a key for. This can include locations or business names. In our data set, there may be multiple businesses with the same name (e.g. McDonald's), or there may be multiple businesses at a location (e.g. Las Vegas NV).

By performing these groupings in our row keys, we can have a fast lookup mechanism for these groups. Granted Hbase is not good with JOINs, but it is significantly well with fast look ups on row keys. A normal SQL database may have to scan a whole table and filter out by certain values. However, in Hbase, if we make these values keys, then we can easily obtain certain groups.

