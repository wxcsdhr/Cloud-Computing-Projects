// ETL code for q1 //
CREATE database task1;
USE task1;
CREATE TABLE users( user_id INT NOT NULL, password text NOT NULL, PRIMARY KEY ( user_id ) );
CREATE TABLE usersinfo( user_id INT NOT NULL, user_name text NOT NULL, user_profile text NOT NULL, PRIMARY KEY ( user_id ) );
LOAD DATA LOCAL INFILE 'users.csv' INTO TABLE users FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';
LOAD DATA LOCAL INFILE 'userinfo.csv' INTO TABLE usersinfo FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';

//ETL code for q2 //
// create new data structure for q2//
awk 'BEGIN{FS=","; OFS="|";} {a[$1] = a[$1] $2 OFS} END {for(i in a) print i FS a[i]}' links.csv > followee_follower.csv
scp -i Project33.pem Subtask1/xiaoche1/links2.csv hadoop@ec2-52-23-162-21.compute-1.amazonaws.com://home/hadoop/Project3_3/
//hbase command line//
mkdir Project3_3
cd Project3_3/
hadoop fs -put followee_follower_info.csv followee_follower_info.csv
hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.columns="HBASE_ROW_KEY,links:user_name,links:profile, links:follower" -Dimporttsv.separator="," task2 followee_follower_info.csv
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles followee_follower_info.csv task2

//ETL code for q3//
mongoimport --db task3 --collection posts --drop --file posts.json
use task3
db.posts.createIndex({"timestamp":1})

//ETL code for q4//
//create data structure for q4//
awk 'BEGIN {FS = ","; OFS=","} FNR==NR{a[$1] = $2 ; next} {print $1, $2, $3, $4, a[$1]}' follower_followee.csv followee_follower_info.csv > final_info.csv
//hbase command line for q4//
hadoop fs -put final_info.csv final_info.csv
hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.columns="HBASE_ROW_KEY,links:user_name,links:profile,links:follower,links:followee" -Dimporttsv.separator="," task4 final_info.csv




