package CloudComputing.Q4ETL;

import java.io.IOException;  
import java.util.PriorityQueue;
import java.util.Queue;
import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.hbase.HColumnDescriptor;  
import org.apache.hadoop.hbase.HTableDescriptor;  
import org.apache.hadoop.hbase.client.HBaseAdmin;  
import org.apache.hadoop.hbase.client.Put;  
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;  
import org.apache.hadoop.hbase.util.Bytes;  
import org.apache.hadoop.mapreduce.Mapper;

public class MRHBase
{
	private static String zkAddr = "172.31.22.104";
    private static String tablename = "q4tweets";
    private static String columnFamily = "data";
    
    public static void main( String[] args ) throws Exception
    {
    	//hbase configuration
        Configuration hbConfig = HBaseConfiguration.create();
        hbConfig.set(TableOutputFormat.OUTPUT_TABLE, tablename);
        hbConfig.set("hbase.master", zkAddr + ":16000");
        hbConfig.set("hbase.zookeeper.quorum", zkAddr);
        hbConfig.set("hbase.zookeeper.property.clientport", "2181");
        hbConfig.set("columnFamily", columnFamily);
        hbConfig.set("tableName", tablename);
        HBaseAdmin admin = new HBaseAdmin(hbConfig);
        // create table;
        if(admin.tableExists(tablename)){
            admin.disableTable(tablename);
            admin.deleteTable(tablename);
        }

        HTableDescriptor tableDescriptor = new HTableDescriptor(tablename);
        // add column family
        tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
        admin.createTable(tableDescriptor);
        Job job = new Job(hbConfig,"MRHBase");
        FileInputFormat.addInputPath(job, new Path(args[1]));   
        job.setJarByClass(MRHBase.class);  
        job.setMapperClass(HBaseMap.class);
        //set reducer write HBase
        TableMapReduceUtil.initTableReducerJob(tablename, HBaseReduce.class, job);
        job.setNumReduceTasks(512); 
        job.setMapOutputKeyClass(Text.class);  
        job.setMapOutputValueClass(Text.class);  
        System.exit(job.waitForCompletion(true)?0:1); 
    }
    
    public static void createHBaseTable(String tablename) throws IOException {  
        HTableDescriptor htd = new HTableDescriptor(tablename);  
        HColumnDescriptor col = new HColumnDescriptor("data");  
        htd.addFamily(col);  
        Configuration cfg = HBaseConfiguration.create();  
        HBaseAdmin admin = new HBaseAdmin(cfg);  
        
        //remove existing table
        if(admin.tableExists(tablename)) {  
            System.out.println("Table already exists, recreate now!");  
            admin.disableTable(tablename);  
            admin.deleteTable(tablename);  
        }
        System.out.println("Create new table:" + tablename);  
        admin.createTable(htd);  
        admin.close();
    }  
    
    public static class HBaseMap extends Mapper<Object, Text, Text, Text> {  
        private Text word = new Text();
        private Text content = new Text();
        
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JSONObject tweet = new JSONObject(value.toString());
            JSONObject output = new JSONObject();

            //remove malformed tweets
            if(!tweet.has("tweetId")){
                return;
            }

            //remove tweet without userId
            if(!tweet.has("userId")){
            	return;
            }
            
            //remove tweet without timestamp
            if(!tweet.has("createdAt")){
                return;
            }

            //do not have hashtags
            if(!tweet.has("hashtags")){
                return;
            }

            //do not have user name
            if(!tweet.has("userName")){
                return;
            }
            
            //do not have text content
            if(!tweet.has("text")){
            	return;
            }
            
            //get key
            String tweetId = tweet.getString("tweetId");
            tweet.remove("tweetId");

            word.set(tweetId);
            content.set(tweet.toString());
            context.write(word, content);
        }
    }

    public static class HBaseReduce extends TableReducer<Text, Text, ImmutableBytesWritable> {  
        @Override  
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException{ 
            for (Text t : values){
            	JSONObject entry = new JSONObject(t.toString());
            	String tweetId = key.toString();
            	String userId = entry.getString("userId");
            	String userName = entry.getString("userName");
            	String text = entry.getString("text");
            	String createdAt = entry.getString("createdAt");
            	JSONArray hashtags = entry.getJSONArray("hashtags");
                Put put = new Put(Bytes.toBytes(key.toString()));  
                put.addColumn(Bytes.toBytes("data"),Bytes.toBytes("tweetId"),Bytes.toBytes(tweetId));
                put.addColumn(Bytes.toBytes("data"),Bytes.toBytes("userId"),Bytes.toBytes(userId));
                put.addColumn(Bytes.toBytes("data"),Bytes.toBytes("userName"),Bytes.toBytes(userName));
                put.addColumn(Bytes.toBytes("data"),Bytes.toBytes("text"),Bytes.toBytes(text));
                put.addColumn(Bytes.toBytes("data"),Bytes.toBytes("createdAt"),Bytes.toBytes(createdAt));
                put.addColumn(Bytes.toBytes("data"),Bytes.toBytes("hashtags"),Bytes.toBytes(hashtags.toString()));
                context.write(null, put); 
            }
        }
    }
}
