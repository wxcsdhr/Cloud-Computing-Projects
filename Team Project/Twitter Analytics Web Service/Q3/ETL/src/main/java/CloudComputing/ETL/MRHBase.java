package CloudComputing.ETL;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import org.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
    private static String zkAddr = "172.31.28.53";
    private static String tablename = "q3tweets";
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

    
    public static class HBaseMap extends Mapper<Object, Text, Text, Text> {  

        private static final String REGEX1 = "[^a-zA-Z0-9]";
        private static final String REGEX2 = "(https?|ftp)://[^\\t\\r\\n /$.?#][^\\t\\r\\n ]*";
        private Text word = new Text();
        private static List<String> ignoreWords = new ArrayList<String>();
        static {
            try{
                Path pt = new Path("hdfs:///code/go_flux_yourself");
                FileSystem fs = FileSystem.get(new Configuration());
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
                String line = null;
                while((line = br.readLine()) != null){
                    char[] currentArray = line.toCharArray();
                    for(int i = 0; i < currentArray.length; i++){
                        char currentChar = currentArray[i];

                        if(currentChar > 96 && currentChar - 13 < 97){
                            currentArray[i] = (char)(26 + currentChar - 13);
                        }else if(currentChar > 96){
                            currentArray[i] = (char)(currentChar - 13);
                        }
                    }
                    ignoreWords.add(String.valueOf(currentArray));
                }
                br.close();
                
                pt = new Path("hdfs:///code/stopwords");
                fs = FileSystem.get(new Configuration());
                br = new BufferedReader(new InputStreamReader(fs.open(pt)));
                while((line = br.readLine()) != null){
                	ignoreWords.add(line.trim());
                }
            }catch(IOException e){
                System.out.println("Static initialization exception");
                e.printStackTrace();
            }
        }

        private String findTc(String tweetId, String timeStamp){
        	StringBuilder sb = new StringBuilder(timeStamp);
        	sb.append(tweetId);
        	return sb.toString();
        }
        
        public static String extractDate(String timeStamp) throws ParseException{
    		
    		Date formerDate = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy").parse(timeStamp);
    		SimpleDateFormat currFormate = new SimpleDateFormat("yyyyMMdd");
    		return currFormate.format(formerDate).substring(3,8);
    	}
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            JSONObject entry = new JSONObject(line);
            JSONObject output = new JSONObject();
            String userId = entry.getString("userId");
            String text = entry.getString("text");
            String createdAt = entry.getString("createdAt");
            String timeStamp = "";
            try {
				timeStamp = extractDate(createdAt);
			} catch (ParseException e) {
				e.printStackTrace();
			}
            String tweetId = entry.getString("tweetId");
            String tc = findTc(tweetId, timeStamp);
            String[] textArray = text.replaceAll(REGEX2, "").toLowerCase().split(REGEX1);
            JSONObject wordCount = findWordCount(textArray);
            if(wordCount.length() == 0){
                return;
            }
            output.put("wc", wordCount);
            output.put("userId", userId);
            word.set(tc);
            context.write(word, new Text(output.toString()));
        }

        public static JSONObject findWordCount(String[] textArray) {
            Map<String, Integer> map = new HashMap<String, Integer>();
            String currentWord;
            JSONObject output = new JSONObject();
            for (int i = 0; i < textArray.length; i++) {
                currentWord = textArray[i];
                if(currentWord.length() == 0){
                    continue;
                }
                if (map.containsKey(currentWord)) {
                    map.put(currentWord, map.get(currentWord) + 1);
                } else {
                    map.put(currentWord, 1);
                }
            }
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                output.put(entry.getKey(), entry.getValue());
            }
            return output;
        }

    }

    public static class HBaseReduce extends TableReducer<Text, Text, ImmutableBytesWritable> {  

        public static String findAllWords(String[] wordCount){
            StringBuilder result = new StringBuilder();
            for(int i = 0; i < wordCount.length - 1; i = i + 2){
                result.append(wordCount[i]);
                if(i != wordCount.length - 2){
                    result.append(",");
                }
            }
            return result.toString();
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException{ 

            JSONObject entries = new JSONObject();
            String userId = "";
            String tc = key.toString();
            JSONObject wordCount = new JSONObject();
            for (Text value : values){
                entries = new JSONObject(value.toString());
                userId = entries.getString("userId");
                wordCount = entries.getJSONObject("wc");
            }
            Put put = new Put(Bytes.toBytes(key.toString()));
            String columnFamily = context.getConfiguration().get("columnFamily");
            put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes("userId"),Bytes.toBytes(userId));
            put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes("wordCount"),Bytes.toBytes(wordCount.toString()));
            context.write(null, put); 
        }
    }
}
