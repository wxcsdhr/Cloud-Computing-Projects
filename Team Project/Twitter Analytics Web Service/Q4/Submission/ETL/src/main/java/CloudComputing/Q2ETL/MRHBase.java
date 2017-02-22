import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.JSONArray;
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
    private static String zkAddr = "172.31.0.155";
    private static String tablename = "q2tweets";
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
        private static List<String> ROT13List = new ArrayList<String>();
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
                    ROT13List.add(String.valueOf(currentArray));
                }
                br.close();
            }catch(IOException e){
                System.out.println("Static initialization exception");
                e.printStackTrace();
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            JSONObject entry = new JSONObject(line);
            String userId = entry.getString("userId");
            String text = entry.getString("text");
            entry.remove("text");
            String[] textArray = text.replaceAll(REGEX2, "").toLowerCase().split(REGEX1);
            String wordCount = findWordCount(textArray);

            if(wordCount.length() == 0){
                return;
            }

            String censoredResults = findCensoreText(text, ROT13List);
            //if there is no word in the content, continue
            if(censoredResults.length() == 0){
                return;
            }
            entry.put("censoredtext", censoredResults);
            entry.put("wordCount", wordCount);
            word.set(userId);
            context.write(word, new Text(entry.toString()));
        }

        public static String findWordCount(String[] textArray) {
            Map<String, Integer> map = new HashMap<String, Integer>();
            String currentWord;
            StringBuilder output = new StringBuilder();
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
                output.append(entry.getKey());
                output.append(":");
                output.append(entry.getValue());
                output.append(":");
            }
            if (output.length() == 0) {
                return "";
            } else {
                return output.substring(0, output.length() - 1);
            }
        }

        public static String findCensoreText(String text, List<String> ROT13List) throws IOException {
            String tempText = text.toLowerCase();
            String[] tempTextArray = tempText.split(REGEX1);
            int stringLength = 0, startIndex, endIndex = 0;
            String subString = null, subReplace = null, stringRegex = null;

            for (String s : tempTextArray) {
                if(s.length() == 0){
                    continue;
                }
                startIndex = -1;
                stringRegex = "(?<=^|[^a-zA-Z0-9])" + s + "(?=[^a-zA-Z0-9]|$)";
                stringLength = s.length();
                Pattern pattern = Pattern.compile(stringRegex);
                Matcher matcher = pattern.matcher(tempText);
                if (matcher.find()) {
                    startIndex = matcher.start();
                }
                if (ROT13List.contains(s) && startIndex > -1) {
                    endIndex = startIndex + stringLength;
                    if (endIndex == text.length() + 1) {
                        startIndex--;
                        endIndex--;
                    }

                    subString = "(?<=^|[^a-zA-Z0-9])"
                            + text.substring(startIndex, endIndex)
                            + "(?=[^a-zA-Z0-9]|$)";

                    subReplace = text.charAt(startIndex)
                            + String.valueOf(new char[stringLength - 2]).replaceAll("\0", "*")
                            + text.charAt(endIndex - 1);

                    text = text.replaceAll(subString, subReplace); //still troublesome
                    tempText = text.toLowerCase();
                }
            }

            return text;
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

            Map<JSONObject, Integer> tweetInfo  = new HashMap<JSONObject, Integer>(); // used to record tweet info

            PriorityQueue<Map.Entry<JSONObject, Integer>> sortedResult = new PriorityQueue<Map.Entry<JSONObject, Integer>>(new Comparator<Map.Entry<JSONObject, Integer>>() {
                @Override
                public int compare(Entry<JSONObject, Integer> a, Entry<JSONObject, Integer> b) {
                    if(a.getValue().equals(b.getValue())){
                        long tweetIdA = Long.valueOf(a.getKey().getString("tweetId"));
                        long tweetIdB = Long.valueOf(b.getKey().getString("tweetId"));
                        return (int)(tweetIdA - tweetIdB);
                    }else{
                        return b.getValue() - a.getValue();
                    }
                }
            });

            PriorityQueue<Map.Entry<String, Integer>> sortedWordCount = new PriorityQueue<Map.Entry<String, Integer>>(new Comparator<Map.Entry<String, Integer>>(){
                @Override
                public int compare(Entry<String, Integer> a, Entry<String, Integer> b){
                    return a.getKey().compareTo(b.getKey());
                }
            });

            Map<String, Integer> wordCountTotal  = new HashMap<String, Integer>(); // used to record total wordCount

            for (Text value : values){
                JSONObject entries = new JSONObject(value.toString());
                String[] wordCountArray = entries.getString("wordCount").split(":");
                entries.remove("wordCount");
                entries.put("words", findAllWords(wordCountArray));
                tweetInfo.put(entries, entries.getInt("impactScore"));

                int currentCount = 0;
                String currentWord = null;
                if(wordCountArray.length > 1){
                    for(int i = 0; i < wordCountArray.length; i = i + 2){
                        currentWord = wordCountArray[i];
                        currentCount = Integer.valueOf(wordCountArray[i + 1]);
                        if(wordCountTotal.containsKey(currentWord)){
                            wordCountTotal.put(currentWord, currentCount + wordCountTotal.get(currentWord));
                        }else{
                            wordCountTotal.put(currentWord, currentCount);
                        }
                    }
                }
            }

            //sort and retrieve all word and count values;
            JSONObject wordCountJSON = new JSONObject();
            sortedWordCount.addAll(wordCountTotal.entrySet());
            while(!sortedWordCount.isEmpty()){
                Map.Entry<String, Integer> entry = sortedWordCount.poll();
                wordCountJSON.put(entry.getKey(), entry.getValue());
            }

            //sorted tweet info and retrieve
            JSONArray tweets = new JSONArray();
            sortedResult.addAll(tweetInfo.entrySet());
            while(!sortedResult.isEmpty()){
                Map.Entry<JSONObject, Integer> entry = sortedResult.poll();
                tweets.put(entry.getKey());
            }

            Put put = new Put(Bytes.toBytes(key.toString()));
            String columnFamily = context.getConfiguration().get("columnFamily");
            put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes("content"),Bytes.toBytes(tweets.toString()));
            put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes("wordCount"),Bytes.toBytes(wordCountJSON.toString()));
            context.write(null, put); 
        }
    }
}
