import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Predictor {
	public static String zkAddr = "172.31.25.83";
    public static class predictorMapper
            extends Mapper<Object, Text, Text, Text>{
        private Text word = new Text();
        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
        	Text result = new Text();
        	String line = value.toString().toLowerCase();  // [phrase]	[phrase count]
        	String[] lineArray = line.split("\t");
        	String content = lineArray[0].trim(); // phrase
        	int lastWordIndex = content.lastIndexOf(" ") + 1;
        	String lastWord = content.substring(lastWordIndex).trim();
        	String newPhase;
        	String truncated;
        	int count = Integer.parseInt(lineArray[1]);
        	if(lastWordIndex != 0){ // more than one tokens
        		truncated = content.substring(0, lastWordIndex).trim();
        		newPhase = lastWord + "#" + Integer.toString(count);
        		word.set(truncated);
            	result.set(newPhase);
    	       	context.write(word, result); // [truncated]\t[lastWord]\t[count]
        	}
        }
    }

    public static class IntSumReducer
            extends TableReducer<Text,Text,ImmutableBytesWritable> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
        		//find total count and rowkey
        	int totalCount = 0;
        	double count;
        	Map<String, Double> map = new HashMap<String, Double>();
        	for(Text val: values){
        		String word = val.toString();
        		String[] wordArray = word.split("#");
        		String lastWord = wordArray[0];
        		count = Double.parseDouble(wordArray[1]);
        		totalCount+=count;
           		map.put(lastWord, count);
        	}
        	for(String newKey: map.keySet()){
        		double prob = map.get(newKey)/(1.0*totalCount);
        		map.put(newKey, prob);
        	}
        	
            //Sort result; refered from references[2]
            List<Map.Entry<String, Double>> list = new LinkedList<Map.Entry<String, Double>>(map.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Double>>(){
            	@Override
            	public int compare(Map.Entry<String, Double> a, Map.Entry<String, Double> b){
            		double probA = a.getValue();
            		double probB = b.getValue();
            		String phraseA = a.getKey();
            		String phraseB = b.getKey();
            		if(probA == probB){
            			return phraseA.compareTo(phraseB);
            		}else{
            			return Double.compare(probB, probA);
            		}
            	}
            });
            	
            Put put = new Put(Bytes.toBytes(key.toString()));
           	count = 0;
           	int numberOfWords = context.getConfiguration().getInt("numberOfWords", 5);
           	String columnFamily = context.getConfiguration().get("columnFamily");
           	for(Map.Entry<String, Double> entry: list){
           		if(count < numberOfWords && !entry.getValue().equals(1.0) && entry.getKey().trim().length() > 0){
           			count++;
           			put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue().toString()));
           			context.write(null, put);
           		}
           	}
           		
           	
        }
    }
        
    public static void main(String[] args) throws Exception {
    	 // create table; cited from references[3]
    	Configuration hBaseConf = HBaseConfiguration.create();
    	hBaseConf.set("hbase.master", zkAddr + ":16000");
    	hBaseConf.set("hbase.zookeeper.quorum", zkAddr);
    	hBaseConf.set("hbase.zookeeper.property.clientport", "2181");
    	GenericOptionsParser optionParser  = new GenericOptionsParser(hBaseConf, args);
    	String[] remainingArgs = optionParser.getRemainingArgs();
    	int numberOfWords = Integer.parseInt(remainingArgs[1]);
    	String tableName = "wordPredictor";
    	String columnFamily = "wordProb";
    	hBaseConf.set("columnFamily", columnFamily);
    	hBaseConf.set("tableName", tableName);
    	hBaseConf.setInt("numberOfWords", numberOfWords);
    	HBaseAdmin admin = new HBaseAdmin(hBaseConf);
    	// create table;
    	if(admin.tableExists(tableName)){
    		admin.disableTable(tableName);
    		admin.deleteTable(tableName);
    	}
    	HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
    	// add column family
    	tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
    	admin.createTable(tableDescriptor);
    
        Job hBaseJob = Job.getInstance(hBaseConf, "Predictor");
        hBaseJob.setJarByClass(Predictor.class);
        hBaseJob.setMapperClass(predictorMapper.class);
        TableMapReduceUtil.initTableReducerJob(tableName, IntSumReducer.class, hBaseJob);
        hBaseJob.setMapOutputKeyClass(Text.class);
        hBaseJob.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(hBaseJob, new Path(args[0]));
        System.exit(hBaseJob.waitForCompletion(true) ? 0 : 1);
    }
}