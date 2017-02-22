import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class PredictorTest {
	public static String zkAddr = "172.31.15.84";
    public static class predictorMapper
            extends Mapper<Object, Text, Text, Text>{
        private Text word = new Text();
        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
        	Text result = new Text();
        	String line = value.toString().toLowerCase();  // [phrase]	[phrase count]
        	String[] lineArray = line.split("\t");
        	String content = lineArray[0]; // phrase
        	String lastWord = content.substring(content.lastIndexOf(" ") + 1);
        	String newPhase;
        	int count = Integer.parseInt(lineArray[1]);
        	 // set output count
	        if(lastWord.equals(content)){ // only one token
	       		word.set(content);
	       		newPhase = Integer.toString(count);
	       		result.set(newPhase);
	       		context.write(word, result); //[content]\t[count]
	       	}else{ // more than one token
	       		String truncated = content.replace(lastWord, "");
	       		word.set(truncated);
	       		newPhase = lastWord + "#" + Integer.toString(count);
	       		result.set(newPhase);
	       		context.write(word, result); // [truncated]\t[lastWord]\t[count]        		
	       		word.set(content);
	       		newPhase = Integer.toString(count);
	       		result.set(newPhase);
	       		context.write(word, result); //[content]\t[count]
	       	}
        }
    }

    public static class IntSumReducer
            extends TableReducer<Text,Text,ImmutableBytesWritable> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
        		//find total count and rowkey
        	int totalCount = 1;
        	double count;
        	Map<String, Double> map = new HashMap<String, Double>();
        	for(Text val: values){
        		String word = val.toString();
        		String[] wordArray = word.split("#");
        		if(wordArray.length == 1){
        			totalCount = Integer.parseInt(wordArray[0]);
        		}else{
        			String lastWord = wordArray[0];
        			count = Double.parseDouble(wordArray[1]);
            		map.put(lastWord, count);
        		}
        	}
        	for(String newKey: map.keySet()){
        		double prob = 1.0*map.get(newKey)/totalCount;
        		map.put(newKey, prob);
        	}
        	
            //Sort result; refered from references[2]
            List<Map.Entry<String, Double>> list = new LinkedList<Map.Entry<String, Double>>(map.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Double>>(){
            		
            	public int compare(Map.Entry<String, Double> a, Map.Entry<String, Double> b){
            		if(a.getValue() == b.getValue()){
            			return a.getKey().compareTo(b.getKey());
            		}else{
            			return b.getValue().compareTo(a.getValue());
            		}
            	}
            });
            	
            Put put = new Put(Bytes.toBytes(key.toString()));
           	count = 0;
           	int numberOfWords = context.getConfiguration().getInt("numberOfWords", 5);
           	String columnFamily = context.getConfiguration().get("columnFamily");
           	for(Map.Entry<String, Double> entry: list){
           		if(count < numberOfWords){
           			count++;
           			put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue().toString()));
           			context.write(null, put);
           		}
           	}
           		
           	
        }
    }
        
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "NgramCount");
        job.setJarByClass(NgramCount.class);
        job.setMapperClass(predictorMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}