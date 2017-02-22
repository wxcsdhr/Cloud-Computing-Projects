package CloudComputing.Q4ETL;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
public class MRMySQL {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("mapred.reduce.child.java.opts", "-Xmx3072m");
        conf.set("mapred.task.timeout", "10000000");
        DBConfiguration.configureDB(conf,
                "com.mysql.jdbc.Driver", //driver class
                "jdbc:mysql://52.90.114.189:3306/phase3?useSSL=false&useUnicode=true",// db url
                System.getenv("DBUSER"), // username
                System.getenv("DBPWD"));  // password
        Job job = new Job(conf, "MySQL");
        FileInputFormat.addInputPath(job, new Path(args[1]));
        job.setJarByClass(MRMySQL.class);
        job.setMapperClass(MySQLMap.class);
        job.setReducerClass(MySQLReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(DBOutputFormat.class);
        DBOutputFormat.setOutput(job, "q4tweets", new String[] {"tweetid","timestamp", "hashtag", "userid","username","text"});
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class MySQLMap extends Mapper<Object, Text, Text, Text>{
        private Text word = new Text();
        private Text content = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	JSONObject tweet = new JSONObject(value.toString());

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

    public static class DBOutputWritable implements Writable, DBWritable{
        private Long tweetId;
        private String createdAt;
        private String hashtags;
        private Long userId;
        private String userName;
        private String text;

        public DBOutputWritable(Long tweetId, String createdAt, String hashtags, Long userId, String userName, String text){
            this.tweetId = tweetId;
            this.createdAt = createdAt;
            this.hashtags = hashtags;
            this.userId = userId;
            this.userName = userName;
            this.text = text;
        }

        public void readFields(ResultSet resultSet) throws SQLException{
            this.tweetId = resultSet.getLong(1);
            this.createdAt = resultSet.getString(2);
            this.hashtags = resultSet.getString(3);
            this.userId = resultSet.getLong(4);
            this.userName=  resultSet.getString(5);
            this.text = resultSet.getString(6);
        }

        public void readFields(DataInput in) throws IOException {
            this.tweetId = in.readLong();
            this.createdAt = in.readUTF();
            this.hashtags = in.readUTF();
            this.userId = in.readLong();
            this.userName = in.readUTF();
            this.text = in.readUTF();
        }

        public void write(PreparedStatement ps) throws SQLException{
            ps.setLong(1, tweetId);
            ps.setString(2, createdAt);
            ps.setString(3, hashtags);
            ps.setLong(4, userId);
            ps.setString(5, userName);
            ps.setString(6, text);
        }

        public void write(DataOutput out) throws IOException{
            out.writeLong(this.tweetId);
            out.writeUTF(this.createdAt);
            out.writeUTF(this.hashtags);
            out.writeLong(this.userId);
            out.writeUTF(this.userName);
            out.writeUTF(this.text);
        }
    }

    public static class MySQLReduce extends Reducer<Text, Text, DBOutputWritable, NullWritable>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            for (Text t : values){
            	JSONObject entry = new JSONObject(t.toString());
            	Long tweetId = Long.valueOf(key.toString());
            	Long userId = Long.valueOf(entry.getString("userId"));
            	String userName = entry.getString("userName");
            	String text = entry.getString("text");
            	String createdAt = entry.getString("createdAt");
            	JSONArray hashtags = entry.getJSONArray("hashtags");
            	StringBuilder sb = new StringBuilder();
            	for(int i = 0; i < hashtags.length(); i++){
            		sb.append(hashtags.get(i));
            		if(i != hashtags.length() - 1){
            			sb.append("\t");
            		}
            	}
            	String hashtag = sb.toString();
                context.write(new DBOutputWritable(tweetId, createdAt, hashtag, userId, userName, text),NullWritable.get());
                break;
            }
        }
    }
}
