package CloudComputing.ETL;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.InputStreamReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
        DBConfiguration db = new DBConfiguration(conf);
        DBConfiguration.configureDB(conf,
                "com.mysql.jdbc.Driver", //driver class
                "jdbc:mysql://54.152.116.56:3306/phase3?useSSL=false",// db url
                "root", // username
                "craig300");  // password
        Job job = new Job(conf, "MySQL");
        FileInputFormat.addInputPath(job, new Path(args[1]));
        job.setJarByClass(MRMySQL.class);
        job.setMapperClass(MySQLMap.class);
        job.setReducerClass(MySQLReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(DBOutputFormat.class);
        DBOutputFormat.setOutput(job, "q3tweets", new String[] {"tweetId","userId","createdAt", "wc"});
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class MySQLMap extends Mapper<Object, Text, Text, Text>{
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
            }catch(Exception e){
                System.out.println("Static initialization exception");
                e.printStackTrace();
            }
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
            String[] textArray = text.replaceAll(REGEX2, "").toLowerCase().split(REGEX1);
            JSONObject wordCount = findWordCount(textArray);
            if(wordCount.length() == 0){
                return;
            }
            output.put("wc", wordCount.toString());
            output.put("userId", userId);
            output.put("createdAt", timeStamp);
            word.set(tweetId);
            context.write(word, new Text(output.toString()));
        }
        
        public static String extractDate(String timeStamp) throws ParseException{
            
            Date formerDate = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy").parse(timeStamp);
            SimpleDateFormat currFormate = new SimpleDateFormat("yyyyMMdd");
            return currFormate.format(formerDate);
        }
        
        public static JSONObject findWordCount(String[] textArray) {
            Map<String, Integer> map = new HashMap<String, Integer>();
            String currentWord;
            JSONObject output = new JSONObject();
            for (int i = 0; i < textArray.length; i++) {
                currentWord = textArray[i];
                if(ignoreWords.contains(currentWord)){
                	continue;
                }
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
        private Long userId;
        private int createdAt;
        private String wc;

        public DBOutputWritable(Long tweetId, Long userId, int createdAt, String wc){
            this.tweetId = tweetId;
            this.userId = userId;
            this.createdAt = createdAt;
            this.wc = wc;
        }

        public void readFields(ResultSet resultSet) throws SQLException{
            this.tweetId = resultSet.getLong(1);
            this.userId = resultSet.getLong(2);
            this.createdAt = resultSet.getInt(3);
            this.wc = resultSet.getString(4);
        }

        public void readFields(DataInput in) throws IOException {
            this.tweetId = in.readLong();
            this.userId = in.readLong();
            this.createdAt = in.readInt();
            this.wc = in.readUTF();
        }

        public void write(PreparedStatement ps) throws SQLException{
            ps.setLong(1, tweetId);
            ps.setLong(2, userId);
            ps.setInt(3, createdAt);
            ps.setString(4, wc);
        }

        public void write(DataOutput out) throws IOException{
            out.writeLong(this.tweetId);
            out.writeLong(this.userId);
            out.writeInt(this.createdAt);
            out.writeUTF(this.wc);
        }
    }

    public static class MySQLReduce extends Reducer<Text, Text, DBOutputWritable, NullWritable>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            JSONObject entries = null;
            Long userId = 0L;
            Long tweetId = Long.valueOf(key.toString());
            String wordCount = null;
            int createdAt = 0;
            for (Text value : values){
                entries = new JSONObject(value.toString());
                userId = Long.valueOf(entries.getString("userId"));
                createdAt = Integer.valueOf(entries.getString("createdAt"));
                wordCount = entries.getString("wc");
                context.write(new DBOutputWritable(tweetId, userId, createdAt, wordCount),NullWritable.get());
                break;
            }
        }
    }
}




