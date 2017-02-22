package CloudComputing.TeamServer;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.json.JSONArray;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class HBaseQuery2 {

    private String zkAddr = "";
    private TableName tableName = TableName.valueOf("q2tweets");
    private Table table;
    private Connection conn;
    private byte[] bColFamily = Bytes.toBytes("data");
    private final Logger LOGGER = Logger.getRootLogger();
    private byte[] bCol = Bytes.toBytes("content");
    private byte[] bCol1 = Bytes.toBytes("wordCount");

    public HBaseQuery2(){
        try{
            initializeConnection();
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    /**
     * Initialize HBase connection.
     * @throws IOException
     */
    private void initializeConnection() throws IOException {
        // Remember to set correct log level to avoid unnecessary output.
        LOGGER.setLevel(Level.ERROR);
        if (!zkAddr.matches("\\d+.\\d+.\\d+.\\d+")) {
            System.out.print("Malformed HBase IP address");
            System.exit(-1);
        }
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", zkAddr + ":16000");
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.set("hbase.zookeeper.property.clientport", "2181");
        conn = ConnectionFactory.createConnection(conf);

        table = conn.getTable(tableName);
    }

    /**
     * @param userId
     * @return JSONArray which contains all the tweet of input user
     */
    public JSONArray query(String userId) throws IOException{

        Result results = table.get(new Get(userId.getBytes()));
	    String tweet = new String(results.getValue(bColFamily,bCol), StandardCharsets.UTF_8);
	    String wordCount = new String(results.getValue(bColFamily,bCol1), StandardCharsets.UTF_8);
        String[] tweets = tweet.split("!");

        JSONArray array = new JSONArray();
        for (String tw : tweets){
            String[] content = tw.split(",");
            JSONObject obj = new JSONObject();
            obj.put("tweetId", content[0]);
            obj.put("createdAt", content[1]);
            obj.put("censoredText", content[2]);
            obj.put("impactScore", content[4]);
            obj.put("wordCount", wordCount);
            array.put(obj);
        }
        
        return array;
    }
}
