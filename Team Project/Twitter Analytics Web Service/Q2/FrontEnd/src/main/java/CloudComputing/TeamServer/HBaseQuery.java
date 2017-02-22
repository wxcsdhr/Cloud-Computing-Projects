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
import java.util.List;


public class HBaseQuery {

    private String zkAddr = "172.31.55.29";
    private TableName tableName = TableName.valueOf("q2tweets");
    private Table table;
    private Connection conn;
    private byte[] bColFamily = Bytes.toBytes("data");
    private final Logger LOGGER = Logger.getRootLogger();
    private byte[] bColContent = Bytes.toBytes("content");
    private byte[] bColWC = Bytes.toBytes("wordCount");

    public HBaseQuery(){
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
    public JSONArray[] query(List<Get> ids) throws IOException{

        Result[] results = table.get(ids);
        JSONArray[] ret = new JSONArray[2];

        for (int idx = 0; idx <= 1; idx++){
            Result result = results[idx];
            JSONArray tweets = new JSONArray(new String(result.getValue(bColFamily,bColContent), StandardCharsets.UTF_8));
            JSONObject wordCount = new JSONObject(new String(result.getValue(bColFamily,bColWC), StandardCharsets.UTF_8));
            tweets.put(wordCount);

            ret[idx] = tweets;
        }
        
        return ret;
    }
}
