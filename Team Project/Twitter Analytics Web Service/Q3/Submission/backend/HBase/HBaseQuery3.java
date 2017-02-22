package CloudComputing.TeamServer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import io.vertx.core.MultiMap;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HBaseQuery3 {

    private String zkAddr = "172.31.17.1";
    private TableName tableName = TableName.valueOf("tweets");
    private Table table;
    private Connection conn;
    private final Logger LOGGER = Logger.getRootLogger();

    public HBaseQuery3(){
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
    public String query(MultiMap map) throws IOException{

        String prefix1 = map.get("p1"), prefix2 = map.get("p2"), prefix3 = map.get("p3");
        String prefix1l = prefix1.toLowerCase(), prefix2l = prefix2.toLowerCase(), prefix3l = prefix3.toLowerCase();

        byte[] bColFamily = Bytes.toBytes("content");
        byte[] bColUid = Bytes.toBytes("uid");
        byte[] bColDate = Bytes.toBytes("date");
        //byte[] bColTid = Bytes.toBytes("tid");
        byte[] bColWordCount = Bytes.toBytes("wordCount");
        
        byte[] bStartUid = Bytes.toBytes(map.get("uid_start"));
        byte[] bEndUid = Bytes.toBytes(map.get("uid_end"));
        byte[] bStartDate = Bytes.toBytes(map.get("date_start"));
        byte[] bEndDate = Bytes.toBytes(map.get("date_end"));
        byte[] bTweetStartId = Bytes.toBytes(map.get("tid_start"));
        byte[] bTweetEndId = Bytes.toBytes(map.get("tid_end"));

        String startRK = buildRowKey(map.get("date_start"), map.get("tid_start"));
        String endRK = buildRowKey(map.get("date_end"),  map.get("tid_end"));

        Scan scan = new Scan(Bytes.toBytes(startRK), Bytes.toBytes(endRK));
        //scan.addColumn(bColFamily, bColUid);
        //scan.addColumn(bColFamily, bColDate);
        scan.addColumn(bColFamily, bColWordCount);
        //scan.addColumn(bColFamily, bColTid);

        FilterList filter = new FilterList();
        //filter.addFilter(new RowFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(bTweetStartId)));
        //filter.addFilter(new RowFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator(bTweetEndId)));
        filter.addFilter(new SingleColumnValueFilter(bColFamily, bColUid, CompareOp.GREATER_OR_EQUAL, bStartUid));
        filter.addFilter(new SingleColumnValueFilter(bColFamily, bColUid, CompareOp.LESS_OR_EQUAL, bEndUid));
        //filter.addFilter(new SingleColumnValueFilter(bColFamily, bColDate, CompareOp.GREATER_OR_EQUAL, bStartDate));
        //filter.addFilter(new SingleColumnValueFilter(bColFamily, bColDate, CompareOp.LESS_OR_EQUAL, bEndDate));
        
        scan.setFilter(filter);

        StringBuilder result = new StringBuilder();
        Map<String, Integer> countMap = new HashMap<>();
        countMap.put(prefix1, 0);
        countMap.put(prefix2, 0);
        countMap.put(prefix3, 0);

        ResultScanner rs = table.getScanner(scan);

        for (Result r = rs.next(); r != null; r = rs.next()) {

            String[] words = Bytes.toString(r.getValue(bColFamily, bColWordCount)).split(":");

            for (int j = 0;j < words.length - 1;j+=2){
                String word = words[j];
                int value = Integer.parseInt(words[j+1].trim());

                if (word.startsWith(prefix1l)){
                    countMap.put(prefix1, countMap.get(prefix1) + value);
                }
                else if (word.startsWith(prefix2l)){
                    countMap.put(prefix2, countMap.get(prefix2) + value);
                }
                else if (word.startsWith(prefix3l)){
                    countMap.put(prefix3, countMap.get(prefix3) + value);
                }
            }
        }
        
        result.append(prefix1 + ":" + countMap.get(prefix1) + "\n")
        	  .append(prefix2 + ":" + countMap.get(prefix2) + "\n")
        	  .append(prefix3 + ":" + countMap.get(prefix3));
        
        return result.toString();
    }

    private String buildRowKey(String date, String tid){
        StringBuilder sb = new StringBuilder();
        sb.append(date.replaceAll("-","").substring(3));

        int diff = 18 - tid.length();
        while(diff-- > 0)
            sb.append(0);

        sb.append(tid);
        return sb.toString();
    }
}
