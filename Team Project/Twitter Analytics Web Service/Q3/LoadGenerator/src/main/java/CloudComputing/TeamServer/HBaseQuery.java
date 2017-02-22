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
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import io.vertx.core.MultiMap;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class HBaseQuery {

    private String zkAddr = "172.31.51.12";
    private TableName tableName = TableName.valueOf("q3tweets");
    private Table table;
    private Connection conn;
    private final Logger LOGGER = Logger.getRootLogger();

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
//    public String query(MultiMap map) throws IOException{
//
//        String prefix1 = map.get("p1"), prefix2 = map.get("p2"), prefix3 = map.get("p3");
//        String prefix1l = prefix1.toLowerCase(), prefix2l = prefix2.toLowerCase(), prefix3l = prefix3.toLowerCase();
//        String uStart = map.get("uid_start"), uEnd = map.get("uid_end");
//        String dStart = extractDate(map.get("date_start")), dEnd = extractDate(map.get("date_end"));
//        String tStart = map.get("tid_start").substring(0,7), tEnd = map.get("tid_end").substring(0, 7);
//        String rangeStart = dStart + tStart, rangeEnd = dEnd + tEnd;
//
//        byte[] bColFamily = Bytes.toBytes("data");
//        byte[] bColContent = Bytes.toBytes("content");
//
//        Scan scan = new Scan(Bytes.toBytes(uStart), Bytes.toBytes(uEnd));
//        scan.addColumn(bColFamily, bColContent);
//        scan.setCacheBlocks(false);
//        scan.setBatch(1000);
//        scan.setStartRow(Bytes.toBytes(uStart));
//        Map<String, Integer> countMap = new HashMap<>();
//        countMap.put(prefix1, 0);
//        countMap.put(prefix2, 0);
//        countMap.put(prefix3, 0);
//
//        ResultScanner rs = table.getScanner(scan);
//
//        for (Result r = rs.next(); r != null; r = rs.next()) {
//
//            JSONArray arr = new JSONArray(Bytes.toString(r.getValue(bColFamily, bColContent)));
//
//            for (int idx = 0; idx < arr.length(); idx++){
//                JSONObject obj = arr.getJSONObject(idx);
//                String tc = obj.getString("tc");
//                if (tc.compareTo(rangeStart) >= 0 && tc.compareTo(rangeEnd) <= 0){
//                    JSONObject wc = obj.getJSONObject("wordCount");
//
//                    for (String key : wc.keySet()){
//                        if (key.startsWith(prefix1l)){
//                            countMap.put(prefix1, countMap.get(prefix1) + wc.getInt(key));
//                        }
//                        else if (key.startsWith(prefix2l)){
//                            countMap.put(prefix2, countMap.get(prefix2) + wc.getInt(key));
//                        }
//                        else if (key.startsWith(prefix3l)){
//                            countMap.put(prefix3, countMap.get(prefix3) + wc.getInt(key));
//                        }
//                    }
//                }
//            }
//        }
//        
//        StringBuilder result = new StringBuilder();
//        result.append(prefix1 + ":" + countMap.get(prefix1) + "\n")
//              .append(prefix2 + ":" + countMap.get(prefix2) + "\n")
//              .append(prefix3 + ":" + countMap.get(prefix3) + "\n");
//        
//        return result.toString();
//    }

    private String extractDate(String date){
        StringBuilder sb = new StringBuilder();
        sb.append(date.substring(3,4))
            .append(date.substring(5,7))
            .append(date.substring(8,10));
        return sb.toString();
    }
    
    /**
     * @param userId
     * @return JSONArray which contains all the tweet of input user
     */
     public String query_bk(MultiMap map) throws IOException{
    
         String prefix1 = map.get("p1"), prefix2 = map.get("p2"), prefix3 = map.get("p3");
         String prefix1l = prefix1.toLowerCase(), prefix2l = prefix2.toLowerCase(), prefix3l = prefix3.toLowerCase();

         byte[] bColFamily = Bytes.toBytes("data");
         byte[] bColUid = Bytes.toBytes("userId");
         byte[] bColWordCount = Bytes.toBytes("wordCount");
        
         byte[] bStartUid = Bytes.toBytes(map.get("uid_start"));
         byte[] bEndUid = Bytes.toBytes(map.get("uid_end"));
         String dStart = extractDate(map.get("date_start")), dEnd = extractDate(map.get("date_end"));
         String tStart = map.get("tid_start"), tEnd = map.get("tid_end");
         byte[] rangeStart = Bytes.toBytes(dStart + tStart), rangeEnd = Bytes.toBytes(dEnd + tEnd);

         Scan scan = new Scan();
         scan.addColumn(bColFamily, bColUid);
         scan.addColumn(bColFamily, bColWordCount);
         
         RegexStringComparator comp = new RegexStringComparator("\"(" + prefix1l + "|" + prefix2l + "|" + prefix3l + ")");
         FilterList filter = new FilterList();
         filter.addFilter(new RowFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(rangeStart)));
         filter.addFilter(new RowFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator(rangeEnd)));
         filter.addFilter(new SingleColumnValueFilter(bColFamily, bColUid, CompareOp.GREATER_OR_EQUAL, bStartUid));
         filter.addFilter(new SingleColumnValueFilter(bColFamily, bColUid, CompareOp.LESS_OR_EQUAL, bEndUid));
         filter.addFilter(new SingleColumnValueFilter(bColFamily, bColWordCount, CompareOp.EQUAL, comp));
         scan.setFilter(filter);

         StringBuilder result = new StringBuilder();
         Map<String, Integer> countMap = new HashMap<>();
         countMap.put(prefix1, 0);
         countMap.put(prefix2, 0);
         countMap.put(prefix3, 0);
         ResultScanner rs = table.getScanner(scan);

         for (Result r = rs.next(); r != null; r = rs.next()) {

             JSONObject obj = new JSONObject(Bytes.toString(r.getValue(bColFamily, bColWordCount)));
             Set<String>keys = obj.keySet();

             for (String key : keys){
                     if (key.startsWith(prefix1l)){
                         countMap.put(prefix1, countMap.get(prefix1) + obj.getInt(key));
                     }
                     else if (key.startsWith(prefix2l)){
                         countMap.put(prefix2, countMap.get(prefix2) + obj.getInt(key));
                     }
                     else if (key.startsWith(prefix3l)){
                         countMap.put(prefix3, countMap.get(prefix3) + obj.getInt(key));
                     }
                 }
             }
         result.append(prefix1 + ":" + countMap.get(prefix1) + "\n")
         	  .append(prefix2 + ":" + countMap.get(prefix2) + "\n")
         	  .append(prefix3 + ":" + countMap.get(prefix3) + "\n");
        
         return result.toString();
     }

}
