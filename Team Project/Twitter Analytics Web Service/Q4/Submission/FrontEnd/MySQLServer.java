package CloudComputing.TeamServer;

import java.sql.SQLException;
import java.util.Comparator;
import java.util.Map;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;

import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;

public class MySQLServer {
    private static Vertx vertx = Vertx.vertx();

    public void start() {
        vertx.createHttpServer().requestHandler(new MyHandler()).listen(8080);
    }
}

class MyHandler implements Handler<HttpServerRequest> {

    private static MySQLQuery4 mysql = new MySQLQuery4();
    //<tweetid, <field, [sequence + "#" + operation]>>
    private ConcurrentHashMap<String, Map<String, PriorityQueue<String>>> tweetFieldOps = new ConcurrentHashMap<>();
    private String teamID = System.getenv("TEAMID"), awsAccount = System.getenv("AWSACCOUNT");
    private String responseHead = teamID + "," + awsAccount + "\n";

    private final String splitter = "#";

    @Override
    public void handle(HttpServerRequest event) {
        if (event.method() == HttpMethod.GET) {
            try{
                MultiMap map = event.params();

                String tweetid = map.get("tweetid"), field = map.get("field"), op = map.get("op"), seq = map.get("seq"), payload = map.get("payload");

                if (field == null || field.isEmpty()){
                    event.response().end(responseHead);
                    return;
                }
                if (op.equals("write") || op.equals("delete"))
                    event.response().end(responseHead + "success\n");
                
                String combo = tweetid + field;
                syncFlow(event, combo, tweetid, field, op, seq, payload); // thread synchronization

            }catch(Exception e){
                e.printStackTrace();
            }
        } else {
            event.response().end("TEAM PUT");
        }
    }

    private void syncFlow(HttpServerRequest event, String combo, String tweetid, String field, String op, String seq, String payload){
        //add current request to global map
        synchronized(combo.intern()){
            if (!tweetFieldOps.containsKey(tweetid))
                tweetFieldOps.put(tweetid, new HashMap<String,PriorityQueue<String>>());

            // <field, PQ<sequence + operation>>
            Map<String,PriorityQueue<String>> fieldOps = tweetFieldOps.get(tweetid);
            if (!fieldOps.containsKey(field)){
                fieldOps.put(field, new PriorityQueue<String>(new Comparator<String>(){

                    public int compare(String str1, String str2){
                        return Integer.parseInt(str1.split(splitter)[0]) - Integer.parseInt(str2.split(splitter)[0]);
                    }
                }));
            }

            PriorityQueue<String> ops = fieldOps.get(field);
            ops.add(seq + splitter + op);
            fieldOps.put(field, ops);
            tweetFieldOps.put(tweetid, fieldOps);

            combo.intern().notifyAll();
        }

        Thread t = new Thread(new Runnable(){
            @Override
            public void run() {
                synchronized(combo.intern()){
                    while(true){
                        if (!acquire_lock(tweetid, field, seq, op)){
                            try{
                                combo.intern().wait();
                            }catch(InterruptedException e){
                                e.printStackTrace();
                            }
                        }else
                            break;
                    }

                    try{
                        if (op.equals("delete")) {
                            mysql.delete(tweetid, field);
                        } else if (op.equals("write")) {
                            mysql.write(tweetid, field, payload);
                        } else {
                            String ret = mysql.read(tweetid, field);
                            if (ret == null)
                                event.response().end(responseHead);
                            else{
                                String temp = responseHead + ret + "\n";
                                event.response().putHeader("Content-Type", "text/plain");
                                event.response().putHeader("Content-Length", String.valueOf(temp.length()));
                                event.response().end(temp);
                            }
                            event.response().close();
                        }
                    }catch(SQLException e){
                        e.printStackTrace();
                    }

                    release_lock(tweetid, field, seq, op);
                    combo.intern().notifyAll();
                }
            }
        });
    t.start();
    }

    private synchronized boolean acquire_lock(String tweetid, String field, String sequence, String operation){
        Map<String,PriorityQueue<String>> fieldOps = tweetFieldOps.get(tweetid);
        PriorityQueue<String> ops = new PriorityQueue<>(new Comparator<String>(){
            public int compare(String str1, String str2){
                return Integer.parseInt(str1.split(splitter)[0]) - Integer.parseInt(str2.split(splitter)[0]);
            }
        });

        //order all operations of a specific field of a tweet
        ops.addAll(fieldOps.get(field));

        //write/delete must wait its turn
        if (!operation.equals("read"))
            return ops.peek().equals(sequence + splitter + operation);
        else{ 
            //if all previous operations are read, don't need to block current read
            while(!ops.isEmpty()){
                String[] vals = ops.poll().split(splitter);

                if (vals[0].compareTo(sequence) < 0 && (vals[1].equals("write") || vals[1].equals("delete")))
                    return false;
            }
            return true;
        }
    }

    private synchronized void release_lock(String tweetid, String field, String sequence, String operation){
        Map<String,PriorityQueue<String>> fieldOps = tweetFieldOps.get(tweetid);
        PriorityQueue<String> ops = new PriorityQueue<>(fieldOps.get(field));
        ops.remove(sequence + splitter + operation);
        fieldOps.put(field, ops);
        tweetFieldOps.put(tweetid, fieldOps);
    }
}
