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
    private Map<String, Map<String, PriorityQueue<String>>> tweetFieldOps = new HashMap<>();
    private String teamID = System.getenv("TEAM_ID"), awsAccount = System.getenv("TEAM_AWS_ACCOUNT_ID");
    private String responseHead = teamID + "," + awsAccount + "\n";
    private final String splitter = "#";

    @Override
    public void handle(HttpServerRequest event) {
        if (event.method() == HttpMethod.GET) {
            try{
                MultiMap map = event.params();
		
                String tweetid = map.get("tweetid"), field = map.get("field"), op = map.get("op"), seq = map.get("seq"), payload = map.get("payload");

                syncFlow(event, tweetid + field, tweetid, field, op, seq, payload); // thread synchronization
            }catch(Exception e){
                e.printStackTrace();
            }
        } else if (event.method() == HttpMethod.PUT){
            event.response().end("TEAM PUT");
            event.response().close();
        } else{
            event.response().putHeader("Content-Type", "text/html");
            String response = "Not found.";
            event.response().putHeader("Content-Length", String.valueOf(response.length()));
            event.response().end(response);
            event.response().close();
        }
    }

    private void syncFlow(HttpServerRequest event, String combo, String tweetid, String field, String op, String seq, String payload){
        //add current request to global map
        synchronized(tweetid.intern()){
            if (!tweetFieldOps.containsKey(tweetid))
                tweetFieldOps.put(tweetid, new HashMap<String,PriorityQueue<String>>());

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

            //tweetid.intern().notifyAll();
        }

        Thread t = new Thread(new Runnable(){
            @Override
            public void run() {
                synchronized(tweetid.intern()){
                    while(true){
                        if (!acquire_lock(tweetid, field, seq, op)){
                            try{
                                //combo.intern().wait();
                                tweetid.intern().wait();
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
                            // a field start reading, it will always be reading
                        	String ret = mysql.read(tweetid, field);
                        	if (ret == null)
                            	event.response().end();
                        	else{
					String resp = responseHead + ret + "\n";
					event.response().putHeader("Content-Type", "text/plain");
					event.response().putHeader("Content-Length", String.valueOf(resp.length()));
                            		event.response().end(resp);
				}

                        	event.response().close();
                        }
                    }catch(SQLException e){
                        e.printStackTrace();
                    }

                    release_lock(tweetid, field, seq, op);
                    tweetid.intern().notifyAll();
                    //combo.intern().notifyAll();
                }
            }
        });
        t.start();
    }

    //private synchronized boolean acquire_lock(String tweetid, String field, String sequence, String operation){
    private boolean acquire_lock(String tweetid, String field, String sequence, String operation){
        Map<String,PriorityQueue<String>> fieldOps = tweetFieldOps.get(tweetid);
        PriorityQueue<String> ops = new PriorityQueue<>(new Comparator<String>(){
            public int compare(String str1, String str2){
                return Integer.parseInt(str1.split(splitter)[0]) - Integer.parseInt(str2.split(splitter)[0]);
            }
        });

        ops.addAll(fieldOps.get(field));

        if (!operation.equals("read"))
            return ops.peek().equals(sequence + splitter + operation);
        else{
            while(!ops.isEmpty()){
                String[] vals = ops.poll().split(splitter);

                if (vals[0].compareTo(sequence) < 0 && (vals[1].equals("write") || vals[1].equals("delete")))
                    return false;
            }
            return true;
        }
    }

    //private synchronized void release_lock(String tweetid, String field, String sequence, String operation){
    private void release_lock(String tweetid, String field, String sequence, String operation){
        Map<String,PriorityQueue<String>> fieldOps = tweetFieldOps.get(tweetid);
        PriorityQueue<String> ops = new PriorityQueue<>(fieldOps.get(field));
        ops.remove(sequence + splitter + operation);
        fieldOps.put(field, ops);
        tweetFieldOps.put(tweetid, fieldOps);
    }
}
