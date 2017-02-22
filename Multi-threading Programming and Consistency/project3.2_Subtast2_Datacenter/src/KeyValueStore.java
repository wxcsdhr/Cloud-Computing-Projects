import java.util.concurrent.ConcurrentHashMap;
import java.util.PriorityQueue;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class KeyValueStore extends Verticle {
    /* TODO: Add code to implement your backend storage */
    private static ConcurrentHashMap<String, String> keyValue = new ConcurrentHashMap<String, String>();
    private static ConcurrentHashMap<String, PriorityQueue<String>> putTable = new ConcurrentHashMap<String ,PriorityQueue<String>>();
    private static ConcurrentHashMap<String, PriorityQueue<String>> putFinishTable = new ConcurrentHashMap<String, PriorityQueue<String>>();
    
    @Override
    public void start() {
        final KeyValueStore keyValueStore = new KeyValueStore();
        final RouteMatcher routeMatcher = new RouteMatcher();
        final HttpServer server = vertx.createHttpServer();
        server.setAcceptBacklog(32767);
        server.setUsePooledBuffers(true);
        server.setReceiveBufferSize(4 * 1024);
        routeMatcher.get("/put", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                String key = map.get("key");
                String value = map.get("value");
                String consistency = map.get("consistency");
                Integer region = Integer.parseInt(map.get("region"));
                String timestamp = map.get("timestamp");
                /* TODO: Add code here to handle the put request
                     Remember to use the explicit timestamp if needed! */
                Thread t = new Thread(new Runnable(){
                        public void run(){
                            synchronized(putTable.get(key)){ // block every get operation
                                //Whether there is an operation processing
                                while(!putTable.get(key).peek().equals(timestamp)){
                                    try {
                                        putTable.get(key).wait();
                                    } catch (InterruptedException e) {
                                        // TODO Auto-generated catch block
                                        e.printStackTrace();
                                    }
                                }
                                // Put key and value into table.
                                keyValue.put(key, value);
                                
                                // After complete, put it into the finish queue and notify all PUTs and GETs that are waiting.
                                putTable.get(key).notifyAll();
                                String response = "stored";
                                req.response().putHeader("Content-Type", "text/plain");
                                req.response().putHeader("Content-Length",
                                                    String.valueOf(response.length()));
                                req.response().end(response);
                                putFinishTable.get(key).add(timestamp);
                                req.response().close();
                            }
                        }
                });
                
                t.start();
            }
        });
        
        routeMatcher.get("/get", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                final String key = map.get("key");
                String consistency = map.get("consistency");
                final String timestamp = map.get("timestamp");
                putTable.putIfAbsent(key, new PriorityQueue<String>());
                /* TODO: Add code here to handle the get request
                     Remember that you may need to do some locking for this */
                Thread t = new Thread(new Runnable(){
                        public void run(){
                            String response = "";
                            synchronized(putTable.get(key)){
                                while(putTable.get(key).size()!=0 && Long.parseLong(timestamp) > Long.parseLong(putTable.get(key).peek())){
                                    try {
                                        putTable.get(key).wait();
                                    } catch (InterruptedException e) {
                                        // TODO Auto-generated catch block
                                        e.printStackTrace();
                                    }
                                }
                response = keyValue.get(key);
                req.response().putHeader("Content-Type", "text/plain");
                req.response().putHeader("Content-Length",
                        String.valueOf(response.length()));
                req.response().end(response);
                                putTable.get(key).notifyAll();
                req.response().close();
                            }
                        }
                });
                t.start();
            }
        });
        // Clears this stored keys. Do not change this
        routeMatcher.get("/reset", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                /* TODO: Add code to here to flush your datastore. This is MANDATORY */
                keyValue = new ConcurrentHashMap<String, String>();
                putTable = new ConcurrentHashMap<String ,PriorityQueue<String>>();
                putFinishTable = new ConcurrentHashMap<String ,PriorityQueue<String>>();
                req.response().putHeader("Content-Type", "text/plain");
                req.response().end();
                req.response().close();
            }
        });
        // Handler for when the AHEAD is called
        routeMatcher.get("/ahead", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                final String key = map.get("key");
                final String timestamp = map.get("timestamp");
                /* TODO: Add code to handle the signal here if you wish */
                putTable.putIfAbsent(key, new PriorityQueue<String>());
                putFinishTable.putIfAbsent(key, new PriorityQueue<String>());
                // Put timestamp into the queue
                synchronized(putTable.get(key)){
                    putTable.get(key).add(timestamp); // wait for processing
                    putTable.get(key).notifyAll();
                }
                req.response().end();
                req.response().close();
            }
        });
        
        // Handler for when the COMPLETE is called
        routeMatcher.get("/complete", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                String key = map.get("key");
                final String timestamp = map.get("timestamp");
                /* TODO: Add code to handle the signal here if you wish */
                Thread t = new Thread(new Runnable(){
                        public void run(){
                            synchronized(putTable.get(key)){
                                while(!putFinishTable.get(key).contains(timestamp)){ // Put operation is still processing.
                                    try {
                                        putTable.get(key).wait();
                                    } catch (InterruptedException e) {
                                        // TODO Auto-generated catch block
                                        e.printStackTrace();
                                    }
                                }
                                // Remove already completed PUT operation from putTable;
                                putTable.get(key).remove(timestamp);
                                putFinishTable.get(key).remove(timestamp);
                                req.response().putHeader("Content-Type", "text/plain");
                                req.response().end();
                                req.response().close();
                                putTable.get(key).notifyAll();
                            }
                        }
                });
                t.start();
            }
        });
        routeMatcher.noMatch(new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                req.response().putHeader("Content-Type", "text/html");
                String response = "Not found.";
                req.response().putHeader("Content-Length",
                        String.valueOf(response.length()));
                req.response().end(response);
                req.response().close();
            }
        });
        server.requestHandler(routeMatcher);
        server.listen(8080);
    }
}

