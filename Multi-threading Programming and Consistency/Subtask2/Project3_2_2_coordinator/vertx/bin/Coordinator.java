import java.io.IOException;
import java.util.HashMap;
import java.util.Queue;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.PriorityQueue;
import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class Coordinator extends Verticle {

    // This integer variable tells you what region you are in
    // 1 for US-E, 2 for US-W, 3 for Singapore
    private static int region = KeyValueLib.region;

    // Default mode: Strongly consistent
    // Options: strong, eventual
    private static String consistencyType = "strong";

    // ConcurrentHashMap to handle key and timestamps
    private static ConcurrentHashMap<String, PriorityQueue<String>> table = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, PriorityQueue<String>> putTable = new ConcurrentHashMap<>();

    // Lock object to insert timestamp
    private static Object lock = new Object();


    /**
     * TODO: Set the values of the following variables to the DNS names of your
     * three dataCenter instances. Be sure to match the regions with their DNS!
     * Do the same for the 3 Coordinators as well.
     */
    private static final String dataCenterUSE = "";
    private static final String dataCenterUSW = "";
    private static final String dataCenterSING = "";

    private static final String coordinatorUSE = "";
    private static final String coordinatorUSW = "";
    private static final String coordinatorSING = "";

    @Override
    public void start() {
        KeyValueLib.dataCenters.put(dataCenterUSE, 1);
        KeyValueLib.dataCenters.put(dataCenterUSW, 2);
        KeyValueLib.dataCenters.put(dataCenterSING, 3);
        KeyValueLib.coordinators.put(coordinatorUSE, 1);
        KeyValueLib.coordinators.put(coordinatorUSW, 2);
        KeyValueLib.coordinators.put(coordinatorSING, 3);
        final RouteMatcher routeMatcher = new RouteMatcher();
        final HttpServer server = vertx.createHttpServer();
        server.setAcceptBacklog(32767);
        server.setUsePooledBuffers(true);
        server.setReceiveBufferSize(4 * 1024);

        routeMatcher.get("/put", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                final String key = map.get("key");
                final String value = map.get("value");
                final Long timestamp = Long.parseLong(map.get("timestamp"));
                final String forwarded = map.get("forward");
                final String forwardedRegion = map.get("region");
                // Obtain the primary Coordinator by hash function.
                final String primaryCoordinator = Integer.toString((Character.getNumericValue(key.charAt(0)) + 2)%3 + 1);
                // Create entry if does not exist
                table.putIfAbsent(key, new PriorityQueue<String>());
                putTable.putIfAbsent(key, new PriorityQueue<String>());
                synchronized(lock){                
                    table.get(key).add(timestamp);
                    putTable.get(key).add(timestamp);
                }
                Thread t = new Thread(new Runnable() {
                    public void run() {

                    /* TODO: Add code for PUT request handling here
                     * Each operation is handled in a new thread.
                     * Use of helper functions is highly recommended */
                    synchronized(putTable.get(key)){
                        //Whether needs to forward the request
                        if(!primaryCoordinator.equals(region)){
                            KeyValueLib.AHEAD(key, timestamp);
                            if(primaryCoordinator.equals("2")){
                                KeyValueLib.
                            }
                        }
                        while(!table.get(key).peek().equals(timestamp)){
                            try{
                                putTable.get(key).wait();
                            }catch(InterruptedException e){
                                e.printStackTrace();
                            }
                        }
                        // Put key:value into each data center.
                        try{
                            KeyValueLib.PUT(dataCenterUSE, key, value);
                            KeyValueLib.PUT(dataCenterUSW, key, value);
                            KeyValueLib.PUT(dataCenterSING, key, value);
                        }catch(Exception e){
                            e.printStackTrace();
                        }

                        
                    }
                    }
                });

                // If primary coordinator does not match, forward the request to its primary coordinator

                t.start();
                req.response().end(); // Do not remove this
            }
        });

        routeMatcher.get("/get", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                final String key = map.get("key");
                final Long timestamp = Long.parseLong(map.get("timestamp"));
                Thread t = new Thread(new Runnable() {
                    public void run() {
                    /* TODO: Add code for GET requests handling here
                     * Each operation is handled in a new thread.
                     * Use of helper functions is highly recommended */
                        String response = "0";

                        req.response().end(response);
                    }
                });
                t.start();
            }
        });
        /* This endpoint is used by the grader to change the consistency level */
        routeMatcher.get("/consistency", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                consistencyType = map.get("consistency");
                req.response().end();
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

