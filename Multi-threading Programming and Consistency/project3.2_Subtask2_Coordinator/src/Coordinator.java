import java.io.IOException;
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
    // Lock object to insert timestamp

    /**
     * TODO: Set the values of the following variables to the DNS names of your
     * three dataCenter instances. Be sure to match the regions with their DNS!
     * Do the same for the 3 Coordinators as well.
     */
    private static final String dataCenterUSE = "ec2-54-173-62-77.compute-1.amazonaws.com";
    private static final String dataCenterUSW = "ec2-52-90-134-79.compute-1.amazonaws.com";
    private static final String dataCenterSING = "ec2-54-197-148-94.compute-1.amazonaws.com";

    private static final String coordinatorUSE = "ec2-52-87-218-33.compute-1.amazonaws.com";
    private static final String coordinatorUSW = "ec2-54-161-15-130.compute-1.amazonaws.com";
    private static final String coordinatorSING = "ec2-54-162-123-190.compute-1.amazonaws.com";

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
                final String timestamp = map.get("timestamp");
                final String forwarded = map.get("forward");
                final String forwardedRegion = map.get("region");
                // Obtain the primary Coordinator by hash function.
                final String primaryCoordinator = Integer.toString((Character.getNumericValue(key.charAt(0)) + 2)%3 + 1);
                Thread t = new Thread(new Runnable() {
                    public void run() {

                    /* TODO: Add code for PUT request handling here
                     * Each operation is handled in a new thread.
                     * Use of helper functions is highly recommended */
                    // Whether needs wait other PUT operations
                    if(!primaryCoordinator.equals(Integer.toString(region))){ // need forward
                        // Tell every datacenter that PUT operation is initiated
                        try {
                            if(consistencyType.equals("strong")){
                                KeyValueLib.AHEAD(key,timestamp);
                                }
                        }catch (IOException e2) {
                            e2.printStackTrace();
                        }

                        try{
                            //Wether needs to forward the request
                            if(primaryCoordinator.equals("1") && region != 1){
                                KeyValueLib.FORWARD(coordinatorUSE, key, value, timestamp);
                            }else if(primaryCoordinator.equals("2") && region != 2){
                                KeyValueLib.FORWARD(coordinatorUSW, key, value, timestamp);
                            }else if(primaryCoordinator.equals("3") && region != 3){
                                KeyValueLib.FORWARD(coordinatorSING, key, value, timestamp);
                            }
                        }catch(IOException e){
                            e.printStackTrace();
                        }
                        
                        try {
                            if(consistencyType.equals("strong")){
                                KeyValueLib.COMPLETE(key, timestamp);
                            }
                        } catch (IOException e1) {
                            // TODO Auto-generated catch block
                            e1.printStackTrace();
                        }
                    }else{ // does not need forward
                        if(forwarded == null){
                            // Put key:value into each data center.
                            try {
                                if(consistencyType.equals("strong"))
                                    KeyValueLib.AHEAD(key,timestamp);
                            } catch (IOException e2) {
                                // TODO Auto-generated catch block
                                e2.printStackTrace();
                            } // "PUT" operation has started
                        }
                        try{
                            KeyValueLib.PUT(dataCenterUSE, key, value, timestamp, consistencyType);
                            KeyValueLib.PUT(dataCenterUSW, key, value, timestamp, consistencyType);
                            KeyValueLib.PUT(dataCenterSING, key, value, timestamp, consistencyType);
                        }catch(Exception e){
                            e.printStackTrace();
                        }
                        if(forwarded == null){
                            try {
                                if(consistencyType.equals("strong"))
                                    KeyValueLib.COMPLETE(key, timestamp);
                            } catch (IOException e1) {
                                // TODO Auto-generated catch block
                                e1.printStackTrace();
                            }            
                        }
                                
                        //release lock and notify all datacenter that the PUT operation has finished.
                        }
                    }
                });
                t.start();
                req.response().end(); // Do not remove this
            }
        });

        routeMatcher.get("/get", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                final String key = map.get("key");
                final String timestamp = map.get("timestamp");
                Thread t = new Thread(new Runnable() {
                    public void run() {
                    /* TODO: Add code for GET requests handling here
                     * Each operation is handled in a new thread.
                     * Use of helper functions is highly recommended */
                        String response = "0";
                        try{
                            if(region == 1){
                                response = KeyValueLib.GET(dataCenterUSE, key, timestamp, consistencyType);
                            }else if(region == 2){
                                response = KeyValueLib.GET(dataCenterUSW, key, timestamp, consistencyType);
                            }else if(region == 3){
                                response = KeyValueLib.GET(dataCenterSING, key, timestamp, consistencyType);
                            }
                            
                        }catch(Exception e){
                            e.printStackTrace();
                        }
                        if(response.equals("null")){
                            response = "0";
                        }
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

