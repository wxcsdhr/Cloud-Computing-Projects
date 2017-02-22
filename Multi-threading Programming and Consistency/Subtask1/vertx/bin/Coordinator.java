import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.net.URL;
import java.sql.Timestamp;
import java.util.TimeZone;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;


public class Coordinator extends Verticle {

    /**
     * TODO: Set the values of the following variables to the DNS names of your
     * three dataCenter instances
     */
    private static final String dataCenter1 = "ec2-54-172-64-90.compute-1.amazonaws.com";
    private static final String dataCenter2 = "ec2-54-205-41-40.compute-1.amazonaws.com";
    private static final String dataCenter3 = "ec2-52-87-178-27.compute-1.amazonaws.com";
    private static final Object lock = new Object();
    private static ConcurrentHashMap<String, PriorityQueue<String>> table = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, PriorityQueue<String>> putTable = new ConcurrentHashMap<>();

    @Override
    public void start() {
        //DO NOT MODIFY THIS
        KeyValueLib.dataCenters.put(dataCenter1, 1);
        KeyValueLib.dataCenters.put(dataCenter2, 2);
        KeyValueLib.dataCenters.put(dataCenter3, 3);
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
                //You may use the following timestamp for ordering requests
                final String timestamp = new Timestamp(System.currentTimeMillis() 
                                                + TimeZone.getTimeZone("EST").getRawOffset()).toString();
                
                //Create one entry if does not exist
                table.putIfAbsent(key, new PriorityQueue<String>());
                putTable.putIfAbsent(key, new PriorityQueue<String>());
                synchronized(lock){
                    table.get(key).add(timestamp);
                    putTable.get(key).add(timestamp);
                }
                Thread t = new Thread(new Runnable() {
                    public void run() {
                        //TODO: Write code for PUT operation here.
                        //Each PUT operation is handled in a different thread.
                        //Highly recommended that you make use of helper functions.

                        //whether needs to wait for a get or put complete.
                        synchronized(putTable.get(key)){
                            //Put new timestamp into table and tablePut.
                            //If there are other operations has not bee finished
                            while(!table.get(key).peek().equals(timestamp)){
                                try{
                                    putTable.get(key).wait();
                                }catch(InterruptedException e){
                                    e.printStackTrace();
                                }
                            }
                            try{
                                KeyValueLib.PUT(dataCenter1, key, value);
                                KeyValueLib.PUT(dataCenter2, key, value);
                                KeyValueLib.PUT(dataCenter3, key, value);
                            }catch(Exception e){
                                e.printStackTrace();
                            }
                            //Finish operation, remove timestamp and notifyAll
                            putTable.get(key).remove(timestamp);
                            table.get(key).remove(timestamp);
                            putTable.get(key).notifyAll();
                        }
                    }
                });
                t.start();
                
                // Every important notice should be repeated for three times
                //Do not remove this
                //Do not remove this
                //Do not remove this
                req.response().end(); 

            }
        });

        routeMatcher.get("/get", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                final String key = map.get("key");
                final String loc = map.get("loc");
                //You may use the following timestamp for ordering requests
                final String timestamp = new Timestamp(System.currentTimeMillis() 
                                + TimeZone.getTimeZone("EST").getRawOffset()).toString();
                
                //Create one entry if does not exist
                table.putIfAbsent(key, new PriorityQueue<String>());
                putTable.putIfAbsent(key, new PriorityQueue<String>());

                synchronized(lock){
                    table.get(key).add(timestamp);
                }
                Thread t = new Thread(new Runnable() {
                    public void run() {
                        //TODO: Write code for GET operation here.
                        //Each GET operation is handled in a different thread.
                        //Highly recommended that you make use of helper functions.
                        synchronized(putTable.get(key)){
                            //Put new timestamp into table
                            while(putTable.get(key).size() != 0)
                                try{
                                    putTable.get(key).wait();
                                }catch(InterruptedException e){
                                    e.printStackTrace();
                                }
                            putTable.get(key).notifyAll();
                        }
                        String result = "0";
                        try{
                            if(loc.equals("1")){
                                result = KeyValueLib.GET(dataCenter1, key);
                            }else if (loc.equals("2")){
                                result = KeyValueLib.GET(dataCenter2, key);
                            }else if (loc.equals("3")){
                                result = KeyValueLib.GET(dataCenter3, key);
                            }
                            if(result.equals("null")){
                                result = "0";
                            }                                
                        }catch(Exception e){
                            e.printStackTrace();
                        }
                        req.response().end(result);
                        synchronized(table.get(key)){
                            table.get(key).remove(timestamp);
                        }
                    }
                });
                t.start();
            }
        });

	routeMatcher.get("/flush", new Handler<HttpServerRequest>() {
		@Override
		public void handle(final HttpServerRequest req) {
			//Flush all datacenters before each test.
			URL url = null;
			try {
				flush(dataCenter1);
				flush(dataCenter2);
				flush(dataCenter3);
			} catch (Exception e) {
				e.printStackTrace();
			}
			//This endpoint will be used by the auto-grader to flush your datacenter before tests
			//You can initialize/re-initialize the required data structures here
			req.response().end();
		 }
		 
		 private void flush(String dataCenter) throws Exception {
			URL url = new URL("http://" + dataCenter + ":8080/flush");
			BufferedReader in = new BufferedReader(new InputStreamReader(url.openConnection().getInputStream()));
			String inputLine;
			while ((inputLine = in.readLine()) != null);
			in.close();
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
