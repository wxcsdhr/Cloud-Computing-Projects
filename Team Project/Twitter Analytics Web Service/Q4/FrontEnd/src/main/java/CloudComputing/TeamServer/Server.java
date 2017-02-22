package CloudComputing.TeamServer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;


public class Server {
    private static Vertx vertx = Vertx.vertx();

	public void start() {
		vertx.createHttpServer().requestHandler(new HBaseHandler()).listen(8080);
	}
}

class HBaseHandler implements Handler<HttpServerRequest> {
	private String TEAM_ID = System.getenv("TEAMID");
	private String TEAM_AWS_ACCOUNT_ID = System.getenv("AWSACCOUNT");
	
	private static ExecutorService q1Executor = Executors.newFixedThreadPool(4);
	private static ExecutorService q2Executor = Executors.newFixedThreadPool(15);
	private static ExecutorService q3Executor = Executors.newFixedThreadPool(12);

    //Q1 variables
	private static PDC decrypt = new PDC();
    private static SimpleDateFormat fm = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");

    //Q2 variables
    private static HBaseQuery2 hbase2 = new HBaseQuery2();
	private static SimilarityCalculator simCalc = new SimilarityCalculator();
	private static ResultOrganizer organizer = new ResultOrganizer();

    //Q3 variables
    private static HBaseQuery3 hbase3 = new HBaseQuery3();

    //Q4 variables
    private static String responseHead = System.getenv("TEAMID") + "," + System.getenv("AWSACCOUNT")+ "\n";
    private static String targetDNS = "ec2-52-90-114-189.compute-1.amazonaws.com";


    @Override
    public void handle(HttpServerRequest event) {
        if (event.method() == HttpMethod.GET) {

            MultiMap map = event.params();
            if (map.isEmpty()){ // health check
                event.response().end("health check");
                event.response().close();
                return;
            }else if(map.contains("key")){ //Query 1
                q1Executor.execute(new Runnable(){
                    @Override
                    public void run() {
                        final String key = map.get("key");
                        final String message = map.get("message");
                    
                        int Z = decrypt.keyGen(key);
                        if(Z == -1){
                            event.response().end("");
                        }else{
                            String result = decrypt.spiralize(message, Z);
                            String resposne = TEAM_ID + "," + TEAM_AWS_ACCOUNT_ID + "\n" + fm.format(new Date()) + "\n"+ result + "\n";
                            event.response().end(resposne);
                        }
                        event.response().close();
                    }
                });
            }else if (map.contains("userid1")){ //Query 2
                q2Executor.execute(new Runnable(){
                    @Override
                    public void run() {
                        final String userId1 = map.get("userid1");
                        final String userId2 = map.get("userid2");
                        int n = Integer.parseInt(map.get("n"));

                        try{
                            List<Get> ids = new ArrayList<Get>(){
								private static final long serialVersionUID = 1L;
								{
                                    add(new Get(Bytes.toBytes(userId1)));
                                    add(new Get(Bytes.toBytes(userId2)));
                                }
                            };

                            JSONArray[] result = hbase2.query(ids);
                            JSONArray tweetsOfUser1 = result[0];
                            JSONArray tweetsOfUser2 = result[1];

                            //shared words & calculate similarity
                            List<String> sharedWords = new ArrayList<>();
                            int similarity = simCalc.calculate(tweetsOfUser1, tweetsOfUser2, sharedWords);
                            event.response().end(organizer.response(TEAM_ID, TEAM_AWS_ACCOUNT_ID, 
                                                    tweetsOfUser1, tweetsOfUser2, sharedWords, similarity, n));
                            event.response().close();
                        }catch(Exception e){
                            e.printStackTrace();
                        }
                    }
                });
            }else if (map.contains("date_start")){ //Query 3
                q3Executor.execute(new Runnable(){
                    @Override
                    public void run() {
                        try{
                            event.response().end(TEAM_ID + "," + TEAM_AWS_ACCOUNT_ID + "\n" + hbase3.query(map));
                            event.response().close();
                        }catch(Exception e){
                            e.printStackTrace();
                        }
                    }
                });
            }else { //Query 4
            	//backup plan: forward to single mysql machine
                String tweetid = map.get("tweetid"), field = map.get("field"), op = map.get("op"), seq = map.get("seq"), payload = map.get("payload");

                if (field == null || field.isEmpty()){
                    event.response().end(responseHead);
                    event.response().close();
                    return;
                }
                if (op.equals("write") || op.equals("delete")){
                    event.response().end(responseHead + "success\n");
                    
                    Thread t = new Thread(new Runnable() {
                    	String result = null;
                        public void run() {
                            try{
                                result = Dispatcher.FORWARD(targetDNS, tweetid, field, op, seq, payload);
                            }catch(IOException e){
                                e.printStackTrace();
                            }
                            if (result == null || result.isEmpty())
                            	event.response().end();
                            else
                            	event.response().end(result);
                        }
                    });
                    t.start();
                    
                }else if (op.equals("read")){
                	Thread t = new Thread(new Runnable() {
                        String result = null;
                        public void run() {
                            try{
                                result = Dispatcher.READ(targetDNS, tweetid, field, op, seq, payload);
                            }catch(IOException e){
                                e.printStackTrace();
                            }
                            if (result == null || result.isEmpty()){
                                event.response().end();
                            }else{
                            	event.response().end(result);
                            }
                        }
                    });
                    t.start();
                }
            }
		} else if (event.method() == HttpMethod.PUT){
			event.response().end("TEAM PUT");
			event.response().close();
		} else {
            event.response().putHeader("Content-Type", "text/html");
            String response = "Not found.";
            event.response().putHeader("Content-Length", String.valueOf(response.length()));
            event.response().end(response);
            event.response().close();
        }
	}
}