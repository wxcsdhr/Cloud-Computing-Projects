package CloudComputing.TeamServer;

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

import org.json.JSONArray;


public class HBaseServer {
    private static Vertx vertx = Vertx.vertx();

	public void start() {
		vertx.createHttpServer().requestHandler(new HBaseHandler()).listen(8080);
	}
}

class HBaseHandler implements Handler<HttpServerRequest> {
   private static final String TEAM_ID = System.getenv("TEAMID");
	private static final String TEAM_AWS_ACCOUNT_ID = System.getenv("ACCOUNTID");
	private static ExecutorService executor = Executors.newFixedThreadPool(4);
	private static SimpleDateFormat fm = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");

	private static HBaseQuery2 hbase2 = new HBaseQuery2();
	private static HBaseQuery3 hbase3 = new HBaseQuery3();
	private static PDC decrypt = new PDC();
	private static SimilarityCalculator simCalc = new SimilarityCalculator();
	private static ResultOrganizer organizer = new ResultOrganizer();

	@Override
	public void handle(HttpServerRequest event) {
		if (event.method() == HttpMethod.GET) {
			
			//event.response().end("TEAM GET");
			executor.execute(new Runnable(){
			@Override
				public void run() {
					MultiMap map = event.params();
					if (map.isEmpty()){ // health check
					  	event.response().end("health check");
						return;
					}else if(map.contains("key")){ //Query 1
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
					}else if(map.contains("userid1")){ //Query 2
						final String userId1 = map.get("userid1");
						final String userId2 = map.get("userid2");
						int n = Integer.parseInt(map.get("n"));

						try{
					    	JSONArray tweetsOfUser1 = hbase2.query(userId1);
						    JSONArray tweetsOfUser2 = hbase2.query(userId2);

						    //shared words & calculate similarity
						    List<String> sharedWords = new ArrayList<>();
						    int similarity = simCalc.calculate(tweetsOfUser1, tweetsOfUser2, sharedWords);
						    event.response().end(organizer.response(TEAM_ID, TEAM_AWS_ACCOUNT_ID, 
													tweetsOfUser1, tweetsOfUser2, sharedWords, similarity, n));
						}catch(Exception e){
							e.printStackTrace();
						}
					}else{ //Query 3
						try{
						    event.response().end(TEAM_ID + "\n" + TEAM_AWS_ACCOUNT_ID + "\n" + hbase3.query(map));
						}catch(Exception e){
							e.printStackTrace();
						}
					}
				}
			});
		} else {
			event.response().end("TEAM PUT");
		}
	}
}
	
