package CloudComputing.TeamServer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;

import org.json.JSONArray;


public class MySQLServer {
    private static Vertx vertx = Vertx.vertx();

	public void start() {
		vertx.createHttpServer().requestHandler(new MySQLHandler()).listen(8080);
	}
}

class MySQLHandler implements Handler<HttpServerRequest> {
   private static final String TEAM_ID = System.getenv("TEAMID");
	private static final String TEAM_AWS_ACCOUNT_ID = System.getenv("ACCOUNTID");
	private static ExecutorService executor = Executors.newFixedThreadPool(4);
	private static SimpleDateFormat fm = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
	private static PDC decrypt = new PDC();
	private static SimilarityCalculator simCalc = new SimilarityCalculator();
	private static ResultOrganizer organizer = new ResultOrganizer();

	@Override
	public void handle(HttpServerRequest event) {
		if (event.method() == HttpMethod.GET) {
			
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
						    JSONArray tweetsOfUser1 = MySqlQuery.getInstance(getShardingUrl(userId1)).getTweetsByUserId(userId1);
						    JSONArray tweetsOfUser2 = MySqlQuery.getInstance(getShardingUrl(userId2)).getTweetsByUserId(userId2);

					        //query MySQL
//				 	 	    JSONArray tweetsOfUser1 = MySqlQuery.getInstance(getShardingUrl(userId1)).getTweetsByUserId(userId1);
//						    JSONArray tweetsOfUser2 = MySqlQuery.getInstance(getShardingUrl(userId2)).getTweetsByUserId(userId2);

						    //shared words & calculate similarity
						    List<String> sharedWords = new ArrayList<>();
						    int similarity = simCalc.calculate(tweetsOfUser1, tweetsOfUser2, sharedWords);

						    event.response().end(organizer.response(TEAM_ID, TEAM_AWS_ACCOUNT_ID, 
													tweetsOfUser1, tweetsOfUser2, sharedWords, similarity, n));
						    //event.response().end(tweetsOfUser1.toString());
						}catch(Exception e){
							e.printStackTrace();
						}
					}else{ //Query 3
						event.response().end("15619 is awesome!");
					}

					
				}
			});
		} else {
			event.response().end("TEAM PUT");
		}
	}
	
	/**
	 * This method is a key component of sharding strategy. This is not a good
	 * sharding algorithm but it works. However, due to our ETL not working properly,
	 * this method is also not working effectively.
	 * @param userId
	 * @return
	 */
	private String getShardingUrl(String userId) {
		long seed = Long.parseLong(userId);
		if (seed % 5L == 0L) {
			return "ec2-54-85-253-53.compute-1.amazonaws.com";
		} else if (seed % 5L == 1L) {
			return "ec2-54-146-190-201.compute-1.amazonaws.com";
		} else if (seed % 5L == 2L) {
			return "ec2-54-88-35-90.compute-1.amazonaws.com";
		} else if (seed % 5L == 3L) {
			return "ec2-54-147-234-254.compute-1.amazonaws.com";
		} else if (seed % 5L == 4L) {
			return "ec2-52-91-46-164.compute-1.amazonaws.com";
		}
		return null;
	}
}
	
