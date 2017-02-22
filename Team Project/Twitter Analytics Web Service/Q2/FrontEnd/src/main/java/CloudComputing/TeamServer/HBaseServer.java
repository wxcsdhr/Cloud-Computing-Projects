package CloudComputing.TeamServer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.IOException;

import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;

import org.json.JSONArray;
import org.json.JSONObject;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseServer {
    private static Vertx vertx = Vertx.vertx();

	public void start() {
		vertx.createHttpServer().requestHandler(new MyHandler()).listen(8080);
	}
}

class MyHandler implements Handler<HttpServerRequest> {
	
    private static final String TEAM_ID = System.getenv("TEAMID");
	private static final String TEAM_AWS_ACCOUNT_ID = System.getenv("AWSACCOUNT");
	private static ExecutorService executor = Executors.newFixedThreadPool(4);
	private static HBaseQuery hbase = new HBaseQuery();
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

					try{
						List<Get> ids = new ArrayList<Get>(){
							{
								add(new Get(Bytes.toBytes(map.get("userId1"))));
								add(new Get(Bytes.toBytes(map.get("userId2"))));
							}
						};

						JSONArray[] result = hbase.query(ids);
						JSONArray tweetsOfUser1 = result[0];
						JSONArray tweetsOfUser2 = result[1];

						List<String> sharedWords = new ArrayList<>();
					    int similarity = simCalc.calculate(tweetsOfUser1, tweetsOfUser2, sharedWords);
					    event.response().end(organizer.response(TEAM_ID, TEAM_AWS_ACCOUNT_ID, 
								tweetsOfUser1, tweetsOfUser2, sharedWords, similarity, Integer.parseInt(map.get("n"))));
				        //query MySQL
//			 	 	    JSONArray tweetsOfUser1 = MySqlQuery.getInstance(getShardingUrl(userId1)).getTweetsByUserId(userId1);
//					    JSONArray tweetsOfUser2 = MySqlQuery.getInstance(getShardingUrl(userId2)).getTweetsByUserId(userId2);

					    //shared words & calculate similarity
					    //List<String> sharedWords = new ArrayList<>();
//					    int similarity = simCalc.calculate(tweetsOfUser1, tweetsOfUser2, sharedWords);

//					    event.response().end(organizer.response(TEAM_ID, TEAM_AWS_ACCOUNT_ID, 
//												tweetsOfUser1, tweetsOfUser2, sharedWords, similarity, n));
					    //event.response().end(tweetsOfUser1.toString());
					}catch(Exception e){
						e.printStackTrace();
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
		if (userId.compareTo("999999542") <= 0) {
			return "ec2-54-159-64-62.compute-1.amazonaws.com";
		} else if (userId.compareTo("999999542") <= 0) {
			return "ec2-54-161-39-46.compute-1.amazonaws.com";
		} else {
			return "ec2-54-210-183-163.compute-1.amazonaws.com";
		}
	}
}
	
