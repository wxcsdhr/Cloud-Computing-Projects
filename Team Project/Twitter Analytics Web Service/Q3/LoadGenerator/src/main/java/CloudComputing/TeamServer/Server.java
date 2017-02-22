package CloudComputing.TeamServer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;

import org.json.JSONArray;


public class Server {
    private static Vertx vertx = Vertx.vertx();

	public void start() {
		vertx.createHttpServer().requestHandler(new MyHandler()).listen(8080);
	}
}

class MyHandler implements Handler<HttpServerRequest> {
	
    private static final String TEAM_ID = System.getenv("TEAMID");
	private static final String TEAM_AWS_ACCOUNT_ID = System.getenv("AWSACCOUNT");
	private static ExecutorService executor = Executors.newFixedThreadPool(4);
	private static SimpleDateFormat fm = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
	private static String FILENAME = "";
	
	@Override
	public void handle(final HttpServerRequest event) {
		BufferedWriter bw;
		try {
			bw = new BufferedWriter(new FileWriter(FILENAME, true));
			if (event.method() == HttpMethod.GET) {
				new Thread(new Runnable() {

					@Override
					public void run() {

						MultiMap map = event.params();
						if (map.isEmpty()) {
							return;
						}
						try{
							String tweetid = map.get("tweetid");
							String op = map.get("op");
							String seq = map.get("seq");
							String field = map.get("field");
							String hashtag = map.get("hashtag");
							String payload = map.get("payload");
							StringBuilder sb = new StringBuilder();
							sb.append(tweetid + "\t")
							  .append(op + "\t")
							  .append(seq + "\t")
							  .append(field + "\t")
							  .append(hashtag + "\t")
							  .append(payload + "\n");
						    event.response().end("testetstest");
						    if(op.equals("read")){
						    		bw.append(sb.toString());
						    }
						    bw.close();
						}catch(Exception e){
							e.printStackTrace();
						}
					
					}
					
				}).start();
			} else {
				event.response().end("TEAM PUT");
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
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
		return "localhost";
	}
}
	
