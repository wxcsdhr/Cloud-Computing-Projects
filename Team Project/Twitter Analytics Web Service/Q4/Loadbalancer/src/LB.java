
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;

public class LB {
	private static Vertx vertx = Vertx.vertx();
	
	public void start() {
		HttpClient client = vertx.createHttpClient(new HttpClientOptions());
		vertx.createHttpServer().requestHandler(new MyHandler(client)).listen(8080);
	}
}

class MyHandler implements Handler<HttpServerRequest>{
	private static final String TEAM_ID = System.getenv("TEAMID");
	private static final String TEAM_AWS_ACCOUNT_ID = System.getenv("AWSACCOUNT");
	private static ExecutorService executor = Executors.newFixedThreadPool(4);
	private static SimpleDateFormat fm = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
	private static HttpClient client;
	public MyHandler(HttpClient client){
		this.client = client;
	}
	//all frondends
	private static final String FE1 = "";
	private static final String FE2 = "";
	private static final String FE3 = "";
	@Override
	public void handle(HttpServerRequest event){
		if(event.method() == HttpMethod.GET){
			executor.execute(new Runnable(){
				@Override
				public void run(){
					MultiMap map = event.params();
					String tweetId = map.get("tweetid");
					String op = map.get("op");
					String seq = map.get("seq");
					String field = map.get("field");
					String payload = map.get("payload");
					StringBuilder result = new StringBuilder(TEAM_ID + "," + TEAM_AWS_ACCOUNT_ID + "\n" );
					result.append(TEAM_ID)
					      .append(",")
					      .append(TEAM_AWS_ACCOUNT_ID)
					      .append("\n");
					
					try {
						result.append(sendRequest(tweetId, op, seq, field, payload));
					} catch (IOException e) {
						e.printStackTrace();
					}
				event.response().end(result.toString());
				}
			});
		}else{
			event.response().end("15619 is awesome");
		}
	}
	
	
	private static String sendRequest(String tweetId, String op, String seq, String field, String payload) throws IOException{
		StringBuilder sb = new StringBuilder();
		int id = Integer.valueOf(tweetId.substring(tweetId.length() - 1));
		String url = "";
		if(id%3 == 0){
			url = FE1;
		}else if(id%3 == 1){
			url = FE2;
		}else{
			url = FE3;
		}
		URL request = new URL(url);
		HttpURLConnection con = (HttpURLConnection) request.openConnection();
		con.setRequestMethod("GET");
		BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream()));
		String line = "";
		while((line = br.readLine()) != null){
			sb.append(line);
		}
		br.close();
		return sb.toString();
	}
}
