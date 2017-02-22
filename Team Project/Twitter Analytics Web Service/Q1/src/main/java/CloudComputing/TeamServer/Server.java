package CloudComputing.TeamServer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;

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
	private static PDC decrypt = new PDC();
	private static SimpleDateFormat fm = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");

	@Override
	public void handle(HttpServerRequest event) {
		if (event.method() == HttpMethod.GET) {
			executor.execute(new Runnable(){
			@Override
				public void run() {
					MultiMap map = event.params();
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
				}
			});
		} else {
			event.response().end("This is just a test for PUT");
		}
	}
}
	
