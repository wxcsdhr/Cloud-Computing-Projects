package CloudComputing.TeamServer;

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
	
	@Override
	public void handle(final HttpServerRequest event) {
		if (event.method() == HttpMethod.GET) {
			
			executor.execute(new Runnable(){
			@Override
				public void run() {
					MultiMap map = event.params();
					if (map.isEmpty()) {
						return;
					}

					try{
					    event.response().end(TEAM_ID + "," + TEAM_AWS_ACCOUNT_ID + "\n" + MySqlQuery.getInstance().queryRange(map));
					}catch(Exception e){
						e.printStackTrace();
					}
				}
			});
		} else {
			event.response().end("TEAM PUT");
		}
	}
}
	
