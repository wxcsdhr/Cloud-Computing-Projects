import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class query1WarmUp {
	final static long NANOSEC_PER_SEC = 1000l*1000*1000;
	static long startTime = System.nanoTime();
//	static private AtomicInteger count = new AtomicInteger(0);
	private static void sendGet() throws Exception{
		String url = "http://elb-614236269.us-east-1.elb.amazonaws.com/q1?key=4024123659485622445001958636275419709073611535463684596712464059093821&message=JGNNFTNQYQ";
		while (true){
			URL obj = new URL(url);
			HttpURLConnection con = (HttpURLConnection) obj.openConnection();
			con.setRequestMethod("GET");
			try{
				int responseCode = con.getResponseCode();
				BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream()));
			}catch(Exception e){
				break;
			}
//			count.addAndGet(1);
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		
		ExecutorService pool = Executors.newFixedThreadPool(10);
		for(int i = 0; i <= 10; i++){
			Runnable r = new Runnable(){
				public void run(){
					try {
						sendGet();
//						System.out.println(count.toString());
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			};
			pool.execute(r);
			
		}
		
	}

}
