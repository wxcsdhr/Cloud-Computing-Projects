import java.io.BufferedReader;
import java.io.FileReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class query2WarmUp{
	final static long NANOSEC_PER_SEC = 1000l*1000*1000;
	static long startTime = System.nanoTime();
	static String url = "http://Q4ELB-1331338084.us-east-1.elb.amazonaws.com";
	static List<String> entry = new ArrayList<String>();
//	static private AtomicInteger count = new AtomicInteger(0)
	private static void sendGet() throws Exception{
		while (true){
			for(AtomicInteger i = new AtomicInteger(0); i.get() < entry.size(); i.incrementAndGet()){
				URL obj = new URL(entry.get(i.intValue()));
				try{
					HttpURLConnection con = (HttpURLConnection) obj.openConnection();
					con.setRequestMethod("GET");
					
					int responseCode = con.getResponseCode();
				}catch (Exception e){
					continue;
				}
			}
			// BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream()));
//			count.addAndGet(1);
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		String line;
		BufferedReader br = new BufferedReader(new FileReader("query2Load"));
		
		while((line = br.readLine()) != null){
			StringBuilder request = new StringBuilder();
			String[] array = line.split("\\s+");
			request.append(url)
			       .append("/q2?userid1=")
			       .append(array[0])
			       .append("&userid2=")
			       .append(array[1])
			       .append("&n=")
			       .append(array[2]);
			entry.add(request.toString());
		}
		ExecutorService pool = Executors.newFixedThreadPool(4);
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
