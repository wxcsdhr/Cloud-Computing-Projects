import java.io.BufferedReader;
import java.io.FileReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class query3WarmUp{
	final static long NANOSEC_PER_SEC = 1000l*1000*1000;
	static long startTime = System.nanoTime();
	static AtomicInteger ai = new AtomicInteger(0);
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
	
	public static String convert(String date) {
		StringBuilder sb = new StringBuilder();
		sb.append(date.substring(0, 4))
		  .append(date.substring(4, 6))
		  .append(date.substring(6));
		return sb.toString();
	}
	
	public static void main(String[] args) throws Exception {
		String line;
		BufferedReader br = new BufferedReader(new FileReader("query3Load"));
		
		while((line = br.readLine()) != null){
			StringBuilder request = new StringBuilder();
			String[] array = line.split("\\s+");
			String p1 = array[0];
			String p2 = array[1];
			String p3 = array[2];
			String date_start = convert(array[3]);
			String date_end = convert(array[4]);
			String tid_start = array[5];
			String tid_end = array[6];
			String uid_start = array[7];
			String uid_end = array[8];
			request.append(url)
			       .append("/q3?date_start=")
			       .append(date_start)
			       .append("&date_end=")
			       .append(date_end)
			       .append("&tid_start=")
			       .append(tid_start)
			       .append("&tid_end=")
			       .append(tid_end)
			       .append("&uid_start=")
			       .append(uid_start)
			       .append("&uid_end=")
			       .append(uid_end)
			       .append("&p1=")
			       .append(p1)
			       .append("&p2=")
			       .append(p2)
			       .append("&q3=")
			       .append(p3);
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
