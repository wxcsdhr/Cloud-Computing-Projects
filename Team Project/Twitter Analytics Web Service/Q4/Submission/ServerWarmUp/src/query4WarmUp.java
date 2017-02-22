import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class query4WarmUp{
	static AtomicInteger ai = new AtomicInteger(0);
	static String url = "http://elb-614236269.us-east-1.elb.amazonaws.com";
	static List<String> entry = new ArrayList<String>();
//	static private AtomicInteger count = new AtomicInteger(0)
	private static void sendGet() throws Exception{
		while (true){
			for(AtomicInteger i = new AtomicInteger(0); i.get() < entry.size(); i.incrementAndGet()){
				URL obj = new URL(entry.get(i.intValue()));
//				System.out.println(i.get());
				try{
					HttpURLConnection con = (HttpURLConnection) obj.openConnection();
					con.setRequestMethod("GET");
					
					int responseCode = con.getResponseCode();
                    BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream()));
                    String line;
                    while((line = br.readLine()) != null){
                        System.out.println(line  + "\t" + entry.get(i.intValue()));
                    }
                    br.close();
				}catch (Exception e){
					continue;
				}
			}
			break;
//			count.addAndGet(1);
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		String line;
		BufferedReader br = new BufferedReader(new FileReader("query4LoadCopy"));
		
		while((line = br.readLine()) != null){
			StringBuilder request = new StringBuilder();
			String[] array = line.split("\\s+");
			request.append(url)
			       .append("/q4?tweetid=")
			       .append(array[0])
			       .append("&op=")
			       .append(array[1])
			       .append("&seq=")
			       .append(array[2])
			       .append("&field=")
			       .append(array[3])
			       .append("&payload=");
			entry.add(request.toString());
		}
//		ExecutorService pool = Executors.newFixedThreadPool(1);
//		for(int i = 0; i < 1; i++){
//			Runnable r = new Runnable(){
//				public void run(){
//					try {
						sendGet();
//					} catch (Exception e) {
//						e.printStackTrace();
//					}
//				}
//			};
//			pool.execute(r);
			
//		}
		
	}

}
