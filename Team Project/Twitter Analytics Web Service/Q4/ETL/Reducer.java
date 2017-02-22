import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.json.JSONObject;


public class Reducer {

	public static void main(String[] args) throws IOException {

		//load entries
		String line, tweetId;
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String lastTweetId = null;
		String[] entries = new String[7];
		while((line = br.readLine()) != null){
			entries = line.split("\\s+");
			tweetId = entries[0].trim();
			if(tweetId.equals(lastTweetId)){
				continue; //this tweet is duplicate
			}
			String jsonObject = line.substring(line.indexOf("{"), line.length());
            JSONObject output = new JSONObject(jsonObject);
            output.put("tweetId", tweetId);
			//update latest tweet id
			lastTweetId = tweetId;
			// output result
			System.out.println(output.toString());
		}
	}

}
