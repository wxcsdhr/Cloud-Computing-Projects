import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.json.JSONObject;
import org.json.JSONException;
public class Mapper {
	

	private static final String SPLITER = " 003giarc";
    private static final String RLSPLITER = "&rnl&";
    private static final String NLSPLITER = "&nnl&";
	public static void main(String[] args) throws IOException{
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String line = null;
		//Start maping
		while(true){
			line = br.readLine();
			if(line == null){
				break;
			}
			try{
				JSONObject tweet = new JSONObject(line);
				
				//remove malformed tweets
				if((!tweet.has("id") && !tweet.has("id_str")) && 
				   !tweet.has("created_at") && !tweet.has("text")){
					continue;
				}
				
				
				String tweetContent = tweet.getString("text");
				if(tweetContent.length() == 0){
					continue;
				}
				String lang = tweet.getString("lang");
				if(!lang.equals("en") || lang.length() ==0){
					continue;
				}
				// get tweet_id
				String tweetId;
				if(tweet.has("id")){
					tweetId = Long.toString(tweet.getLong("id"));
				}else{
					tweetId = tweet.getString("id_str");
				}
				
				//get User id
				String userId = null;
				if(tweet.getJSONObject("user").has("id_str")){
					userId = tweet.getJSONObject("user").getString("id_str");
				}else{
					userId = Long.toString(tweet.getJSONObject("user").getLong("id"));
				}
				
				
				//get followers_count;
				int followers_count = 0;
				if(tweet.getJSONObject("user").has("followers_count")){
					followers_count = tweet.getJSONObject("user").getInt("followers_count");
				}
				
				//get create time
				String createdAt = tweet.getString("created_at");
				
				//get text content
				String text = tweet.getString("text")
						           .replaceAll("\n", NLSPLITER)
						           .replaceAll("\r", RLSPLITER);
				
				//get favorite_count
				int favorite_count = 0;
				if(tweet.has("favorite_count")){
					favorite_count = tweet.getInt("favorite_count");
				}
				
				//get retweet_count
				int retweet_count = 0;
				if(tweet.has("retweet_count")){
					retweet_count = tweet.getInt("retweet_count");
				}
				
				//output result
				System.out.println(tweetId + SPLITER
								 + userId + SPLITER
						         + createdAt + SPLITER
						         + text + SPLITER
						         + retweet_count + SPLITER
						         + favorite_count + SPLITER
						         + followers_count);

			}catch(JSONException e){
				continue;
			}
			
		}
	}
}
