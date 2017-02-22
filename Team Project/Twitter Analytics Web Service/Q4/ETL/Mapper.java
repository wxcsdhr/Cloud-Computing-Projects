import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.PriorityQueue;
import java.util.Queue;

import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;
public class Mapper {
	
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
				JSONObject output = new JSONObject();
				//remove malformed tweets
				if(!tweet.has("id") && !tweet.has("id_str")){
					continue;
				}

				//remove tweet without timestamp
				if(!tweet.has("created_at")){
					continue;
				}

				//do not have hashtags
				if(!tweet.has("entities") 
						&& !tweet.getJSONObject("entities").has("hashtags")){
					continue;
				}
				//do not have user name
				if(!tweet.has("user") && !tweet.getJSONObject("user").has("name")){
					continue;
				}
				// store all hashtags in a sorted dataset
				Queue<String> hashTagsQueue = new PriorityQueue<String>();
				JSONArray hashTagsArray = tweet.getJSONObject("entities").getJSONArray("hashtags");
				// put every text into the queue
				for(int i = 0; i < hashTagsArray.length(); i++){
					JSONObject hashTagText = hashTagsArray.getJSONObject(i);
					if(hashTagText.has("text")){
						String text = hashTagText.getString("text");
						hashTagsQueue.add(text);
					}
				}
				if(hashTagsQueue.isEmpty()){ //no hash tags
					continue;
				}

				//put every hashtag text into an JSONarray;
				JSONArray hashText = new JSONArray();
				while(!hashTagsQueue.isEmpty()){
					hashText.put(hashTagsQueue.poll());
				}
				output.put("hashtags", hashText);
				
				// get tweet_id
				String tweetId;
				if(tweet.has("id")){
					tweetId = Long.toString(tweet.getLong("id"));
				}else{
					tweetId = tweet.getString("id_str");
				}
				
				//get User name
				String userName = tweet.getJSONObject("user").getString("name");
				if(userName.length() != 0){
					output.put("userName", userName);
				}

				//get User id
				String userId = null;
				if(tweet.getJSONObject("user").has("id_str")){
					userId = tweet.getJSONObject("user").getString("id_str");
				}else{
					userId = Long.toString(tweet.getJSONObject("user").getLong("id"));
				}
				if(userId.length() != 0){
					output.put("userId", userId);
				}

				//get timestamp
				String createdAt = tweet.getString("created_at");
				if(createdAt.length() != 0){
					output.put("createdAt", createdAt);
				}
				
				//get text content
				String text = tweet.getString("text");
				if(text.length() != 0){
					output.put("text", text);
				}

				//output result
				System.out.println(tweetId + "\t" + output.toString());
			}catch(JSONException e){
				continue;
			}
			
		}
	}
}
