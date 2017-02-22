package CloudComputing.TeamServer;

import org.json.JSONArray;
import org.json.JSONObject;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Arrays;

import java.util.List;
import java.util.PriorityQueue;
import java.util.Comparator;

public class ResultOrganizer{
	
	public String splitter = ":";
	public String linebreak = "\n";

	public String response(String TEAM_ID, String TEAM_AWS_ACCOUNT_ID, JSONArray tweetsOfUser1, JSONArray tweetsOfUser2, List<String> sharedWords, int similarity, int n) {
//		long t1 = System.currentTimeMillis();
		StringBuilder ret = new StringBuilder();

		ret.append(TEAM_ID).append(",").append(TEAM_AWS_ACCOUNT_ID).append(linebreak)
			.append(String.valueOf(similarity)).append(linebreak)
			.append(String.join("\t", sharedWords)).append(linebreak)
			.append(buildLine(tweetsOfUser1, sharedWords, n))
			.append(buildLine(tweetsOfUser2, sharedWords, n));

		return ret.toString();
	}

	private String buildLine(JSONArray tweets, List<String> sharedWords, int n) {
		StringBuilder ret = new StringBuilder();

		for (int i = 0; i < tweets.length() && n > 0; i++){
			JSONObject obj = tweets.getJSONObject(i);
        	if (shouldInclude(obj.getString("censoredText"), sharedWords)){
        		n--;
				ret.append(buildLine(obj));
			}
		}
		return ret.toString();
	}

	private boolean shouldInclude(String text, List<String> sharedWords){
		List<String> words = new LinkedList<String>(Arrays.asList(text.toLowerCase().replaceAll("(https?|ftp)://[^\\t\\r\\n /$.?#][^\\t\\r\\n ]*","").split("[^*a-zA-Z0-9]")));
		words.retainAll(sharedWords);
		return words.size() > 0;
	}

	private String buildLine(JSONObject json) {
		StringBuilder ret = new StringBuilder();

		try{
		ret.append(json.get("impactScore")).append(splitter)
			.append(json.getString("tweetId")).append(splitter)
			.append(new SimpleDateFormat("yyyy-MM-dd HH-mm-ss").format((Date)(new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy")).parse(json.getString("createdAt")))).append(splitter)
			.append(json.getString("censoredText")
				    .replaceAll("&FS&",",")
				    .replaceAll("&TS&","!")
				    .replaceAll("&LFS&","#")
				    .replaceAll("\\\\n","\n")
				    .replaceAll("\\\\t","\t")
				    .replaceAll("\\\\r","\r"))
				    .append(linebreak);

		}catch(Exception e){
			e.printStackTrace();
		}
		return ret.toString();
	}

}
