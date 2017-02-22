import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.json.JSONArray;
import org.json.JSONObject;

public class Reducer {

	public static void main(String[] args) throws IOException{
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String line;
		String lastRowKey = null;
		String currentRowKey;
		String tweetId;
		String wordCount;
		List<JSONObject> list = new ArrayList<JSONObject>();
		while((line = br.readLine()) != null){
			if(line.length() == 0){
				continue;
			}
			JSONObject tweet= new JSONObject();
			currentRowKey = line.split("\\s+")[0];
			tweetId = line.split("\\s+")[1];
			wordCount = line.split("\\s+")[2];
			tweet.put(tweetId, wordCount);
			if(!currentRowKey.equals(lastRowKey) && lastRowKey != null){
				StringBuilder sb = new StringBuilder();
				sb.append(lastRowKey);
				sb.append(",");
				JSONArray jsonArray = new JSONArray();
				for(int i = 0; i < list.size(); i++){
					jsonArray.put(list.get(i));
				}
				sb.append(jsonArray);
				System.out.println(sb.toString());
				list = new ArrayList<JSONObject>();
			}
			list.add(tweet);
			lastRowKey = currentRowKey;
		}
		StringBuilder sb= new StringBuilder();
		sb.append(lastRowKey);
		sb.append(",");
		JSONArray jsonArray = new JSONArray();
		for(int i = 0; i < list.size(); i++){
			jsonArray.put(list.get(i));
		}
		sb.append(jsonArray);
		System.out.println(sb.toString());
	}

}
