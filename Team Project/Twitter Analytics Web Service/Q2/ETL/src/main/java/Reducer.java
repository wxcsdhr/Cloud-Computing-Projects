import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONObject;

import java.util.*;

public class Reducer {

	private static final String REGEX1 = "[^a-zA-Z0-9]+";
	private static final String REGEX2 = "(https?|ftp)://[^\\t\\r\\n /$.?#][^\\t\\r\\n ]*";
	private static final String SPLITER = " 003giarc";
    private static final String RLSPLITER = "&rnl&";
    private static final String NLSPLITER = "&nnl&";
	
	public static String findCensoreText(String text, List<String> ROT13List) throws IOException{
		String tempText = text.toLowerCase();
		String[] tempTextArray = tempText.split(REGEX1);
		int stringLength = 0;
		int startIndex;
		int endIndex = 0;
		String subString = null;
		String subReplace = null;
		String stringRegex = null;
		for(String s : tempTextArray){
			startIndex = -1;
			stringRegex = "(?<=^|[^a-zA-Z0-9])" + s + "(?=[^a-zA-Z0-9]|$)";
			stringLength = s.length();
			Pattern pattern = Pattern.compile(stringRegex);
			Matcher matcher = pattern.matcher(tempText);
			if(matcher.find()){
				startIndex = matcher.start();
			}
			if(ROT13List.contains(s) && startIndex > -1){
					endIndex = startIndex + stringLength;
					subString = "(?<=^|[^a-zA-Z0-9])" + text.substring(startIndex, endIndex) + "(?=[^a-zA-Z0-9]|$)";
					subReplace = text.charAt(startIndex)
							  + String.valueOf(new char[stringLength - 2]).replaceAll("\0", "*")
							  + text.charAt(endIndex - 1);
					text = text.replaceAll(subString, subReplace); /// still troublesome
					tempText = text.toLowerCase();
				}
			}
		return text;
	}
	
	public static int calculateEwCount(String[] textArray, List<String>stopwords){
		int sum = 0;
		for(int i = 0; i < textArray.length; i ++){
			if(!stopwords.contains(textArray[i].trim()) && textArray[i].length() > 0){
				sum++;
			}
		}
		return sum;
	}
	
	public static int calculateSentimentScore(String[] text, Map<String, Integer> map){
		int sum = 0;
		String currentString = null;
		for(int i = 0; i < text.length; i++){
			currentString = text[i];
			if(map.containsKey(currentString)){
				sum += map.get(currentString);
			}
		}
		return sum;
	}
	public static void main(String[] args) throws IOException {
		//load AFINN file
		BufferedReader br = new BufferedReader(new FileReader("AFINN"));
		String line;
		Map<String, Integer> AFINNMap = new HashMap<String, Integer>();
		while((line = br.readLine()) != null){
			AFINNMap.put(line.split("\t")[0], Integer.parseInt(line.split("\t")[1]));
		}
		br.close();
		
		//load ROT13 file
		br = new BufferedReader(new FileReader("go_flux_yourself"));
		List<String> ROT13List = new ArrayList<String>();
		while((line = br.readLine()) != null){
			char[] currentArray = line.toCharArray();
			for(int i = 0; i < currentArray.length; i++){
				char currentChar = currentArray[i];
				if(currentChar > 96 && currentChar - 13 < 97){
					currentArray[i] = (char)(26 + currentChar - 13);
				}else if(currentChar > 96){
					currentArray[i] = (char)(currentChar - 13);
				}
			}
			ROT13List.add(String.valueOf(currentArray));
		}
		br.close();
		//load stopwords
		br = new BufferedReader(new FileReader("stopwords"));
		List<String> stopwords = new ArrayList<String>();
		while((line = br.readLine()) != null){
			stopwords.add(line.trim());
		}
		br.close();
		
		//load entries
		br = new BufferedReader(new InputStreamReader(System.in));
		String tweetId, userId, createdAt, censoredText, text;
		String lastTweetId = null;
		int retweet_count, favorite_count, followers_count, 
		    impactScore, sentimentScore, influentialScore, ewCount;
		String[] entries = new String[7];
		while((line = br.readLine()) != null){
			entries = line.split(SPLITER);
			tweetId = entries[0].trim();
			
			if(tweetId.equals(lastTweetId)){
				continue; //this tweet is duplicate
			}
			
			userId = entries[1].trim();
			createdAt = entries[2].trim();
            text = entries[3].trim().replaceAll(RLSPLITER, "\r")
                                    .replaceAll(NLSPLITER, "\n");
			String[] textArray = text.replaceAll(REGEX2, "")
					                       .toLowerCase()
					                       .split(REGEX1);
                
			retweet_count = Integer.parseInt(entries[4].trim());
			favorite_count = Integer.parseInt(entries[5].trim());
			followers_count = Integer.parseInt(entries[6].trim());
			sentimentScore = calculateSentimentScore(textArray, AFINNMap);
			ewCount = calculateEwCount(textArray, stopwords);
			influentialScore = ewCount * (favorite_count + retweet_count + followers_count);
			impactScore = sentimentScore + influentialScore;
			censoredText = findCensoreText(text, ROT13List);

			
			//update latest tweet id
			lastTweetId = tweetId;
			// output result
			JSONObject result = new JSONObject();
			result.put("tweetId", tweetId);
			result.put("userId", userId);
			result.put("impactScore", impactScore);
			result.put("createdAt", createdAt);
			result.put("text", text);
			result.put("censoredText", censoredText);
			System.out.println(result.toString());
		}
	}

}
