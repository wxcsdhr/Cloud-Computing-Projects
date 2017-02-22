package CloudComputing.ETL;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.json.JSONObject;


public class Mapper {

	private static final String REGEX1 = "[^a-zA-Z0-9]";
	private static final String REGEX2 = "(https?|ftp)://[^\\t\\r\\n /$.?#][^\\t\\r\\n ]*";
    
    public static String findWordCount(String[] textArray, Set<String> ignoreWords){
    	Map<String, Integer>map = new HashMap<String, Integer>();
    	String currentWord;
    	StringBuilder output = new StringBuilder();
    	for(int i =0; i < textArray.length; i++){
    		currentWord = textArray[i];
    		if(currentWord.trim().length() == 0){
    			continue;
    		}
    		if(ignoreWords.contains(currentWord)){
    			continue;
    		}
    		if(map.containsKey(currentWord)){
    			map.put(currentWord, map.get(currentWord) + 1);
    		}else{
    			map.put(currentWord, 1);
    		}
    	}
    	for(Map.Entry<String, Integer> entry:map.entrySet()){
    		output.append(entry.getKey());
    		output.append(":");
    		output.append(entry.getValue());
    		output.append(":");
    	}
    	if(output.length() == 0){
    		return "";
    	}else{
    		return output.substring(0,  output.length() - 1);
    	}
    }
    
	public static String extractDate(String timeStamp) throws ParseException{
		
		Date formerDate = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy").parse(timeStamp);
		SimpleDateFormat currFormate = new SimpleDateFormat("yyyyMMdd");
		return currFormate.format(formerDate).substring(3,8);
	}

	public static void main(String args[]) throws IOException, ParseException{
		BufferedReader br = new BufferedReader(new FileReader("go_flux_yourself"));
		
		//load ROT13 file
		 String line = null;
		 Set<String> ignoreWords = new HashSet<String>();
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
            if(currentArray.length > 0){
                ignoreWords.add(String.valueOf(currentArray));
            }
		 }
		 br.close();

        //load Stopwords
        br = new BufferedReader(new FileReader("stopwords"));
        while((line = br.readLine()) != null){
        	ignoreWords.add(line.trim());
        }
        br.close();

		//start map
		br = new BufferedReader(new InputStreamReader(System.in));
		String userId, tweetId, createdAt, text, wordCount, rowKey, date;
		String[] textArray;
		JSONObject entry;
		while((line = br.readLine()) != null){
			StringBuilder output = new StringBuilder();
		    entry = new JSONObject(line);
			userId = entry.getString("userId");
			tweetId = entry.getString("tweetId");
			text = entry.getString("text");
			textArray = text.replaceAll(REGEX2, "").toLowerCase().split(REGEX1);
			//do not count any tweet that does not have any word
			wordCount = findWordCount(textArray, ignoreWords);
            if(wordCount.length() == 0){
                continue;
            }
			createdAt = entry.getString("createdAt");
			date = extractDate(createdAt);
            //create new schema for rowkey.
			output.append(userId)
			      .append(date)
			      .append("\t")
			      .append(tweetId)
			      .append("\t")
			      .append(wordCount);
			System.out.println(output.toString());
		}

	}
}
