import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONObject;

public class myCensoredText {
	private static final String REGEX1 = "[^a-zA-Z0-9]+";
	private static final String REGEX2 = "(https?|ftp)://[^\\t\\r\\n /$.?#][^\\t\\r\\n ]*";
    private static final String FILEDSPLITER = "&FS&"; // used to replace all ,
    private static final String TWEETSPLITER = "&TS&"; // used to replace all !
    private static final String LARGESPLITER = "&LFS&";
	
	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader("go_flux_yourself"));
		
		//load ROT13 file
		 String line = null;
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
		 
		br = new BufferedReader(new InputStreamReader(System.in));
		while((line = br.readLine()) != null){
			JSONObject context = new JSONObject(line);
			String text = context.getString("text");
			String id=  context.getString("tweetId");
			text = findCensoreText(text, ROT13List);
			System.out.println(id + " " + text);
		}
	}
	
	public static String findCensoreText(String text, List<String> ROT13List) throws IOException{
	 	String tempText = text.toLowerCase();
	 	String[] tempTextArray = tempText.split(REGEX1);
        //check wether there is word inside array
	 	int stringLength = 0;
	 	int startIndex;
	 	int endIndex = 0;
	 	String subString = null;
	 	String subReplace = null;
	 	String stringRegex = null;
	 	StringBuilder removedString = new StringBuilder();
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
	 				removedString.append(s);
	 				removedString.append(",");
	 				if(endIndex == text.length() + 1){
	 					startIndex --;
	 					endIndex --;
	 				}
	 				
	 				subString = "(?<=^|[^a-zA-Z0-9])" + text.substring(startIndex, endIndex) + "(?=[^a-zA-Z0-9]|$)";
	 				subReplace = text.charAt(startIndex)
	 						  + String.valueOf(new char[stringLength - 2]).replaceAll("\0", "*")
	 						  + text.charAt(endIndex - 1);
	 				text = text.replaceAll(subString, subReplace); /// still troublesome
	 				tempText = text.toLowerCase();
	 		}
	 	}
 		
	 	return tempText;
	}
}
