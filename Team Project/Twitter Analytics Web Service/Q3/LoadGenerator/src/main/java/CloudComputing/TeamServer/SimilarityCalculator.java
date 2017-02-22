package CloudComputing.TeamServer;

import java.util.HashMap;
import java.util.Map;
import java.util.List;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Tuple2;

import org.json.JSONObject;
import org.json.JSONArray;


public class SimilarityCalculator {
	
	public SimilarityCalculator(){

	}

	public int calculate(JSONArray tweetsOfUser1, JSONArray tweetsOfUser2, List<String> sharedWords){
                Map<String, Tuple2<Integer,Integer>> wordCount = new HashMap<>();

                JSONObject obj = tweetsOfUser1.getJSONObject(0);
                String countMap = obj.getString("wordCount");
                String[] strs = countMap.split(":");

                for (int j = 0;j < strs.length - 1;j+=2) {
                     Tuple2<Integer, Integer> t2 = wordCount.get(strs[j]);
                     if (t2 == null) {
                         wordCount.put(strs[j], new Tuple2<Integer, Integer>(Integer.parseInt(strs[j+1].trim()), 0));
                     } else {
                         wordCount.put(strs[j], new Tuple2<Integer, Integer>(Integer.parseInt(strs[j+1].trim()) + wordCount.get(strs[j])._1(), 0));
                     }
                }

                obj = tweetsOfUser2.getJSONObject(0);
                countMap = obj.getString("wordCount");
                strs = countMap.split(":");
                for (int j = 0;j < strs.length - 1;j+=2) {
                    Tuple2<Integer, Integer> t2 = wordCount.get(strs[j]);
                    if (t2 == null) {
                        wordCount.put(strs[j], new Tuple2<Integer, Integer>(0, Integer.parseInt(strs[j+1].trim())));
                    } else {
                        wordCount.put(strs[j], new Tuple2<Integer, Integer>(wordCount.get(strs[j])._1(), Integer.parseInt(strs[j+1].trim()) + wordCount.get(strs[j])._2()));
                    }
                }
              
                sharedWords.addAll(wordCount.entrySet().stream()
                                .filter(item -> item.getValue()._1() > 0 && item.getValue()._2() > 0)
                                .map(item -> item.getKey())
                                .collect(Collectors.toList()));

                return wordCount.entrySet().stream()
                                .filter(item -> item.getValue()._1() > 0 && item.getValue()._2() > 0)
                                .mapToInt(item -> item.getValue()._1() * item.getValue()._2())
                                .sum();
    }
}
