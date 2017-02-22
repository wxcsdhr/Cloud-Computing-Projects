package CloudComputing.TeamServer;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.Collections;
import java.util.Comparator;

import scala.Tuple2;

import org.json.JSONObject;
import org.json.JSONArray;


public class SimilarityCalculator {

    public SimilarityCalculator(){}

    public int calculate(JSONArray tweetsOfUser1, JSONArray tweetsOfUser2, List<String> sharedWords){
                Map<String, Tuple2<Integer,Integer>> wordCount = new HashMap<>();

                JSONObject wc = tweetsOfUser1.getJSONObject(tweetsOfUser1.length() - 1);
                Set<String> keys = wc.keySet();
                Tuple2<Integer, Integer> t2 = null;

                for (String key : keys){
                    if ("".equals(key) || key == null)
                        continue;

                    t2 = wordCount.get(key);
                    if (t2 == null) {
                        wordCount.put(key, new Tuple2<Integer, Integer>(wc.getInt(key), 0));
                    } else {
                        wordCount.put(key, new Tuple2<Integer, Integer>(wc.getInt(key) + t2._1(), 0));
                    }
                }

                wc = tweetsOfUser2.getJSONObject(tweetsOfUser2.length() - 1);
                keys = wc.keySet();

                for (String key : keys){
                    if ("".equals(key) || key == null)
                        continue;

                    t2 = wordCount.get(key);
                    if (t2 == null) {
                        wordCount.put(key, new Tuple2<Integer, Integer>(0, wc.getInt(key)));
                    } else {
                        wordCount.put(key, new Tuple2<Integer, Integer>(t2._1(), wc.getInt(key) + t2._2()));
                    }
                }

                sharedWords.addAll(wordCount.entrySet().stream()
                                .filter(item -> item.getValue()._1() > 0 && item.getValue()._2() > 0)
                                .map(item -> item.getKey())
                                .collect(Collectors.toList()));

                Collections.sort(sharedWords, new Comparator<String>(){

                        public int compare(String str1, String str2){
                                return str1.compareTo(str2);
                        }
                });

                return wordCount.entrySet().stream()
                                .filter(item -> item.getValue()._1() > 0 && item.getValue()._2() > 0)
                                .mapToInt(item -> item.getValue()._1() * item.getValue()._2())
                                .sum();
    }
}
