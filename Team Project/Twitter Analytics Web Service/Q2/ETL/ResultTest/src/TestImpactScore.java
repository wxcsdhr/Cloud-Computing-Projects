import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.json.JSONObject;

public class TestImpactScore {

	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String line;
		while((line = br.readLine()) != null){
			JSONObject context = new JSONObject(line);
			int impactScore = context.getInt("impactScore");
			String id=  context.getString("tweetId");
			System.out.println(id + " " + impactScore);
		}
	}

}
