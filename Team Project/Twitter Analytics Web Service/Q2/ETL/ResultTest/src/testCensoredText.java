import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.json.JSONObject;

public class testCensoredText {

	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String line;
		while((line = br.readLine()) != null){
			JSONObject context = new JSONObject(line);
			String censoredText = context.getString("censoredText").toLowerCase();
			String id=  context.getString("id");
			System.out.println(id + " " + censoredText);
		}
	}

}
