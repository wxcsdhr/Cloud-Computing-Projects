import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

public class Prefix {

	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String line;
		Set<String> set = new HashSet<String>();
		while((line = br.readLine()) != null){
			String[] content = line.split("\\s+");
			for(int i = 6; i < 9; i++){
				set.add(content[i]);
			}
		}
		for(String s: set){
			System.out.println(s);
		}
	}

}
