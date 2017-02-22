import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

public class FileMerger {
	
	private static final long NUM = 5L;
	
	private static Map<Long, BufferedWriter> writers = new HashMap<Long, BufferedWriter>();
	
	static {
		try {
			for (long i = 0L;i < NUM;i++) {
				writers.put(i, new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/home/ubuntu/merged/merged-" + i))));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		for (String fileName : args) {
			BufferedReader reader = new BufferedReader(new FileReader(fileName));
			String line = null;
			while ((line = reader.readLine()) != null) {
				long seed = Long.parseLong(line.split("#")[0]);
				writers.get(seed % NUM).write(line + "\n");
			}
			reader.close();
		}
		for (long i = 0L;i < NUM;i++) {
			writers.get(i).close();
		}
	}

}