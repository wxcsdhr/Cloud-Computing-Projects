import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class FileMerger {
	
	private static final int NUM = 5;
	
	private static Map<Integer, BufferedWriter> writers = new HashMap<Integer, BufferedWriter>();
	
	static {
		try {
			for (int i = 0;i < NUM;i++) {
				writers.put(i, new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/home/ubuntu/merged/merged-" + i), StandardCharsets.UTF_8)));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		for (String fileName : args) {
			Scanner scanner = new Scanner(new File(fileName), StandardCharsets.UTF_8.name());
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				long seed = Long.parseLong(line.split("#")[0]);
				writers.get(seed % NUM).write(line);
			}
			scanner.close();
		}
		for (int i = 0;i < NUM;i++) {
			writers.get(i).close();
		}
	}

}
