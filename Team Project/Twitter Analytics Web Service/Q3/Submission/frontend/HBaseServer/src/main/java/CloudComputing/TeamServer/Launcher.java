package CloudComputing.TeamServer;

import java.io.IOException;

public class Launcher{

	public static void main(String[] args) throws IOException {
		new HBaseServer().start();
	}
}
