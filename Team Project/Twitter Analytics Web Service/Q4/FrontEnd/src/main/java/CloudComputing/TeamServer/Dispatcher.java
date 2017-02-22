package CloudComputing.TeamServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class Dispatcher {

    public static String FORWARD(String dns, String tweetid, String field, String op, String seq, String payload) throws IOException {
        return Dispatcher.URLHandler(String.format("http://%s:8080/get?tweetid=%s&field=%s&op=%s&seq=%s&payload=%s", dns, tweetid, field, op, seq, payload));
    }
    
    public static String READ(String dns, String tweetid, String field, String op, String seq, String payload) throws IOException {
        return Dispatcher.URLHandler(String.format("http://%s:8080/get?tweetid=%s&field=%s&op=%s&seq=%s&payload=%s", dns, tweetid, field, op, seq, payload));
    }

    private static String URLHandler(String url) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        try {
            URL uRL = new URL(url);
            HttpURLConnection httpURLConnection = (HttpURLConnection)uRL.openConnection();
            if (httpURLConnection.getResponseCode() != 200) {
                throw new IOException(httpURLConnection.getResponseMessage());
            }
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(httpURLConnection.getInputStream()));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                stringBuilder.append(line);
                stringBuilder.append("\n");
            }
            bufferedReader.close();
            httpURLConnection.disconnect();
        }
        catch (MalformedURLException e) {
            e.printStackTrace();
        }
        return stringBuilder.toString();
    }

}
