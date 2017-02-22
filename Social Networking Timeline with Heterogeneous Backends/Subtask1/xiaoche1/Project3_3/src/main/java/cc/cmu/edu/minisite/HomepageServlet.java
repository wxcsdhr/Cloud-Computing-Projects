package cc.cmu.edu.minisite;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.bson.Document;
import org.json.JSONObject;
import org.json.JSONArray;

import static com.mongodb.client.model.Sorts.ascending;


public class HomepageServlet extends HttpServlet {

    MongoClient mongoClient = new MongoClient(new ServerAddress("ec2-52-90-24-137.compute-1.amazonaws.com", 27017));
    MongoDatabase db = mongoClient.getDatabase("task3");
    
    public HomepageServlet() {

    }


    @Override
    protected void doGet(final HttpServletRequest request, 
            final HttpServletResponse response) throws ServletException, IOException {

        String id = request.getParameter("id");
        String collections = "posts";
        FindIterable<Document> it = db.getCollection(collections).find(new Document("uid", Integer.parseInt(id))).sort(ascending("timestamp"));
        JSONArray posts = new JSONArray();
        JSONObject result = new JSONObject();
        for(Document document:it){
        	posts.put(new JSONObject(document.toJson()));
        }
        /*
            Task 3:
            Implement your logic to return all the posts authored by this user.
            Return this posts as-is, but be cautious with the or     der.

            You will need to sort the posts by Timestamp in ascending order
	     (from the oldest to the latest one). 
        */
        
        PrintWriter writer = response.getWriter();
        result.put("posts", posts);
        writer.write(String.format("returnRes(%s)", result.toString()));
        writer.close();
    }

    @Override
    protected void doPost(final HttpServletRequest request, 
            final HttpServletResponse response) throws ServletException, IOException {
        doGet(request, response);
    }
}

