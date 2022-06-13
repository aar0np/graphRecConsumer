package graphRecConsumer;

import static com.datastax.dse.driver.api.core.graph.DseGraph.g;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.shade.com.google.gson.Gson;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.datastax.dse.driver.api.core.graph.FluentGraphStatement;
import com.datastax.oss.driver.api.core.CqlSession;

public class grConsumer {
	private static final String SERVICE_URL = System.getenv("ASTRA_STREAM_URL");
	private static final String YOUR_PULSAR_TOKEN = System.getenv("ASTRA_STREAM_TOKEN");
	private static final String STREAMING_TENANT = System.getenv("ASTRA_STREAM_TENANT");
	private static final String STREAMING_PREFIX = STREAMING_TENANT + "/default/";
	private static final String TOPIC = "persistent://" + STREAMING_PREFIX + "user-ratings";
	private static final String SUBSCRIPTION = "graph-recommendations";
	
	private static CqlSession session;
	
	public static void main(String[] args) {
		// Connect to DSE Graph
		DseDAL dse = new DseDAL();
		session = dse.getSession();
		
		// Create Pulsar/Astra Streaming client
		PulsarClient client = initializeClient(TOPIC);
		boolean receivedMsg = false;
		String strMessage = null;
		UserRating recommendation;
		
    	// consumer
    	while (!receivedMsg) {
    		try {
    			Consumer<String> consumer = client.newConsumer(Schema.STRING)
    					.topic(TOPIC)
    					.subscriptionName(SUBSCRIPTION)
    					.subscriptionType(SubscriptionType.Exclusive)
    					.subscribe();
    			
    			Message<String> msg = consumer.receive();
    			
    			if (msg != null) {
    				// Re-enable this ack when going beyond dev
    				// consumer.acknowledge(msg);
    				strMessage = msg.getValue();
    				
    				System.out.println(strMessage);
					receivedMsg = true;
    			}
    			
    			consumer.close();
    		} catch (Exception e) {
				System.out.println(e.toString());
				receivedMsg = true;
    		}
    	}
    	
    	// get properties from message
    	recommendation = new Gson().fromJson(strMessage, UserRating.class);
    	
    	Double starRating = recommendation.getRating();
    	int movieId = recommendation.getMovieId();
    	int userId = recommendation.getUserId();
    	Date timestamp = recommendation.getTimestamp();
    	
    	// create edge in graph
    	writeRecommendation(userId, movieId, starRating, timestamp);
    	
    	// exit
    	System.exit(0);
	}

	public static void writeRecommendation(int userId, int movieId, Double rating, Date timestamp) {
		
		GraphTraversal<Vertex, Edge> addEdge = g
				.V().has("User", "user_id", userId).as("userR")
				.V().has("Movie", "movie_id", movieId).as("movieR")
				.addE("rated")
					.from("userR").to("movieR")
					.property("rating",rating)
					.property("timestamp",timestamp);

		FluentGraphStatement stmt = FluentGraphStatement.newInstance(addEdge);
		stmt.setGraphName("movies_dev");
		session.execute(stmt);
	}
	
	private static String convertToJSON(String message) {
		return new Gson().toJson(message);
	}
	
	private static PulsarClient initializeClient(String topic) {
		System.out.println("topic=" + topic);

		try {
	        // Create client object
	        PulsarClient client = PulsarClient.builder()
	                .serviceUrl(SERVICE_URL)
	                .authentication(
	                    AuthenticationFactory.token(YOUR_PULSAR_TOKEN)
	                )
	                .build();
	       
	        return client;
		} catch (Exception e) {
		    System.out.println(e.toString());
			return null;
		}		
	}
}
