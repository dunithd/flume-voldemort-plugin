package voldemort;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.util.Pair;
import org.apache.log4j.Logger;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;


public class VoldemortSink extends EventSink.Base {

    static Logger logger = Logger.getLogger(VoldemortSink.class);

    private String host = "localhost";
    private int port = 6666;
    private String storeName = "test";
    private String bootstrapUrl = "tcp://";

    private StoreClientFactory factory;
    private StoreClient client;

    public VoldemortSink(String host,int port, String storeName) {
        this.host = host;
        this.port = port;
        this.storeName = storeName;
        if(!host.contains("tcp://")) {
            this.bootstrapUrl += host + ":" + port;
        } else {
            this.bootstrapUrl = host + ":" + port;
        }
    }

    @Override
    public void open() throws IOException {
        this.factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));
        this.client = factory.getStoreClient(storeName);
    }

    @Override
    public void append(Event e) throws IOException {
        String key = generateKey();  //generate a time stamp  based key

        //now prepare the event body with nice formatting
        String date = key;
        String host = e.getHost();

        StringBuffer eventMetaInfo = new StringBuffer();
        eventMetaInfo.append(date);
        eventMetaInfo.append(' ');
        eventMetaInfo.append(host);
        eventMetaInfo.append(' ');

        byte[] eventBody = e.getBody();
        byte[] eventMetaBytes = eventMetaInfo.toString().getBytes();
        byte[] fullEventBody = new byte[eventMetaBytes.length + eventBody.length];

        //Combine event meta data and event body together
        //first, append event meta data into final byte array
        for(int i = 0; i < eventMetaBytes.length; i++) {
            fullEventBody[i] = eventMetaBytes[i];
        }

        //then append event body into final byte array
        for(int i = 0; i < eventBody.length; i++) {
            fullEventBody[i + eventMetaBytes.length] = eventBody[i];
        }
        String value = new String(fullEventBody);   //this is the final value
        client.put(key,value);
        System.out.println("Key: " + key + " Value: "+value);
    }

    @Override
    public void close() throws IOException {
        logger.debug("Voldemort sink is being shutting down...");
    }

    public static SinkFactory.SinkBuilder builder() {
        return new SinkFactory.SinkBuilder() {
            // construct a new parameterized sink
            @Override
            public EventSink build(Context context, String... argv) {
                if (argv.length < 3) {
                    throw new IllegalArgumentException(
                            "usage: voldemortSink(\"host\", \"port\",\"storeName\"...");
                }
                return new VoldemortSink(argv[0],Integer.parseInt(argv[1]),argv[2]);
            }
        };
    }

    /**
     * This is a special function used by the SourceFactory to pull in this class
     * as a plugin sink.
     */
    public static List<Pair<String, SinkFactory.SinkBuilder>> getSinkBuilders() {
        List<Pair<String, SinkFactory.SinkBuilder>> builders =
                new ArrayList<Pair<String, SinkFactory.SinkBuilder>>();
        builders.add(new Pair<String, SinkFactory.SinkBuilder>("voldemortSink", builder()));
        return builders;
    }

    /**
     * Returns a String representing the current date to be used as
     * a key.  This has the format "YYYYMMDDHH".
     */
    private String generateKey() {
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMddhhmmS");
		String timestamp = format.format(new Date());
		return timestamp;
	}
}
