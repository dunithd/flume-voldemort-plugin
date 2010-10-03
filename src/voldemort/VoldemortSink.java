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
import voldemort.versioning.Versioned;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * This allows you to use Voldemort as a Flume sink.
 *
 * Voldemort is a key-value storage system so that each event being stored requires a unique key.
 * A given key can have three granularity levels as DAY,HOUR and MINUTE. These levels can be configured when
 * constructing the sink.
 *
 * @author Dunith Dhanushka, dunithd@gmail.com, dunithd.wordpress.com
 */
public class VoldemortSink extends EventSink.Base {

    static org.apache.log4j.Logger logger = Logger.getLogger(VoldemortSink.class);

    private String storeName = "test";
    private String bootstrapUrl = "tcp://localhost:6666";
    private String granularityLevel = "DAY"; //key space granularity level. Defaults to "DAY"

    private StoreClientFactory factory;
    private StoreClient client;

    /**
     * This is the Voldemort sink for Flume.
     * @param bootstrapUrl bootstrap URL of an active Voldemort instance
     * @param storeName name of the Voldemort store which used to store log entries
     * @param granularityLevel granularity of the keys
     */
    public VoldemortSink(String bootstrapUrl, String storeName,String granularityLevel) {
        this.bootstrapUrl = bootstrapUrl;
        this.storeName = storeName;
        this.granularityLevel = granularityLevel;
    }

    /**
     * Opens a connection to the Voldemort instance
     * @throws IOException
     */
    @Override
    public void open() throws IOException {
        this.factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));
        this.client = factory.getStoreClient(storeName);
    }

    /**
     * Insert an incoming event object into Voldemort
     * @param e  Event which has been sent by Flume source
     * @throws IOException
     */
    @Override
    public void append(Event e) throws IOException {
        //first, generate a key based on the specified granularity
        String key = generateKey(this.granularityLevel);

        //check whether the given key has a value stored in Voldemort
        Versioned<String> version = client.get(key);
        String existingValue  = "";
        if(version != null) {
            existingValue = version.getValue();
        }

        //prepare a single line log entry from event
        String value = formatLogEntry(e);
        existingValue = existingValue.concat(value);    //append the new entry to old entry      

        client.put(key,existingValue);  //finally, perform PUT operation on Voldemort
    }

    /**
     * Close the connection to Voldemort
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        logger.debug("Voldemort sink is being shutting down...");
        factory.close();
    }

    /**
     * Construct a new parameterized sink
     * @return VoldemortSink
     */
    public static SinkFactory.SinkBuilder builder() {
        return new SinkFactory.SinkBuilder() {
            @Override
            public EventSink build(Context context, String... argv) {
                if (argv.length < 3) {
                    throw new IllegalArgumentException(
                            "usage: voldemortSink(\"bootstrapURL\", \"storeName\",\"key space granualirity\"...");
                }
                return new VoldemortSink(argv[0],argv[1],argv[2]);
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
     * Returns a String representing the current date to be used as a key.  This has the format "YYYYMMDDHH".
     * Format depends on the user specified granularity level.
     * @param granularity "DAY|HOUR|MINUTE"
     */
    private String generateKey(String granularity) {
        String key;
        String pattern = "yyyyMMdd";
        if(granularity.equalsIgnoreCase("DAY")) {
            pattern = "yyyyMMdd";
        } else if(granularity.equalsIgnoreCase("HOUR")) {
            pattern+="HH";
        } else if(granularity.equalsIgnoreCase("MINUTE")) {
            pattern+="HHmm";
        }
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        key = format.format(new Date());
        return key;
    }

    /**
     * Format the Event object into a standard single line log entry.
     * @param e Event
     * @return formatted single line log entry
     */
    private String formatLogEntry(Event e) {
        Date date = new Date(e.getTimestamp());
        String host = e.getHost();
        Event.Priority priority = e.getPriority();
        String message = new String(e.getBody());

        StringBuffer eventInfo = new StringBuffer();
        eventInfo.append(date.toString());    //append time stamp
        eventInfo.append(' ');

        eventInfo.append(host); //append host name
        eventInfo.append(' ');

        eventInfo.append(priority.name());  //append priority
        eventInfo.append(' ');

        eventInfo.append(message);  //append log message
        eventInfo.append('|');  // add pipe delimiter to denote the EOL
        return eventInfo.toString();
    }

}
