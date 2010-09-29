package voldemort;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;


public class TestVoldemortSink {

    private static final String HOST = "tcp://localhost:6666";
    private static final String STORE_NAME = "test";
    private VoldemortSink sink;

    final static Logger logger = Logger.getLogger(TestVoldemortSink.class);

    @Before
    public void setUp() throws Exception {
        Logger.getRootLogger().setLevel(Level.INFO);
    }

    @Test
    public void testBuilder() throws FlumeSpecException {
        Exception ex = null;
        try {
            String src = "voldemortSink";
            FlumeBuilder.buildSink(new Context(), src);
        } catch (Exception e) {
            return;
        }
        assertNotNull("No exception thrown!", ex != null);

        // dir / filename
        String src2 = "voldemortSink(\"localhost\",6666,\"test\")";
        FlumeBuilder.buildSink(new Context(), src2);
    }

    @Test
    public void testOpen() throws Exception {
        String src2 = "voldemortSink(\"localhost\",6666,\"test\")";
        EventSink sink = FlumeBuilder.buildSink(new Context(), src2);
        try {
            sink.open();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
           sink.close(); 
        }
    }

    @Test
    public void testAppend() throws Exception {
        String body = "This is just a message";
        Event event = new EventImpl(body.getBytes());

        String src = "voldemortSink(\"localhost\",6666,\"test\")";
        EventSink sink = FlumeBuilder.buildSink(new Context(), src);

        sink.open();
        sink.append(event);

    }
}