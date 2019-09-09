package thrift2avro.thrift;

import com.snowplowanalytics.snowplow.CollectorPayload.thrift.model1.CollectorPayload;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;

import java.util.Map;

class CollectorPayloadThriftDeserializer implements Deserializer<CollectorPayload> {

    private static final Logger LOG = Logger.getLogger(CollectorPayloadThriftDeserializer.class.getName());

    private final TDeserializer deserializer = new TDeserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // nothing to configure
    }

    @Override
    public CollectorPayload deserialize(String topic, byte[] data) {
        var collectorPayload = new CollectorPayload();
        try {
            deserializer.deserialize(collectorPayload, data);
        } catch (TException e) {
            LOG.error("Failed to deserialize data in topic: " + topic, e);
        }
        return collectorPayload;
    }

    @Override
    public void close() {
        // nothing to do
    }
}
