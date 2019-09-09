package thrift2avro.thrift;

import com.snowplowanalytics.snowplow.CollectorPayload.thrift.model1.CollectorPayload;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.util.Map;

class CollectorPayloadThriftSerializer implements Serializer<CollectorPayload> {

    private static final Logger LOG = Logger.getLogger(CollectorPayloadThriftSerializer.class.getName());

    private final TSerializer serializer = new TSerializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // nothing to configure
    }

    @Override
    public byte[] serialize(String topic, CollectorPayload data) {
        try {
            return serializer.serialize(data);
        } catch (TException e) {
            LOG.error("Failed to deserialize data in topic: " + topic, e);
        }
        return new byte[0];
    }


    @Override
    public void close() {
        // nothing to do
    }
}
