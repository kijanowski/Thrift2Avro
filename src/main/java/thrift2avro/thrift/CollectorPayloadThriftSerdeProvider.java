package thrift2avro.thrift;

import com.snowplowanalytics.snowplow.CollectorPayload.thrift.model1.CollectorPayload;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class CollectorPayloadThriftSerdeProvider implements Serde<CollectorPayload> {

    private final CollectorPayloadThriftDeserializer deserializer = new CollectorPayloadThriftDeserializer();
    private final CollectorPayloadThriftSerializer serializer = new CollectorPayloadThriftSerializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {
        deserializer.close();
        serializer.close();
    }

    @Override
    public Serializer<CollectorPayload> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<CollectorPayload> deserializer() {
        return deserializer;
    }
}
