package thrift2avro.avro;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;

import java.util.Collections;

public class CollectorPayloadAvroSerdeProvider {

    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    public static final Serde<GenericRecord> recordSerde = createRecordSerde();

    private static Serde<GenericRecord> createRecordSerde() {
        GenericAvroSerde genericAvroSerde = new GenericAvroSerde();
        genericAvroSerde.configure(
          Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL),
          false);
        return genericAvroSerde;
    }

}
