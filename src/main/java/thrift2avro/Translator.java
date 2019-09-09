package thrift2avro;

import com.snowplowanalytics.snowplow.CollectorPayload.thrift.model1.CollectorPayload;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.log4j.Logger;
import thrift2avro.avro.CollectorPayloadAvroSerdeProvider;
import thrift2avro.thrift.CollectorPayloadThriftSerdeProvider;

import java.io.IOException;
import java.util.Properties;

public class Translator {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String OFFSET_RESET = "earliest";

    private static final Logger LOG = Logger.getLogger(Translator.class.getName());
    private static final Schema schema = readSchema();

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "ga-splitter-3");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET);

        StreamsBuilder builder = new StreamsBuilder();


        builder
          .stream("ga-success", Consumed.with(Serdes.String(), new CollectorPayloadThriftSerdeProvider()))
          .mapValues(Translator::repackage)
          .filterNot((kye, value) -> value.get("ip").equals("qa-client-ip-address"))
          .to("ga-parsed3", Produced.with(Serdes.String(), CollectorPayloadAvroSerdeProvider.recordSerde));

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static GenericRecord repackage(CollectorPayload message) {
        LOG.info("Got payload: " + message);
        final GenericRecord record = new GenericData.Record(schema);
        record.put("ip", message.getIpAddress());
        record.put("browser", message.getUserAgent());
        LOG.info("Repackaged into: " + record);
        return record;
    }

    private static Schema readSchema() {
        try {
            return new Schema.Parser().parse(Translator.class.getResourceAsStream("/gaGeneric.avsc"));
        } catch (IOException exc) {
            throw new RuntimeException("Failed to read schema file: ", exc);
        }
    }
}
