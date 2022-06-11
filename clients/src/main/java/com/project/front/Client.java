package com.project.front;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.specific.SpecificRecordBuilderBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command()
public class Client {

    private KafkaProducer<String, GenericRecord> producer;
    private Schema schema;

    public Client(KafkaProducer<String, GenericRecord> producer, Schema schema) {
        this.producer = producer;
        this.schema = schema;
    }

    @Command(name = "calculate")
    public int calculusCommand(
            @Parameters(paramLabel = "EXPRESSION", arity = "1") String expression) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("expression", expression);
        sendRecord(record);
        return 0;
    }

    @Command(name = "compress-image")
    public int imageCompressionCommand(
            @Parameters(paramLabel = "URL", arity = "1..*") URL[] urls,
            @Option(names = { "--quality", "-q" }, defaultValue = "0.9") float quality) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("urls", Arrays.stream(urls).map(URL::toString).toArray(String[]::new));
        record.put("quality", quality);
        sendRecord(record);
        return 0;
    }

    @Command(name = "format-text")
    public int textFormattingCommand(
            @Parameters(paramLabel = "FORMATTING") String formatting,
            @Parameters(paramLabel = "TEXT") String text) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("formatting", formatting);
        record.put("text", text);
        sendRecord(record);
        return 0;
    }

    private void sendRecord(GenericRecord record) {
        ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<>("task-submitted", null, record);
        producer.send(producerRecord);
    }

    public static void main(String[] args) throws FileNotFoundException, IOException, URISyntaxException {
        Properties props = new Properties();
        var root = Paths.get(Client.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getParent();
        var configPath = root.resolve(
                // relative to target
                "../../librdkafka.config")
                .normalize().toString();
        try (var inputStream = new FileInputStream(configPath)) {
            props.load(inputStream);
        }
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
        props.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, true);

        Schema.Parser parser = new Schema.Parser();
        final Schema schema;
        Path myPath = Paths.get(root.resolve("../../admin/src/").toUri());
        try (Stream<Path> paths = Files.walk(myPath)) {
            schema = paths.filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".avsc"))
                    .map(path -> {
                        Schema schema2 = null;
                        try {
                            var schema3 = parser.parse(path.toFile());
                            System.out.println("Parsed: " + path);
                            System.out.println(schema3.getClass());
                            if (path.endsWith("TaskSubmitted.avsc")) {
                                schema2 = schema3;
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        return schema2;
                    })
                    .filter(Objects::nonNull)
                    .findFirst()
                    .orElseThrow();
        }

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
            int exitCode = new CommandLine(new Client(producer, schema)).execute(args);
            System.exit(exitCode);
        } catch (SerializationException e) {
            // may need to do something with it
        }
    }
}
