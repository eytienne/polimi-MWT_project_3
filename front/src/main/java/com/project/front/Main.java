package com.project.front;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.project.Calculus;
import com.project.CalculusResult;
import com.project.ErrorResult;
import com.project.ImageCompression;
import com.project.ImageCompressionResult;
import com.project.WordCount;
import com.project.WordCountResult;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParseResult;

@Command()
public class Main {
    private KafkaProducer<String, SpecificRecordBase> producer;
    private KafkaConsumer<String, SpecificRecordBase> consumer;
    private String clientId = UUID.randomUUID().toString();
    @Option(names = { "--timeout", "-t" }, description = "Timeout (in seconds) for a response", defaultValue = "60")
    private float timeout;

    @Command(name = "calculate")
    public int calculusCommand(
            @Parameters(paramLabel = "EXPRESSION", arity = "1") String expression) {
        CalculusResult result = handleRecord(new ProducerRecord<>(
                "calculus",
                null,
                Calculus.newBuilder().setExpression(expression).build()), "calculus-result");
        System.out.println(String.format("Calculus result: %s\n", result.getExpression()));
        return 0;
    }

    @Command(name = "compress-image")
    public int imageCompressionCommand(
            @Parameters(paramLabel = "URL", arity = "1..*") URL[] urls,
            @Option(names = { "--quality", "-q" }, defaultValue = "0.9") float quality) {
        ImageCompressionResult result = handleRecord(new ProducerRecord<>(
                "image-compression",
                null,
                ImageCompression
                        .newBuilder()
                        .setUrl(Stream.of(urls).map(URL::toString).collect(Collectors.toUnmodifiableList()))
                        .setQuality(quality)
                        .build()),
                "image-compression-result");
        System.out.println(String.format("Result located at: %s\n", result.getDirectory()));
        return 0;
    }

    @Command(name = "count-words")
    public int wordCountCommand(
            @Parameters(paramLabel = "URL", arity = "1..*") URL[] urls) {
        WordCountResult result = handleRecord(new ProducerRecord<>(
                "word-count",
                null,
                WordCount.newBuilder()
                        .setUrl(Stream.of(urls).map(URL::toString).collect(Collectors.toUnmodifiableList())).build()),
                "word-count-result");
        System.out.println("Counts:");
        for (var entry : result.getCounts().entrySet()) {
            System.out.println(String.format("%s: %s", entry.getKey(), entry.getValue()));
        }
        return 0;
    }

    @SuppressWarnings("unchecked")
    private <T extends SpecificRecordBase> T handleRecord(ProducerRecord<String, SpecificRecordBase> producerRecord,
            String successResultTopic) {
        try {
            consumer.subscribe(List.of(successResultTopic, "error-result"));

            producerRecord.headers().add("client-id", clientId.getBytes(StandardCharsets.UTF_8));
            producer.send(producerRecord).get();

            System.out.println("Waiting for the result from polling from " + successResultTopic + " being " + clientId);

            var time = Duration.ofSeconds(0);
            while (time.toMillis() < timeout * 1000) {
                var duration = Duration.ofMillis(2000);
                ConsumerRecords<String, SpecificRecordBase> records = consumer.poll(duration);
                time = time.plus(duration);
                System.out.println("Assigment: " + String.join(",",
                        consumer.assignment().stream().map((tp) -> tp.toString()).sorted().toArray(String[]::new)));
                // System.out.println("% " + records.count() + " record(s) fetched");
                for (var consumerRecord : records) {
                    System.out.println("DEBUG received after " + time + ":");
                    if (new String(consumerRecord.headers().lastHeader("client-id").value(), StandardCharsets.UTF_8)
                            .equals(clientId)) {
                        var value = consumerRecord.value();
                        if (value instanceof ErrorResult) {
                            var errorValue = (ErrorResult) value;
                            System.err.println(
                                    String.format("Error %d: \"%s\"", errorValue.getCode(), errorValue.getMessage()));
                            throw new BusinessException("Error result");
                        }
                        return (T) value;
                    }
                }
            }
            throw new BusinessException("Timeout expired");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            throw new BusinessException(e);
        }
    }

    private int executionStrategy(ParseResult parseResult) {
        try {
            Properties props = new Properties();
            var classLoader = this.getClass().getClassLoader();
            try (var inputStream = classLoader.getResourceAsStream("librdkafka.config")) {
                props.load(inputStream);
            }
            props.put("zookeeper.session.timeout.ms", 30000);

            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
            props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            // props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "group_" + clientId);

            props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
            props.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, true);
            props.put(AbstractKafkaSchemaSerDeConfig.LATEST_COMPATIBILITY_STRICT, false);
            props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

            try (
                    KafkaProducer<String, SpecificRecordBase> producer = this.producer = new KafkaProducer<>(props);
                    KafkaConsumer<String, SpecificRecordBase> consumer = this.consumer = new KafkaConsumer<>(props)) {
                return new CommandLine.RunLast().execute(parseResult);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        } catch (BusinessException | InvalidGroupIdException e) {
            e.printStackTrace();
            return -2;
        }
    }

    public static void main(String[] args) {
        var main = new Main();
        int exitCode = new CommandLine(main)
                .setExecutionStrategy(main::executionStrategy)
                .execute(args);
        System.exit(exitCode);
    }
}
