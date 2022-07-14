package com.project.front;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.project.Calculus;
import com.project.ImageCompression;
import com.project.TextFormatting;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParseResult;

@Command()
public class Main {
    private KafkaProducer<String, SpecificRecordBase> producer;

    @Command(name = "calculate")
    public int calculusCommand(
            @Parameters(paramLabel = "EXPRESSION", arity = "1") String expression) {
        sendRecord(Calculus.newBuilder().setExpression(expression).build());
        return 0;
    }

    @Command(name = "compress-image")
    public int imageCompressionCommand(
            @Parameters(paramLabel = "URL", arity = "1..*") URL[] urls,
            @Option(names = { "--quality", "-q" }, defaultValue = "0.9") float quality) {
        sendRecord(
                ImageCompression
                        .newBuilder()
                        .setUrl(Stream.of(urls).map(URL::toString).collect(Collectors.toUnmodifiableList()))
                        .setQuality(quality)
                        .build());
        return 0;
    }

    @Command(name = "format-text")
    public int textFormattingCommand(
            @Parameters(paramLabel = "FORMATTING") String formatting,
            @Parameters(paramLabel = "TEXT") String text) {
        sendRecord(TextFormatting.newBuilder().setFormatting(formatting).setText(text).build());
        return 0;
    }

    private void sendRecord(SpecificRecordBase record) {
        ProducerRecord<String, SpecificRecordBase> producerRecord = new ProducerRecord<>("task-submitted", null,
                record);
        producer.send(producerRecord);
    }

    private int executionStrategy(ParseResult parseResult) {
        try {
            Properties props = new Properties();
            var classLoader = this.getClass().getClassLoader();
            try (var inputStream = classLoader.getResourceAsStream("librdkafka.config")) {
                props.load(inputStream);
            }

            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
            props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
            props.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, true);
            props.put(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY,
                    CustomSubjectNameStrategy.class.getName());

            try (KafkaProducer<String, SpecificRecordBase> producer = this.producer = new KafkaProducer<>(props)) {
                return new CommandLine.RunLast().execute(parseResult);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
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
