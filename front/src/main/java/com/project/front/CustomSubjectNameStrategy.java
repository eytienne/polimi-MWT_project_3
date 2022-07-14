package com.project.front;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;

public class CustomSubjectNameStrategy extends TopicNameStrategy {
    public CustomSubjectNameStrategy() {
    }

    @Override
    public String subjectName(String topic, boolean isKey, ParsedSchema schema) {
        return isKey ? topic + "-key" : topic;
    }
}
