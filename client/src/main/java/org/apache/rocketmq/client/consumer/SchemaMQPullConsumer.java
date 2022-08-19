package org.apache.rocketmq.client.consumer;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.SchemaMessageExt;
import org.apache.rocketmq.common.schema.SchemaConstant;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClientFactory;
import org.apache.rocketmq.schema.registry.client.serializer.AvroDeserializer;
import org.apache.rocketmq.schema.registry.client.serializer.Deserializer;

import static org.apache.rocketmq.common.schema.SerializerType.AVRO;

public class SchemaMQPullConsumer extends DefaultLitePullConsumer {

    private Deserializer deserializer;

    public SchemaMQPullConsumer(Properties properties) {
        super();
        String baseUrl = properties.getProperty(SchemaConstant.SCHEMA_REGISTRY_URL);
        SchemaRegistryClient schemaRegistryClient = SchemaRegistryClientFactory.newClient(baseUrl, null);

        if (AVRO.name().equals(properties.getProperty(SchemaConstant.SCHEMA_SERIALIZER_TYPE))) {
            deserializer = new AvroDeserializer(schemaRegistryClient);
        } else {
            // TODO
        }
    }

    public <T> List<SchemaMessageExt<T>> pollSchemaMessages() {

        List<MessageExt> msgs = poll();
        return msgs.stream()
            .map(msg -> {
                String topic = msg.getTopic();

                T schemaMessage;
                try {
                    schemaMessage = (T)deserializer.deserialize(topic, msg.getBody());
                } catch (Exception e) {
                    throw new RuntimeException("decode message failed", e);
                }
                return new SchemaMessageExt<T>(msg, schemaMessage);
            })
            .collect(Collectors.toList());
    }
}
