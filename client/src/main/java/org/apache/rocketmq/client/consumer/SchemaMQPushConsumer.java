package org.apache.rocketmq.client.consumer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.rocketmq.client.consumer.listener.SchemaMessageListenerConcurrently;
import org.apache.rocketmq.common.schema.SchemaConstant;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClientFactory;
import org.apache.rocketmq.schema.registry.client.serializer.AvroDeserializer;

import static org.apache.rocketmq.common.schema.SerializerType.AVRO;

public class SchemaMQPushConsumer extends DefaultMQPushConsumer {

    public static Properties properties;

    /**
     * Default constructor.
     */
    public SchemaMQPushConsumer(Properties properties) {
        super();
        SchemaMQPushConsumer.properties = properties;
    }

    public <T> void registerMessageListener(SchemaMessageListenerConcurrently<T> messageListener) throws IOException {
        String baseUrl = properties.getProperty(SchemaConstant.SCHEMA_REGISTRY_URL);
        SchemaRegistryClient schemaRegistryClient = SchemaRegistryClientFactory.newClient(baseUrl, null);
        if (AVRO.name().equals(properties.getProperty(SchemaConstant.SCHEMA_SERIALIZER_TYPE))) {
            messageListener.setDeserializer(new AvroDeserializer(schemaRegistryClient));
        } else {
            // TODO
        }
        setMessageListener(messageListener);
        this.defaultMQPushConsumerImpl.registerMessageListener(messageListener);
    }
}
