package org.apache.rocketmq.client.producer;

import java.util.Properties;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.SchemaMessage;
import org.apache.rocketmq.common.schema.SchemaConstant;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClientFactory;
import org.apache.rocketmq.schema.registry.client.serializer.AvroSerializer;
import org.apache.rocketmq.schema.registry.client.serializer.Serializer;

import static org.apache.rocketmq.common.schema.SerializerType.AVRO;

public class SchemaMQProducer extends DefaultMQProducer {

    private Serializer serializer;

    public SchemaMQProducer(Properties properties) {
        super();
        String baseUrl = properties.getProperty(SchemaConstant.SCHEMA_REGISTRY_URL);
        SchemaRegistryClient schemaRegistryClient = SchemaRegistryClientFactory.newClient(baseUrl, null);

        if (AVRO.name().equals(properties.getProperty(SchemaConstant.SCHEMA_SERIALIZER_TYPE))) {
            serializer = new AvroSerializer(schemaRegistryClient);
        } else {
            // TODO
        }
    }

    public <T> SendResult send(SchemaMessage<T> msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        String topic = msg.getMessage().getTopic();

        byte[] encodedMsg;
        try {
            encodedMsg = serializer.serialize(topic, msg.getOriginMessage());
        } catch (Exception e) {
            throw new MQClientException("Serialized message failed", e);
        }
        msg.getMessage().setBody(encodedMsg);
        return send(msg.getMessage());
    }
}
