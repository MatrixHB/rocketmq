package org.apache.rocketmq.client.consumer.listener;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.SchemaMessageExt;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.schema.registry.client.serializer.Deserializer;

public abstract class SchemaMessageListenerConcurrently<T> implements MessageListenerConcurrently {

    private final InternalLogger log = ClientLogger.getLog();

    private Deserializer deserializer;

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(final List<MessageExt> msgs,
        final ConsumeConcurrentlyContext context) {
        List<SchemaMessageExt<T>> schemaMsgs = msgs.stream()
            .map(msg -> {
                String topic = msg.getTopic();

                T schemaMessage = null;
                try {
                    schemaMessage = (T)deserializer.deserialize(topic, msg.getBody());
                } catch (Exception e) {
                    log.error("decode message failed", e);
                    throw new RuntimeException("decode message failed", e);
                }
                return new SchemaMessageExt<T>(msg, schemaMessage);
            })
            .collect(Collectors.toList());
        return consumeSchemaMessage(schemaMsgs, context);
    }


    public abstract ConsumeConcurrentlyStatus consumeSchemaMessage(final List<SchemaMessageExt<T>> msgs,
        final ConsumeConcurrentlyContext context);

    public void setDeserializer(Deserializer deserializer) {
        this.deserializer = deserializer;
    }
}
