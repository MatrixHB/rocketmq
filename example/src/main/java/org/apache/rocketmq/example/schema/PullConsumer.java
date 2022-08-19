package org.apache.rocketmq.example.schema;

import java.util.List;
import java.util.Properties;

import org.apache.rocketmq.client.consumer.SchemaMQPullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.SchemaMessageExt;
import org.apache.rocketmq.common.schema.SchemaConstant;
import org.apache.rocketmq.common.schema.SerializerType;

public class PullConsumer {

    public static void main(String[] args) throws MQClientException {

        Properties props = new Properties();
        props.setProperty(SchemaConstant.SCHEMA_REGISTRY_URL, "http://localhost:8080");
        props.setProperty(SchemaConstant.SCHEMA_SERIALIZER_TYPE, SerializerType.AVRO.name());
        SchemaMQPullConsumer consumer = new SchemaMQPullConsumer(props);
        consumer.setConsumerGroup("schema-test");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.subscribe("test_topic", "*");
        //consumer.subscribe("test_topic2", "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.start();
        try {
            while (true) {
                List<SchemaMessageExt<Payment>> messageExts = consumer.pollSchemaMessages();
                System.out.printf("%s%n", messageExts);
                for (SchemaMessageExt<Payment> msg : messageExts) {
                    Payment payment = msg.getMsg();
                    System.out.println(payment);
                }
            }
        } finally {
            consumer.shutdown();
        }
    }
}
