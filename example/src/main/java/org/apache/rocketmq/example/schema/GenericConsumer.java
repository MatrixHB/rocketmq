/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.example.schema;

import java.util.List;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.rocketmq.client.consumer.SchemaMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.SchemaMessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.SchemaMessageExt;
import org.apache.rocketmq.common.schema.SchemaConstant;
import org.apache.rocketmq.common.schema.SerializerType;

public class GenericConsumer {

    public static void main(String[] args) throws MQClientException {

        /*
         * Instantiate with specified consumer group name.
         */
        Properties props = new Properties();
        props.setProperty(SchemaConstant.SCHEMA_REGISTRY_URL, "localhost:8080");
        props.setProperty(SchemaConstant.SCHEMA_SERIALIZER_TYPE, SerializerType.AVRO.name());
        props.setProperty(SchemaConstant.SCHEMA_PARSE_TYPE, "generic");

        SchemaMQPushConsumer consumer = new SchemaMQPushConsumer(props);

        /*
         * Specify name server addresses.
         * <p/>
         *
         * Alternatively, you may specify name server addresses via exporting environmental variable: NAMESRV_ADDR
         * <pre>
         * {@code
         * consumer.setNamesrvAddr("name-server1-ip:9876;name-server2-ip:9876");
         * }
         * </pre>
         */

        /*
         * Specify where to start in case the specific consumer group is a brand-new one.
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        /*
         * Subscribe one more topic to consume.
         */
        consumer.subscribe("TopicTest7", "*");

        /*
         *  Register callback to execute on arrival of messages fetched from brokers.
         */
        consumer.registerMessageListener(new SchemaMessageListenerConcurrently<GenericRecord>() {
            @Override
            public ConsumeConcurrentlyStatus consumeSchemaMessage(List<SchemaMessageExt<GenericRecord>> msgs,
                                                                  ConsumeConcurrentlyContext context) {
                msgs.forEach(msg -> {
                    System.out.printf("%s Receive New Message: %s %n", Thread.currentThread().getName(), msg.getMsg());
                });
//                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        /*
         *  Launch the consumer instance.
         */
        consumer.start();

        System.out.printf("Consumer Started.%n");
    }
}
