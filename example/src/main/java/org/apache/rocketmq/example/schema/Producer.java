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

import java.util.Properties;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SchemaMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.SchemaMessage;
import org.apache.rocketmq.common.schema.SchemaConstant;
import org.apache.rocketmq.common.schema.SerializerType;

/**
 * This class demonstrates how to send messages to brokers using provided {@link DefaultMQProducer}.
 */
public class Producer {
    public static void main(String[] args) throws MQClientException, InterruptedException {

        /*
         * Instantiate with a producer group name.
         */
        Properties properties = new Properties();
        properties.setProperty(SchemaConstant.SCHEMA_REGISTRY_URL, "http://localhost:8080");
        SchemaMQProducer producer = new SchemaMQProducer(properties);

        producer.setNamesrvAddr("localhost:9876");
        producer.setProducerGroup("schema-test");

        producer.start();

        try {

            /*
             * Create a message instance, specifying topic, tag and message body.
             */
            Payment payment = new Payment("pa1", 12.0);
            SchemaMessage<Payment> msg = new SchemaMessage<>("test_topic", "TagA", "key", payment);

            /*
             * Call send message to deliver message to one of brokers.
             */
            System.out.println("send msg: " + msg);
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);

            User user = new User("tom", "aliyun");
            SchemaMessage<User> msg2 = new SchemaMessage<>("test_topic2", "TagA", "key", user);

            /*
             * Call send message to deliver message to one of brokers.
             */
            System.out.println("send msg: " + msg2);
            SendResult sendResult2 = producer.send(msg2);
            System.out.printf("%s%n", sendResult2);
        } catch (Exception e) {
            e.printStackTrace();
            Thread.sleep(1000);
        }

        /*
         * Shut down once the producer instance is not longer in use.
         */
        producer.shutdown();
    }
}
