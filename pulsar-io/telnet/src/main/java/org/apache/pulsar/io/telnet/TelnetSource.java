/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.telnet;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.apache.pulsar.io.telnet.connection.TelnetConnection;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A simple Telnet Source connector to listen for incoming messages and write to user-defined Pulsar topic.
 */
@Connector(
    name = "telnet",
    type = IOType.SOURCE,
    help = "A simple telnet Source connector to listen for incoming messages and write to user-defined Pulsar topic",
    configClass = TelnetSourceConfig.class)
@Slf4j
public class TelnetSource extends PushSource<byte[]> {

    private TelnetConnection telnetConnection;

    private Thread thread;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    public static void main(String[] args) throws Exception {
//        TelnetSource telnetSource = new TelnetSource();
//        Map<String, Object> config = new HashMap<>();
//        config.put("request", "get /21+X212MGPH0124+dat+0 /http/1.1");
//        config.put("host", "10.45.49.235");
//        config.put("port", "81");
//        config.put("password", "get /42+X212MGPH0124+lin+administrator+01234567 /http/1.1");
//        config.put("topic", "test");
//        telnetSource.open(config, null);

        Consumer<String> consumer = PulsarClient.builder().serviceUrl("pulsar://127.0.0.1:6650").build().newConsumer(Schema.STRING).topic("test").subscriptionName("test1").subscribe();

        while (true) {
            Message<String> message = consumer.receive();
            consumer.acknowledge(message);
            System.out.println(message.getValue());
        }
    }

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        TelnetSourceConfig telnetSourceConfig = TelnetSourceConfig.load(config);
        if (telnetSourceConfig.getHost() == null
                || telnetSourceConfig.getPort() <= 0
                || telnetSourceConfig.getRequest() == null) {
            throw new IllegalArgumentException("Required property not set.");
        }

        this.thread = new Thread(new PulsarServerRunnable(telnetSourceConfig, this));

        thread.start();
    }

    @Override
    public void close() throws Exception {
        telnetConnection.shutdownGracefully();
    }

    private static class PulsarServerRunnable implements Runnable {

        private final TelnetSourceConfig telnetSourceConfig;
        private final TelnetSource telnetSource;
        private PulsarClient pulsarClient;

        public PulsarServerRunnable(TelnetSourceConfig telnetSourceConfig, TelnetSource telnetSource) {
            this.telnetSourceConfig = telnetSourceConfig;
            this.telnetSource = telnetSource;
        }

        @Override
        public void run() {
            try {
            TelnetConnection telnetConnection = new TelnetConnection.Builder()
                    .setHost(telnetSourceConfig.getHost())
                    .setPassword(telnetSourceConfig.getPassword())
                    .setTermType(telnetSourceConfig.getTermType())
                    .setUser(telnetSourceConfig.getUser())
                    .setPort(telnetSourceConfig.getPort()).build();

                PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar://127.0.0.1:6650").build();
                System.out.println(telnetSourceConfig.getPassword());
                System.out.println(telnetSourceConfig.getRequest());
                handleTelnet(telnetConnection, pulsarClient);
            } catch (Exception e) {
                if (pulsarClient != null){
                    pulsarClient.closeAsync();
                }
                System.out.println(e);
                run();
            }

        }

        public void handleTelnet(TelnetConnection telnetConnection, PulsarClient pulsarClient) throws Exception {

            Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                    .topic(telnetSourceConfig.getTopic()).create();
            telnetConnection.connect();


            telnetConnection.write(telnetSourceConfig.getRequest());

            while (true) {
                byte[] value = new byte[160];
                telnetConnection.read(value);
                String string = new String(value);
                System.out.println(string);
                producer.send(string);
            }
        }

    }

}
