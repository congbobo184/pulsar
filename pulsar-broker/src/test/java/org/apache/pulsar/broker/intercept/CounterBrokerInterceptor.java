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
package org.apache.pulsar.broker.intercept;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Map;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.eclipse.jetty.server.Response;


@Slf4j
public class CounterBrokerInterceptor implements BrokerInterceptor {

    private final AtomicInteger beforeSendCount = new AtomicInteger();
    private final AtomicInteger count = new AtomicInteger();
    private final AtomicInteger connectionCreationCount = new AtomicInteger();
    private final AtomicInteger producerCount = new AtomicInteger();
    private final AtomicInteger consumerCount = new AtomicInteger();
    private final AtomicInteger messageCount = new AtomicInteger();
    private final AtomicInteger messageDispatchCount = new AtomicInteger();
    private final AtomicInteger messageAckCount = new AtomicInteger();



    public void reset() {
        beforeSendCount.set(0);
        count.set(0);
        connectionCreationCount.set(0);
        producerCount.set(0);
        consumerCount.set(0);
        messageCount.set(0);
        messageDispatchCount.set(0);
        messageAckCount.set(0);
    }

    private final List<ResponseEvent> responseList = new ArrayList<>();

    @Data
    @AllArgsConstructor
    public class ResponseEvent {
        private String requestUri;
        private int responseStatus;
    }

    @Override
    public void onConnectionCreated(ServerCnx cnx){
        log.info("Connection created {}", cnx);
        connectionCreationCount.incrementAndGet();
    }

    @Override
    public void producerCreated(ServerCnx cnx, Producer producer,
                                Map<String, String> metadata){
        log.info("Producer created with name={}, id={}",
            producer.getProducerName(), producer.getProducerId());
        producerCount.incrementAndGet();
    }

    @Override
    public void consumerCreated(ServerCnx cnx,
                                 Consumer consumer,
                                 Map<String, String> metadata) {
        log.info("Consumer created with name={}, id={}",
            consumer.consumerName(), consumer.consumerId());
        consumerCount.incrementAndGet();
    }

    @Override
    public void messageProduced(ServerCnx cnx, Producer producer, long startTimeNs, long ledgerId,
                                 long entryId,
                                 Topic.PublishContext publishContext) {
        log.info("Message published topic={}, producer={}",
            producer.getTopic().getName(), producer.getProducerName());
        messageCount.incrementAndGet();
    }

    @Override
    public void messageDispatched(ServerCnx cnx, Consumer consumer, long ledgerId,
                                   long entryId, ByteBuf headersAndPayload) {
        log.info("Message dispatched topic={}, consumer={}",
            consumer.getSubscription().getTopic().getName(), consumer.consumerName());
        messageDispatchCount.incrementAndGet();
    }

    @Override
    public void messageAcked(ServerCnx cnx, Consumer consumer,
                              CommandAck ack) {
        messageAckCount.incrementAndGet();
    }

    @Override
    public void beforeSendMessage(Subscription subscription,
                                  Entry entry,
                                  long[] ackSet,
                                  MessageMetadata msgMetadata) {
        if (log.isDebugEnabled()) {
            log.debug("Send message to topic {}, subscription {}",
                    subscription.getTopic(), subscription.getName());
        }
        beforeSendCount.incrementAndGet();
    }

    @Override
    public void onPulsarCommand(BaseCommand command, ServerCnx cnx) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] On [{}] Pulsar command", count, command.getType().name());
        }
        count.incrementAndGet();
    }

    @Override
    public void onConnectionClosed(ServerCnx cnx) {
        // np-op
    }

    @Override
    public void onWebserviceRequest(ServletRequest request) {
        count.incrementAndGet();
        if (log.isDebugEnabled()) {
            log.debug("[{}] On [{}] Webservice request", count, ((HttpServletRequest) request).getRequestURL().toString());
        }
    }

    @Override
    public void onWebserviceResponse(ServletRequest request, ServletResponse response) {
        count.incrementAndGet();
        if (log.isDebugEnabled()) {
            log.debug("[{}] On [{}] Webservice response {}", count, ((HttpServletRequest) request).getRequestURL().toString(), response);
        }
        if (response instanceof Response) {
            Response res = (Response) response;
            responseList.add(new ResponseEvent(res.getHttpChannel().getRequest().getRequestURI(), res.getStatus()));
        }
    }

    @Override
    public void onFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        count.set(100);
        chain.doFilter(request, response);
    }

    @Override
    public void initialize(PulsarService pulsarService) throws Exception {

    }

    @Override
    public void close() {

    }

    public int getCount() {
        return count.get();
    }

    public int getProducerCount() {
        return producerCount.get();
    }

    public int getConsumerCount() {
        return consumerCount.get();
    }

    public int getMessagePublishCount() {
        return messageCount.get();
    }

    public int getMessageDispatchCount() {
        return messageDispatchCount.get();
    }

    public int getMessageAckCount() {
        return messageAckCount.get();
    }

    public int getBeforeSendCount() {
        return beforeSendCount.get();
    }

    public int getConnectionCreationCount() {
        return connectionCreationCount.get();
    }

    public void clearResponseList() {
        responseList.clear();
    }

    public List<ResponseEvent> getResponseList() {
        return responseList;
    }
}
