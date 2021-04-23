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
package org.apache.pulsar;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.testng.Assert;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * End to end transaction test.
 */
@Slf4j
public class TransactionNormalTest {


    private final static String TENANT = "tnx1";
    private final static String NAMESPACE1 = TENANT + "/ns1";

    public static void main(String[] args) throws PulsarClientException {
        TransactionNormalTest transactionNormalTest = new TransactionNormalTest();
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl("pulsar://127.0.0.1:6650").enableTransaction(true).build();
        Thread noBatchProduceCommitTest = new Thread(() -> {
            try {
                transactionNormalTest.noBatchProduceCommitTest(pulsarClient);
            } catch (Exception e) {
                log.error("{} startup fail!", "noBatchProduceCommitTest", e);
            }
        });
        noBatchProduceCommitTest.start();

        Thread batchProduceCommitTest = new Thread(() -> {
            try {
                transactionNormalTest.batchProduceCommitTest(pulsarClient);
            } catch (Exception e) {
                log.error("{} startup fail!", "batchProduceCommitTest", e);
            }
        });
        batchProduceCommitTest.start();

        Thread txnIndividualAckTestNoBatchAndSharedSub = new Thread(() -> {
            try {
                transactionNormalTest.txnIndividualAckTestNoBatchAndSharedSub(pulsarClient, "txnIndividualAckTestNoBatchAndSharedSub");
            } catch (Exception e) {
                log.error("{} startup fail!", "txnIndividualAckTestNoBatchAndSharedSub", e);
            }
        });
        txnIndividualAckTestNoBatchAndSharedSub.start();

        Thread txnIndividualAckTestBatchAndSharedSub = new Thread(() -> {
            try {
                transactionNormalTest.txnIndividualAckTestBatchAndSharedSub(pulsarClient);
            } catch (Exception e) {
                log.error("{} startup fail!", "txnIndividualAckTestBatchAndSharedSub", e);
            }
        });
        txnIndividualAckTestBatchAndSharedSub.start();

        Thread txnIndividualAckTestNoBatchAndFailoverSub = new Thread(() -> {
            try {
                transactionNormalTest.txnIndividualAckTestNoBatchAndFailoverSub(pulsarClient);
            } catch (Exception e) {
                log.error("{} startup fail!", "txnIndividualAckTestNoBatchAndFailoverSub", e);
            }
        });
        txnIndividualAckTestNoBatchAndFailoverSub.start();

        Thread txnIndividualAckTestBatchAndFailoverSub = new Thread(() -> {
            try {
                transactionNormalTest.txnIndividualAckTestBatchAndFailoverSub(pulsarClient);
            } catch (Exception e) {
                log.error("{} startup fail!", "txnIndividualAckTestBatchAndFailoverSub", e);
            }
        });
        txnIndividualAckTestBatchAndFailoverSub.start();

        Thread txnAckTestBatchAndCumulativeSub = new Thread(() -> {
            try {
                transactionNormalTest.txnAckTestBatchAndCumulativeSub(pulsarClient);
            } catch (Exception e) {
                log.error("{} startup fail!", "txnAckTestBatchAndCumulativeSub", e);
            }
        });
        txnAckTestBatchAndCumulativeSub.start();

        Thread txnAckTestNoBatchAndCumulativeSub = new Thread(() -> {
            try {
                transactionNormalTest.txnAckTestNoBatchAndCumulativeSub(pulsarClient);
            } catch (Exception e) {
                log.error("{} startup fail!", "txnAckTestNoBatchAndCumulativeSub", e);
            }
        });
        txnAckTestNoBatchAndCumulativeSub.start();

        Thread produceAbortTest = new Thread(() -> {
            try {
                transactionNormalTest.produceAbortTest(pulsarClient);
            } catch (Exception e) {
                log.error("{} startup fail!", "produceAbortTest", e);
            }
        });
        produceAbortTest.start();
    }

    public void noBatchProduceCommitTest(PulsarClient pulsarClient) throws Exception {
        produceCommitTest(false, pulsarClient);
    }

    public void batchProduceCommitTest(PulsarClient pulsarClient) throws Exception {
        produceCommitTest(true, pulsarClient);
    }

    public void txnIndividualAckTestNoBatchAndSharedSub(PulsarClient pulsarClient, String topicName) throws Exception {
        txnAckTest(false, 1, SubscriptionType.Shared, pulsarClient, topicName);
    }

    public void txnIndividualAckTestBatchAndSharedSub(PulsarClient pulsarClient) throws Exception {
        txnAckTest(true, 200, SubscriptionType.Shared, pulsarClient, "txnIndividualAckTestBatchAndSharedSub");
    }

    public void txnIndividualAckTestNoBatchAndFailoverSub(PulsarClient pulsarClient) throws Exception {
        txnAckTest(false, 1, SubscriptionType.Failover, pulsarClient, "txnIndividualAckTestNoBatchAndFailoverSub");
    }

    public void txnIndividualAckTestBatchAndFailoverSub(PulsarClient pulsarClient) throws Exception {
        txnAckTest(true, 200, SubscriptionType.Failover, pulsarClient, "txnIndividualAckTestBatchAndFailoverSub");
    }

    public void txnAckTestBatchAndCumulativeSub(PulsarClient pulsarClient) throws Exception {
        txnCumulativeAckTest(true, 200, pulsarClient, "txnAckTestBatchAndCumulativeSub");
    }

    public void txnAckTestNoBatchAndCumulativeSub(PulsarClient pulsarClient) throws Exception {
        txnCumulativeAckTest(false, 1, pulsarClient, "txnAckTestNoBatchAndCumulativeSub");
    }

    private void produceCommitTest(boolean enableBatch, PulsarClient pulsarClient) throws PulsarClientException {

        String produceCommitTest = NAMESPACE1 + "/produceCommitTest-" + enableBatch;
        Consumer<byte[]> consumer = pulsarClient
                .newConsumer()
                .topic(produceCommitTest)
                .subscriptionName("test")
                .enableBatchIndexAcknowledgment(true)
                .subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient
                .newProducer()
                .topic(produceCommitTest)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(enableBatch)
                .producerName(produceCommitTest)
                .sendTimeout(0, TimeUnit.SECONDS);
        Producer<byte[]> producer = producerBuilder.create();

        System.out.println(produceCommitTest + " startup success");
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            int messageCnt = 1000;
            try {
                Transaction txn1 = getTxn(pulsarClient);
                for (int i = 0; i < messageCnt; i++) {
                    producer.newMessage(txn1).value(("Hello Txn - " + i).getBytes(UTF_8)).sendAsync();
                }
                txn1.commit().get();
            } catch (Exception e) {
                log.error("produceCommitTest send txn fail! isBatch : {} ", enableBatch);
                continue;
            }

            try {
            for (int i = 0; i < messageCnt * 2; i++) {
                Message<byte[]> message = consumer.receive(2, TimeUnit.SECONDS);
                if (message == null) {
                    break;
                }
                consumer.acknowledge(message);
            }
            } catch (Exception e) {
                log.error("produceCommitTest ack message error! isBatch : {} ", enableBatch, e);
            }
        }
    }

    public void produceAbortTest(PulsarClient pulsarClient) throws PulsarClientException {

        String produceAbortTest = NAMESPACE1 + "/produceAbortTest";
        Producer<byte[]> producer = pulsarClient
                .newProducer()
                .topic(produceAbortTest)
                .sendTimeout(0, TimeUnit.SECONDS)
                .producerName(produceAbortTest)
                .enableBatching(false)
                .create();

        Consumer<byte[]> consumer = pulsarClient
                .newConsumer()
                .topic(produceAbortTest)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("test")
                .enableBatchIndexAcknowledgment(true)
                .subscribe();
        System.out.println(produceAbortTest + " startup success");
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                Transaction txn = getTxn(pulsarClient);
                int messageCnt = 10;
                for (int i = 0; i < messageCnt; i++) {
                    try {
                        producer.newMessage(txn).value(("Hello Txn - " + i).getBytes(UTF_8)).send();
                    } catch (PulsarClientException pulsarClientException) {
                        pulsarClientException.printStackTrace();
                    }
                }
                // Can't receive transaction messages before abort.
                Message<byte[]> message = consumer.receive(2, TimeUnit.SECONDS);
                Assert.assertNull(message);

                txn.abort().get();

                // Cant't receive transaction messages after abort.
                message = consumer.receive(2, TimeUnit.SECONDS);
                if (message != null) {
                    log.error("produceAbortTest receive message");
                }
            }  catch (Exception e) {
                log.error("produceAbortTest operation error!", e);
            }
        }
    }

    private void txnAckTest(boolean batchEnable, int maxBatchSize,
                         SubscriptionType subscriptionType, PulsarClient pulsarClient, String testName) throws PulsarClientException {
        String txnAckTest = NAMESPACE1 + "/" + testName;

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(txnAckTest)
                .subscriptionName("test")
                .enableBatchIndexAcknowledgment(true)
                .subscriptionType(subscriptionType)
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(txnAckTest)
                .enableBatching(batchEnable)
                .sendTimeout(0, TimeUnit.SECONDS)
                .producerName(txnAckTest)
                .batchingMaxMessages(maxBatchSize)
                .create();
        while (true) {
            System.out.println(txnAckTest + " startup success");
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            int messageCnt = 500;
            try {
                Transaction txn = getTxn(pulsarClient);
                // produce normal messages
                for (int i = 0; i < messageCnt; i++){
                    producer.newMessage(txn).value("hello".getBytes()).sendAsync();
                }
                txn.commit().get();
            } catch (Exception e) {
                System.out.println("send fail" + e);
                continue;
            }

            try {
                Transaction txn = getTxn(pulsarClient);
                Message<byte[]> message = null;
                for (int i = 0; i < 2000; i++) {
                    message = consumer.receive(2, TimeUnit.SECONDS);
                    if (message != null) {
                        consumer.acknowledgeAsync(message.getMessageId(), txn);
                    } else {
                        break;
                    }
                }
                txn.commit().get();
            } catch (Exception e) {
                System.out.println("ack fail" + e);
            }
        }
    }

    private void txnCumulativeAckTest(boolean batchEnable, int maxBatchSize, PulsarClient pulsarClient, String testName) throws PulsarClientException {
        String txnCumulativeAckTest = NAMESPACE1 + "/" + testName;

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(txnCumulativeAckTest)
                .subscriptionName("test")
                .enableBatchIndexAcknowledgment(true)
                .subscriptionType(SubscriptionType.Failover)
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(txnCumulativeAckTest)
                .enableBatching(batchEnable)
                .sendTimeout(0, TimeUnit.SECONDS)
                .producerName(txnCumulativeAckTest)
                .batchingMaxMessages(maxBatchSize)
                .batchingMaxPublishDelay(1, TimeUnit.SECONDS)
                .create();

        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println(txnCumulativeAckTest + " startup success");
            int messageCnt = 300;
            try {
                Transaction txn = getTxn(pulsarClient);
                // produce normal messages
                for (int i = 0; i < messageCnt; i++){
                    producer.newMessage(txn).value("hello".getBytes()).sendAsync();
                }
                txn.commit().get();
            } catch (Exception e) {
                continue;
            }

            try {
                Transaction txn = getTxn(pulsarClient);

                for (int i = 0; i < messageCnt * 2; i++) {
                    Message<byte[]> message = consumer.receive(2, TimeUnit.SECONDS);
                    if (message != null) {
                        consumer.acknowledgeAsync(message.getMessageId(), txn);
                    } else {
                        break;
                    }
                }
                txn.commit().get();
            } catch (Exception e) {
                log.error("{} ack fail", testName, e);
            }
        }
    }

    private Transaction getTxn(PulsarClient pulsarClient) throws Exception {
        return pulsarClient
                .newTransaction()
                .withTransactionTimeout(60, TimeUnit.SECONDS)
                .build()
                .get();
    }
}
