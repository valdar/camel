/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.processor.idempotent.kafka;

import org.apache.camel.BindToRegistry;
import org.apache.camel.EndpointInject;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.BaseEmbeddedKafkaTest;
import org.apache.camel.component.mock.MockEndpoint;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test whether the KafkaIdempotentRepository successfully recreates its cache from pre-existing topics. This guarantees
 * that the de-duplication state survives application instance restarts.
 *
 */
public class KafkaIdempotentRepositoryPersistenceTest extends BaseEmbeddedKafkaTest {

    // Every instance of the repository must use a different topic to guarantee isolation between tests
    @BindToRegistry("kafkaIdempotentRepository")
    private KafkaIdempotentRepository kafkaIdempotentRepository
            = new KafkaIdempotentRepository("TEST_PERSISTENCE", getBootstrapServers());

    @EndpointInject("mock:out")
    private MockEndpoint mockOut;

    @EndpointInject("mock:before")
    private MockEndpoint mockBefore;

    @Override
    protected RoutesBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("direct:in").to("mock:before").idempotentConsumer(header("id"))
                        .messageIdRepositoryRef("kafkaIdempotentRepository").to("mock:out").end();
            }
        };
    }

    @Test
    public void testRestartFiltersAsExpected() throws InterruptedException {
        send10MsssagesModule5(template);

        // all records sent initially
        assertEquals(10, mockBefore.getReceivedCounter());

        // filters second attempt with same value
        assertEquals(5, kafkaIdempotentRepository.getDuplicateCount());

        // only first 1-4 records are received, the rest are filtered
        assertEquals(5, mockOut.getReceivedCounter());

        //let's simulate a proper restart
        context().stop();
        // we also create a new Kafka Idempotent Repository
        KafkaIdempotentRepository restartedKafkaIdempotentRepository
                = new KafkaIdempotentRepository("TEST_PERSISTENCE", getBootstrapServers());
        context().getRegistry().bind("kafkaIdempotentRepository", restartedKafkaIdempotentRepository);
        context().start();

        //we need to manually recreate the ProducerTemplate
        ProducerTemplate template = context.createProducerTemplate();
        template.start();
        send10MsssagesModule5(template);

        // all records sent initially (we need to manually lookup for mock endpoints)
        assertEquals(10, context().getEndpoint("mock:before", MockEndpoint.class).getReceivedCounter());

        // the state from the previous test guarantees that all attempts now are blocked (we need to use the newly created Kafka Idempotent Repository)
        assertEquals(10, restartedKafkaIdempotentRepository.getDuplicateCount());

        // nothing gets passed the idempotent consumer this time (we need to manually lookup for mock endpoints)
        assertEquals(0, context().getEndpoint("mock:out", MockEndpoint.class).getReceivedCounter());
    }

    private void send10MsssagesModule5(ProducerTemplate template) {
        for (int i = 0; i < 10; i++) {
            template.sendBodyAndHeader("direct:in", "Test message", "id", i % 5);
        }
    }
}
