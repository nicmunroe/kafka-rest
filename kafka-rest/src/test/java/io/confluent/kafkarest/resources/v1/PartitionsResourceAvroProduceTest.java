/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafkarest.resources.v1;

import static io.confluent.kafkarest.TestUtils.assertErrorResponse;
import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.AdminClientWrapper;
import io.confluent.kafkarest.DefaultKafkaRestContext;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestApplication;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.RecordMetadataOrException;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.v1.AvroPartitionProduceRequest;
import io.confluent.kafkarest.entities.v1.AvroPartitionProduceRequest.AvroPartitionProduceRecord;
import io.confluent.kafkarest.entities.v1.PartitionOffset;
import io.confluent.kafkarest.entities.v1.ProduceResponse;
import io.confluent.kafkarest.extension.InstantConverterProvider;
import io.confluent.rest.EmbeddedServerTestHarness;
import io.confluent.rest.RestConfigException;
import io.confluent.rest.exceptions.ConstraintViolationExceptionMapper;
import java.util.Arrays;
import java.util.List;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;

// This test is much lighter than the Binary one since they would otherwise be mostly duplicated
// -- this just sanity checks the Jersey processing of these requests.
public class PartitionsResourceAvroProduceTest
    extends EmbeddedServerTestHarness<KafkaRestConfig, KafkaRestApplication> {

  private AdminClientWrapper adminClientWrapper;
  private ProducerPool producerPool;
  private DefaultKafkaRestContext ctx;

  private final String topicName = "topic1";

  private List<AvroPartitionProduceRecord> produceRecordsWithKeys;
  private List<RecordMetadataOrException> produceResults;
  private final List<PartitionOffset> offsetResults;

  // This test assumes that AvroConverterTest is good enough and testing one primitive type for
  // keys and one complex type for records is sufficient.
  private static final String keySchemaStr = "{\"name\":\"int\",\"type\": \"int\"}";
  private static final String valueSchemaStr = "{\"type\": \"record\", "
                                               + "\"name\":\"test\","
                                               + "\"fields\":[{"
                                               + "  \"name\":\"field\", "
                                               + "  \"type\": \"int\""
                                               + "}]}";

  public PartitionsResourceAvroProduceTest() throws RestConfigException {
    adminClientWrapper = EasyMock.createMock(AdminClientWrapper.class);
    producerPool = EasyMock.createMock(ProducerPool.class);
    ctx = new DefaultKafkaRestContext(config,
        producerPool,
        null,
        adminClientWrapper,
        null,
        null
    );
    addResource(new TopicsResource(ctx));
    addResource(new PartitionsResource(ctx));
    addResource(InstantConverterProvider.class);

    produceRecordsWithKeys = Arrays.asList(
        new AvroPartitionProduceRecord(
            TestUtils.jsonTree("1"), TestUtils.jsonTree("{\"field\":42}")),
        new AvroPartitionProduceRecord(
            TestUtils.jsonTree("2"), TestUtils.jsonTree("{\"field\":84}"))
    );
    TopicPartition tp0 = new TopicPartition(topicName, 0);
    produceResults = Arrays.asList(
        new RecordMetadataOrException(new RecordMetadata(tp0, 0L, 0L, 0L, 0L, 1, 1), null),
        new RecordMetadataOrException(new RecordMetadata(tp0, 0L, 1L, 0L, 0L, 1, 1), null)
    );
    offsetResults = Arrays.asList(
        new PartitionOffset(0, 0L, null, null),
        new PartitionOffset(0, 1L, null, null)
    );
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    EasyMock.reset(adminClientWrapper, producerPool);
  }

  private <K, V> Response produceToPartition(String topic, int partition,
      AvroPartitionProduceRequest request,
      String acceptHeader,
      String requestMediatype,
      EmbeddedFormat recordFormat,
      final List<RecordMetadataOrException> results) throws Exception {
    final Capture<ProducerPool.ProduceRequestCallback>
        produceCallback =
        Capture.newInstance();
    EasyMock.expect(adminClientWrapper.topicExists(topic)).andReturn(true);
    EasyMock.expect(adminClientWrapper.partitionExists(topic, partition)).andReturn(true);
    producerPool.produce(EasyMock.eq(topic),
        EasyMock.eq(partition),
        EasyMock.eq(recordFormat),
        EasyMock.anyObject(),
        EasyMock.anyObject(),
        EasyMock.eq(ctx),
        EasyMock.capture(produceCallback));
    EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {
      @Override
      public Object answer() throws Throwable {
        if (results == null) {
          throw new Exception();
        } else {
          produceCallback.getValue().onCompletion(1, 2, results);
        }
        return null;
      }
    });
    EasyMock.replay(adminClientWrapper, producerPool);

    Response
        response =
        request("/topics/" + topic + "/partitions/" + ((Integer) partition).toString(),
                acceptHeader)
            .post(Entity.entity(request, requestMediatype));

    EasyMock.verify(producerPool);

    return response;
  }

  @Test
  public void testProduceToPartitionByKey() throws Exception {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES_AVRO) {
        AvroPartitionProduceRequest request =
            AvroPartitionProduceRequest.create(
                produceRecordsWithKeys,
                keySchemaStr,
                /* keySchemaId= */ null,
                valueSchemaStr,
                /* valueSchemaId= */ null);
        Response rawResponse =
            produceToPartition(topicName, 0, request, mediatype.header, requestMediatype,
                EmbeddedFormat.AVRO, produceResults);
        assertOKResponse(rawResponse, mediatype.expected);
        ProduceResponse response = TestUtils.tryReadEntityOrLog(rawResponse, ProduceResponse.class);

        assertEquals(offsetResults, response.getOffsets());
        assertEquals((Integer) 1, response.getKeySchemaId());
        assertEquals((Integer) 2, response.getValueSchemaId());

        EasyMock.reset(adminClientWrapper, producerPool);
      }
    }

    // Now use schema IDs
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES_AVRO) {
        AvroPartitionProduceRequest request =
            AvroPartitionProduceRequest.create(
                produceRecordsWithKeys,
                /* keySchema= */ null,
                /* keySchemaId= */ 1,
                /* valueSchema= */ null,
                /* valueSchemaId= */ 2);
        Response rawResponse =
            produceToPartition(topicName, 0, request, mediatype.header, requestMediatype,
                EmbeddedFormat.AVRO, produceResults);
        assertOKResponse(rawResponse, mediatype.expected);
        ProduceResponse response = TestUtils.tryReadEntityOrLog(rawResponse, ProduceResponse.class);

        assertEquals(offsetResults, response.getOffsets());
        assertEquals((Integer) 1, response.getKeySchemaId());
        assertEquals((Integer) 2, response.getValueSchemaId());

        EasyMock.reset(adminClientWrapper, producerPool);
      }
    }
  }

  @Test
  public void testProduceMissingSchema() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES_AVRO) {
        AvroPartitionProduceRequest request =
            AvroPartitionProduceRequest.create(
                produceRecordsWithKeys,
                /* keySchema= */ null,
                /* keySchemaId= */ null,
                /* valueSchema= */ null,
                /* valueSchemaId= */ null);
        Response rawResponse =
            request("/topics/" + topicName + "/partitions/0", mediatype.header)
                .post(Entity.entity(request, requestMediatype));

        assertErrorResponse(ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY,
            rawResponse,
            Errors.KEY_SCHEMA_MISSING_ERROR_CODE,
            null,
            mediatype.expected);

        EasyMock.reset(adminClientWrapper, producerPool);
      }
    }
  }

}