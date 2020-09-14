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

package io.confluent.kafkarest;

import io.confluent.kafkarest.distributedtracing.FooDistributedTracingHelper;
import io.confluent.kafkarest.entities.ProduceRecord;
import java.util.Collection;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.ws.rs.container.ContainerRequestContext;

/**
 * Wrapper producer for content types which have no associated schema (e.g. binary or JSON).
 */
public class NoSchemaRestProducer<K, V> implements RestProducer<K, V> {

  private KafkaProducer<K, V> producer;

  public NoSchemaRestProducer(KafkaProducer<K, V> producer) {
    this.producer = producer;
  }

  @Override
  public void produce(
      ProduceTask task,
      String topic,
      Integer partition,
      Collection<? extends ProduceRecord<K, V>> produceRecords,
      ContainerRequestContext containerRequest,
      KafkaRestContext ctx
  ) {
    for (ProduceRecord<K, V> record : produceRecords) {
      Integer recordPartition = partition;
      if (recordPartition == null) {
        recordPartition = record.getPartition();
      }

      FooDistributedTracingHelper dtHelper = ctx.getDtHelper();

      Object clientSpanTracingContext = dtHelper
          .startSpanForClientCall(
              containerRequest,
              record,
              "produce " + topic,
              dtHelper.generateTagsForProduceRecord(record, producer, topic, recordPartition)
          );

      Callback taskCallback = task.createCallback();
      Callback callbackWithTracing = (metadata, exception) -> {
        try {
          ctx.getDtHelper().completeSpanForClientCall(
              clientSpanTracingContext,
              exception,
              dtHelper.generateTagsForRecordMetadata(metadata)
          );
        }
        finally {
          // No matter what, call the task's callback.
          taskCallback.onCompletion(metadata, exception);
        }
      };

      producer.send(
          new ProducerRecord<>(topic, recordPartition, record.getKey(), record.getValue(), record.getHeaders()),
          callbackWithTracing
      );
    }
  }

  @Override
  public void close() {
    producer.close();
  }
}
