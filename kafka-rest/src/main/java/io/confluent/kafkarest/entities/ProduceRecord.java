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

package io.confluent.kafkarest.entities;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import io.confluent.kafkarest.entities.v2.ProduceRecordHeaders;

public final class ProduceRecord<K, V> {

  @Nullable
  private final K key;

  @Nullable
  private final V value;

  @Nullable
  private final Integer partition;

  @NotNull
  private final Headers headers;

  public ProduceRecord(
      @Nullable K key,
      @Nullable V value,
      @Nullable Integer partition,
      @Nullable List<ProduceRecordHeaders> headers
  ) {
    this.key = key;
    this.value = value;
    this.partition = partition;
    this.headers = convertHeaders(headers);
  }

  private @NotNull Headers convertHeaders(@Nullable List<ProduceRecordHeaders> headers) {
    if (headers == null) {
      return new RecordHeaders();
    }

    List<Header> convertedHeaders = headers
        .stream()
        .map(prh -> {
          byte[] valueBytes = (prh.getValue() != null)
                              ? prh.getValue().getBytes(StandardCharsets.UTF_8)
                              : prh.getBinValue();
          return new RecordHeader(prh.getKey(), valueBytes);
        })
        .collect(Collectors.toList());

    return new RecordHeaders(convertedHeaders);
  }

  @Nullable
  public K getKey() {
    return key;
  }

  @Nullable
  public V getValue() {
    return value;
  }

  @Nullable
  public Integer getPartition() {
    return partition;
  }

  @NotNull
  public Headers getHeaders() {
    return headers;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProduceRecord<?, ?> that = (ProduceRecord<?, ?>) o;
    return Objects.equals(key, that.key)
        && Objects.equals(value, that.value)
        && Objects.equals(partition, that.partition)
        && Objects.equals(headers, that.headers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, value, partition, headers);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ProduceRecord.class.getSimpleName() + "[", "]")
        .add("key=" + key)
        .add("value=" + value)
        .add("partition=" + partition)
        .add("headers=" + headers)
        .toString();
  }
}