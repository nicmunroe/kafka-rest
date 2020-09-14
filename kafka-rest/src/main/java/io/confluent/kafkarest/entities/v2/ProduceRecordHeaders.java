package io.confluent.kafkarest.entities.v2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;

import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;

import io.confluent.kafkarest.entities.EntityUtils;
import io.confluent.rest.validation.ConstraintViolations;

/**
 * TODO: Class Description
 */
public class ProduceRecordHeaders {

    @NotBlank
    private final String key;

    @Nullable
    private final String value;

    @Nullable
    private final byte[] binValue;

    @JsonCreator
    private ProduceRecordHeaders(
        @JsonProperty("key") @NotBlank String key,
        @JsonProperty("value") @Nullable String value,
        @JsonProperty("binValue") @Nullable String binValue
    ) {
        // TODO: This should be handled by the @NotBlank annotation, but it doesn't seem to work.
        //      Does rest proxy actually perform these kinds of validations, or is it just for code-level documentation?
        if (key == null || key.trim().isEmpty()) {
            throw ConstraintViolations.simpleException("Record header key cannot be blank");
        }

        if (value == null && binValue == null) {
            throw ConstraintViolations.simpleException("Record header must specify at least one of: value, binValue");
        }

        if (value != null && binValue != null) {
            throw ConstraintViolations.simpleException("Record header cannot specify both value and binValue");
        }

        this.key = key;
        this.value = value;
        try {
            this.binValue = (binValue != null) ? EntityUtils.parseBase64Binary(binValue) : null;
        } catch (IllegalArgumentException e) {
            throw ConstraintViolations.simpleException("Record header binValue contains invalid base64 encoding");
        }
    }

    @JsonProperty("key")
    public String getKey() {
        return key;
    }

    @JsonProperty("value")
    @Nullable
    public String getValue() {
        return value;
    }

    @JsonProperty("binValue")
    @Nullable
    public byte[] getBinValue() {
        return binValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ProduceRecordHeaders)) {
            return false;
        }
        ProduceRecordHeaders that = (ProduceRecordHeaders) o;
        return Objects.equals(key, that.key) &&
               Objects.equals(value, that.value) &&
               Arrays.equals(binValue, that.binValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value, binValue);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", ProduceRecordHeaders.class.getSimpleName() + "[", "]")
            .add("key=" + key)
            .add("value=" + value)
            .add("binValue=" + binValue)
            .toString();
    }
}
