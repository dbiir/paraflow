package cn.edu.ruc.iir.paraflow.commons;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

public class Message
{
    private final byte[] key;
    private final byte[] value;
    private final long timestamp;
    private String topic = "";
    private int fiberId;
    private boolean hasFiberId = false;

    public Message(byte[] key, byte[] value, long timestamp)
    {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    public byte[] getKey()
    {
        return key;
    }

    public byte[] getValue()
    {
        return value;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public Optional<String> getTopic()
    {
        if (topic != null) {
            return Optional.of(topic);
        }
        return Optional.empty();
    }

    public void setTopic(String topic)
    {
        this.topic = topic;
    }

    public Optional<Integer> getFiberId()
    {
        if (hasFiberId) {
            return Optional.of(fiberId);
        }
        return Optional.empty();
    }

    public void setFiberId(int fiberId)
    {
        this.fiberId = fiberId;
        this.hasFiberId = true;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(key, timestamp, value, topic);
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (other instanceof Message) {
            Message otherMsg = (Message) other;
            return this.key == otherMsg.key &&
                    this.timestamp == otherMsg.timestamp &&
                    Arrays.equals(this.value, otherMsg.value);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return String.format("key: %s, timestamp: %d, topic: %s", Arrays.toString(key), timestamp, topic);
    }
}
