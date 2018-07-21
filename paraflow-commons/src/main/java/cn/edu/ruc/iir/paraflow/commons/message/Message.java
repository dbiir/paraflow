package cn.edu.ruc.iir.paraflow.commons.message;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

//todo change values from string array to bytes array
public class Message
{
    private final int keyIndex;
    private final String[] values;
    private final long timestamp;
    private String topic = "";
    private int fiberId;
    private boolean hasFiberId = false;

    public Message(String[] values)
    {
        this(0, values);
    }

    public Message(int keyIndex, String[] values)
    {
        this(keyIndex, values, Long.MIN_VALUE);
    }

    public Message(int keyIndex, String[] values, long timestamp)
    {
        this.keyIndex = keyIndex;
        this.values = values;
        this.timestamp = timestamp;
    }

    public Message(int keyIndex, String[] values, long timestamp, String topic)
    {
        this.keyIndex = keyIndex;
        this.values = values;
        this.timestamp = timestamp;
        this.topic = topic;
    }

    public Message(int keyIndex, String[] values, long timestamp, String topic, int fiberId)
    {
        this.keyIndex = keyIndex;
        this.values = values;
        this.timestamp = timestamp;
        this.topic = topic;
        this.fiberId = fiberId;
        this.hasFiberId = true;
    }

    public String getKey()
    {
        return this.values[keyIndex];
    }

    public int getKeyIndex()
    {
        return this.keyIndex;
    }

    public String[] getValue()
    {
        return this.values;
    }

    public Optional<Long> getTimestamp()
    {
        if (timestamp != Long.MIN_VALUE) {
            return Optional.of(timestamp);
        }
        return Optional.empty();
    }

    public void setTopic(String topic)
    {
        this.topic = topic;
    }

    public Optional<String> getTopic()
    {
        if (topic != null) {
            return Optional.of(topic);
        }
        return Optional.empty();
    }

    public void setFiberId(int fiberId)
    {
        this.fiberId = fiberId;
        this.hasFiberId = true;
    }

    public Optional<Integer> getFiberId()
    {
        if (hasFiberId) {
            return Optional.of(fiberId);
        }
        return Optional.empty();
    }

    @Override
    public String toString()
    {
        return String.format("keyIndex: %d, key: %s, timestamp: %d, topic: %s", keyIndex, values[keyIndex], timestamp, topic);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(keyIndex, timestamp, values, topic);
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (other instanceof Message) {
            Message otherMsg = (Message) other;
            return this.keyIndex == otherMsg.keyIndex &&
                    this.timestamp == otherMsg.timestamp &&
                    Arrays.equals(this.values, otherMsg.values);
        }
        return false;
    }
}
