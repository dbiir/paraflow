package cn.edu.ruc.iir.paraflow.commons.message;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

public class Message
{
    private final int keyIndex;
    private final String[] values;
    private final long timestamp;
    private final int valueSize;
    private String topic = "";
    private int fiberId;
    private boolean hasFiberId = false;

    public Message(int keyIndex, String[] values)
    {
        this(keyIndex, values, Long.MIN_VALUE);
    }

    public Message(int keyIndex, String[] values, long timestamp)
    {
        this.keyIndex = keyIndex;
        this.values = values;
        this.timestamp = timestamp;

        int vSize = 0;
        for (String v : values) {
            vSize += v.length();
        }
        this.valueSize = vSize;
    }

    public Message(int keyIndex, String[] values, long timestamp, String topic)
    {
        this.keyIndex = keyIndex;
        this.values = values;
        this.timestamp = timestamp;
        this.topic = topic;

        int vSize = 0;
        for (String v : values) {
            vSize += v.length();
        }
        this.valueSize = vSize;
    }

    public Message(int keyIndex, String[] values, long timestamp, String topic, int fiberId)
    {
        this.keyIndex = keyIndex;
        this.values = values;
        this.timestamp = timestamp;
        this.topic = topic;
        this.fiberId = fiberId;

        int vSize = 0;
        for (String v : values) {
            vSize += v.length();
        }
        this.valueSize = vSize;
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

    /**
     * Get message value size in bytes
     * */
    public int getValueSize()
    {
        return valueSize;
    }

    // todo override equals, hashCode and toString
    @Override
    public String toString()
    {
        return String.format("keyIndex: %d, timestamp: %d, topic: %s", keyIndex, timestamp, topic);
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
