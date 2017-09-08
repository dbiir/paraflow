package cn.edu.ruc.iir.paraflow.commons.message;

import java.util.Optional;

public class Message
{
    private int keyIndex;
    private String key;
    private String[] values;
    private long timestamp = Long.MIN_VALUE;
    private String topic;

    public Message(int keyIndex, String[] values)
    {
        this.keyIndex = keyIndex;
        this.values = values;
    }

    public Message(int keyIndex, String[] values, long timestamp)
    {
        this.key = key;
        this.values = values;
        this.timestamp = timestamp;
    }

    public void setKey(String key)
    {
        this.key = key;
    }

    public String getKey()
    {
        return key;
    }

    public String[] getValues()
    {
        return values;
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

    public String getTopic()
    {
        return topic;
    }

    // todo override equals, hashCode and toString
}
