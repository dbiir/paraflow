package cn.edu.ruc.iir.paraflow.commons.message;

import java.util.Optional;

public class Message
{
    private long key = Long.MIN_VALUE;
    private byte[] bytes;
    private long timestamp = Long.MIN_VALUE;

    public Message(byte[] bytes)
    {
        this.bytes = bytes;
    }

    public Message(long key, byte[] bytes, long timestamp)
    {
        this.key = key;
        this.bytes = bytes;
        this.timestamp = timestamp;
    }

    public Optional<Long> getKey()
    {
        if (key != Long.MIN_VALUE) {
            return Optional.of(key);
        }
        return Optional.empty();
    }

    public byte[] getBytes()
    {
        return bytes;
    }

    public Optional<Long> getTimestamp()
    {
        if (key != Long.MIN_VALUE) {
            return Optional.of(timestamp);
        }
        return Optional.empty();
    }

    // TODO override equals, hashCode and toString
}
