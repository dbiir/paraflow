package cn.edu.ruc.iir.paraflow.loader.consumer.utils;

import cn.edu.ruc.iir.paraflow.commons.message.Message;

import java.util.Comparator;

public class MessageListComparator implements Comparator<Message>
{
    @Override
    public int compare(Message o1, Message o2)
    {
        if (o2.getTimestamp().get() < o1.getTimestamp().get()) {
            return -1;
        }
        if (o2.getTimestamp().get() > o1.getTimestamp().get()) {
            return 1;
        }
        return 0;
    }
}
