package cn.edu.ruc.iir.paraflow.loader.consumer.utils;

import cn.edu.ruc.iir.paraflow.commons.message.Message;

import java.util.Comparator;

public class MessageListComparator implements Comparator<Message>
{
    @Override
    public int compare(Message o1, Message o2)
    {
        return (o2.getKeyIndex() - o1.getKeyIndex());
    }
}
