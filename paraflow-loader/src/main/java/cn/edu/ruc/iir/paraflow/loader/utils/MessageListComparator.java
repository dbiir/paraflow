package cn.edu.ruc.iir.paraflow.loader.utils;

import cn.edu.ruc.iir.paraflow.commons.Message;

import java.util.Comparator;

public class MessageListComparator implements Comparator<Message>
{
    @Override
    public int compare(Message o1, Message o2)
    {
        if (o2.getTimestamp() < o1.getTimestamp()) {
            return -1;
        }
        if (o2.getTimestamp() > o1.getTimestamp()) {
            return 1;
        }
        return 0;
    }
}
