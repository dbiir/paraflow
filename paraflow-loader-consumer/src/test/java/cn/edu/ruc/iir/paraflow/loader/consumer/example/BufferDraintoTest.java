package cn.edu.ruc.iir.paraflow.loader.consumer.example;

import cn.edu.ruc.iir.paraflow.commons.message.Message;
import cn.edu.ruc.iir.paraflow.loader.consumer.buffer.ReceiveQueueBuffer;

import java.util.LinkedList;

public class BufferDraintoTest
{
    private ReceiveQueueBuffer buffer = ReceiveQueueBuffer.INSTANCE();

    private void drainto()
    {
        LinkedList<Message> messages = new LinkedList<>();
        buffer.drainTo(messages);
        for (Message message : messages) {
            System.out.println("message : " + message);
        }
    }

    public static void main(String[] args)
    {
        BufferDraintoTest bufferDraintoTest = new BufferDraintoTest();
        bufferDraintoTest.drainto();
    }
}
