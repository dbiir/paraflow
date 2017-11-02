package cn.edu.ruc.iir.paraflow.loader.consumer.example;

import cn.edu.ruc.iir.paraflow.loader.consumer.utils.MessageSizeCalculator;

public class ListColumnsDataTypeTest
{
    private String configPath;

    private void listColumnDataType()
    {
        MessageSizeCalculator messageSizeCalculator = new MessageSizeCalculator();
        long size = messageSizeCalculator.caculate("exampleDb.exampleTbl");
        System.out.println("size : " + size);
    }

    public static void main(String[] args)
    {
        ListColumnsDataTypeTest listColumnsDataTypeTest = new ListColumnsDataTypeTest();
        listColumnsDataTypeTest.listColumnDataType();
    }
}
