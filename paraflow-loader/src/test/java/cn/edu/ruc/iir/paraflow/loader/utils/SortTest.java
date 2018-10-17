package cn.edu.ruc.iir.paraflow.loader.utils;

import org.junit.Test;

import java.util.Arrays;

/**
 * paraflow
 *
 * @author guodong
 */
public class SortTest
{
    @Test
    public void testSort()
    {
        int[] content = new int[10];
        for (int i = 0; i < 10; i++) {
            content[i] = 100 - i;
        }
        Arrays.sort(content);
        for (int i = 0; i < 10; i++) {
            System.out.println(content[i]);
        }
    }
}
