package cn.edu.ruc.iir.paraflow.loader.utils;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * paraflow
 *
 * @author guodong
 */
public class DynamicStringArrayTest
{
    @Test
    public void simpleTest()
    {
        DynamicStringArray stringArray = new DynamicStringArray();
        String[] value0 = {"this", "is", "zero", "value"};
        String[] value1 = {"that", "was", "a", "second", "value"};
        stringArray.addValue(value0);
        stringArray.addValue(value1);
        String[] v0Res = stringArray.getValue(0);
        String[] v1Res = stringArray.getValue(1);
        assertArrayEquals(value0, v0Res);
        assertArrayEquals(value1, v1Res);
    }

    @Test
    public void growTest()
    {
        DynamicStringArray stringArray = new DynamicStringArray(10, 1);
        for (int i = 0; i < 10; i++) {
            String[] value = new String[11];
            for (int j = 0; j < 11; j++) {
                value[j] = String.valueOf(j + i);
            }
            stringArray.addValue(value);
        }
        for (int i = 0; i < 10; i++) {
            String[] expected = new String[11];
            for (int j = 0; j < 11; j++) {
                expected[j] = String.valueOf(j + i);
            }
            String[] result = stringArray.getValue(i);
            assertArrayEquals(expected, result);
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void clearTest()
    {
        DynamicStringArray stringArray = new DynamicStringArray(10, 1);
        for (int i = 0; i < 1; i++) {
            String[] value = new String[11];
            for (int j = 0; j < 11; j++) {
                value[j] = String.valueOf(j + i);
            }
            stringArray.addValue(value);
        }
        stringArray.clear();
    }
}
