package cn.edu.ruc.iir.paraflow.commons.utils;

/**
 * paraflow
 *
 * @author guodong
 */
public class BytesUtils
{
    private BytesUtils()
    {
    }

    public static byte[] toBytes(int v)
    {
        byte[] result = new byte[4];
        result[0] = (byte) (v >> 24);
        result[1] = (byte) (v >> 16);
        result[2] = (byte) (v >> 8);
        result[3] = (byte) (v >> 0);
        return result;
    }
}
