package cn.edu.ruc.iir.paraflow.loader.utils;

/**
 * paraflow
 *
 * @author guodong
 */
// todo valueOffsets is not adaptive
public class DynamicStringArray
{
    private static final int DEFAULT_CHUNK_SIZE = 8 * 1024;
    private static final int DEFAULT_CHUNK_NUM = 128;
    private static final int DEFAULT_VALUE_NUM = 1024 * 512;

    private final int chunkSize;
    private final int chunkNum;

    private String[][] data;                 // real data
    private int dataLen = 0;                 // max data element index + 1
    private int[] valueOffsets;              // array containing start offset of each value in data
    private int valueNum = 0;                // num of values
    private int initChunks = 0;              // the number of created chunks

    public DynamicStringArray(int initChunkSize, int initChunkNum)
    {
        this.chunkSize = initChunkSize;
        this.chunkNum = initChunkNum;

        this.data = new String[chunkNum][chunkSize];
        this.valueOffsets = new int[DEFAULT_VALUE_NUM];
    }

    public DynamicStringArray()
    {
        this.chunkSize = DEFAULT_CHUNK_SIZE;
        this.chunkNum = DEFAULT_CHUNK_NUM;

        this.data = new String[chunkNum][chunkSize];
        this.valueOffsets = new int[DEFAULT_VALUE_NUM];
    }

    public String[] getValue(int index) throws IndexOutOfBoundsException
    {
        if (index >= valueNum) {
            throw new IndexOutOfBoundsException("Value index out of bound.");
        }
        int startOffset = valueOffsets[index];
        int endOffset;
        if (index == valueNum - 1) {
            endOffset = dataLen;
        }
        else {
            endOffset = valueOffsets[index + 1];
        }
        return range(startOffset, endOffset);
    }

    /**
     * Get elements array from startIndex to endIndex(not included)
     * */
    private String[] range(int startIndex, int endIndex)
    {
        int valueSize = endIndex - startIndex;
        String[] result = new String[valueSize];
        int bci = startIndex / chunkSize;
        int bco = startIndex % chunkSize;
        int eci = endIndex / chunkSize;
        int eco = endIndex % chunkSize;
        // value elements are in the same chunk
        if (bci == eci) {
            System.arraycopy(data[bci], bco, result, 0, (eco - bco));
        }
        // value elements are in different chunks
        else {
            int valueEleIndex = 0;
            int tmpBco;
            int tmpEco;
            int tmpBci = bci;
            while (valueEleIndex < valueSize) {
                if (tmpBci == bci) {
                    tmpBco = bco;
                    tmpEco = chunkSize;
                }
                else if (tmpBci == eci) {
                    tmpBco = 0;
                    tmpEco = eco;
                }
                else {
                    tmpBco = 0;
                    tmpEco = chunkSize;
                }
                System.arraycopy(data[tmpBci], tmpBco, result, valueEleIndex, tmpEco - tmpBco);
                tmpBci++;
                valueEleIndex += tmpEco - tmpBco;
            }
        }

        return result;
    }

    public void addValue(String[] value)
    {
        int valueSize = value.length;
        int bci = dataLen / chunkSize;                  // begin chunk index
        int bco = dataLen % chunkSize;                  // begin chunk offset
        int eci = (dataLen + valueSize) / chunkSize;    // end chunk index
        int eco = (dataLen + valueSize) % chunkSize;    // end chunk offset
        grow(eci);
        // record start offset in data
        valueOffsets[valueNum] = dataLen;
        // value elements are in the same chunk
        if (bci == eci) {
            System.arraycopy(value, 0, data[bci], bco, valueSize);
        }
        // value elements are not in the same chunk
        else {
            int valueEleIndex = 0;
            int tmpBco;
            int tmpEco;
            int tmpBci = bci;
            while (valueEleIndex < valueSize) {
                if (tmpBci == bci) {
                    tmpBco = bco;
                    tmpEco = chunkSize;
                }
                else if (tmpBci == eci) {
                    tmpBco = 0;
                    tmpEco = eco;
                }
                else {
                    tmpBco = 0;
                    tmpEco = chunkSize;
                }
                System.arraycopy(value, valueEleIndex, data[tmpBci], tmpBco, tmpEco - tmpBco);
                tmpBci++;
                valueEleIndex += tmpEco - tmpBco;
            }
        }
        valueNum++;
        dataLen += valueSize;
    }

    private void grow(int chunkIndex)
    {
        if (chunkIndex >= initChunks) {
            if (chunkIndex >= data.length) {
                int newSize = Math.max(chunkIndex + 1, 2 * data.length);
                String[][] newChunk = new String[newSize][];
                System.arraycopy(data, 0, newChunk, 0, data.length);
                data = newChunk;
            }
            for (int i = initChunks; i <= chunkIndex; i++) {
                data[i] = new String[chunkSize];
            }
            initChunks = chunkIndex + 1;
        }
    }

    public void clear()
    {
        this.initChunks = 0;
        this.valueNum = 0;
        this.dataLen = 0;
        for (int i = 0; i < data.length; i++) {
            data[i] = null;
        }
    }
}
