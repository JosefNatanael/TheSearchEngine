package comp4321.group2.searchengine.utils;

import java.nio.ByteBuffer;

public class ByteIntUtilities {

    /**
     * convert int to bytes
     *
     * @param value
     * @return int conversion from byte[]
     */
    public static byte[] convertIntToByteArray(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    /**
     * convert bytes to int
     *
     * @param bytes
     * @return byte[] conversion from int
     */
    public static int convertByteArrayToInt(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
    }
}