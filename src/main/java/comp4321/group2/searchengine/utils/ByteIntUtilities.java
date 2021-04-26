package comp4321.group2.searchengine.utils;

import java.nio.ByteBuffer;

public class ByteIntUtilities {

    /**
     * convert int to bytes
     *
     * @return int conversion from byte[]
     */
    public static byte[] convertIntToByteArray(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    /**
     * convert bytes to int
     *
     * @return byte[] conversion from int
     */
    public static int convertByteArrayToInt(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
    }


    public static byte[] doubleToByteArray(double value) {
        byte[] bytes = new byte[8];
        ByteBuffer.wrap(bytes).putDouble(value);
        return bytes;
    }

    public static double convertByteArrayToDouble(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getDouble();
    }
}
