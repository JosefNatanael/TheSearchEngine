package comp4321.group2.searchengine.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ByteIntUtilitiesTest {

    @Test
    void convertIntToByteArrayAndBack() {
        assertEquals(4321, ByteIntUtilities.convertByteArrayToInt(ByteIntUtilities.convertIntToByteArray(4321)));
    }

    @Test
    void convertDoubleToByteArrayAndBack() {
        assertEquals(4321.0, ByteIntUtilities.convertByteArrayToDouble(ByteIntUtilities.doubleToByteArray(4321.0)));
    }
}
