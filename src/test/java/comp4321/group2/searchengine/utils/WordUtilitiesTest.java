package comp4321.group2.searchengine.utils;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import org.junit.jupiter.api.Test;
import comp4321.group2.searchengine.exceptions.InvalidWordIdConversionException;


class WordUtilitiesTest {
    @Test
    public void wordIdAndDocIdToDBKey_ShouldGenerateByteArrayCorrectly_NormativeCase()
        throws InvalidWordIdConversionException {
        int wordId = 1;
        int pageId = 1;
        byte[] dbKey = WordUtilities.wordIdAndPageIdToDBKey(wordId, pageId);
        String dbKeyStr = new String(dbKey);
        assertEquals("00000001@1", dbKeyStr);
        assertEquals(10, dbKey.length);
    }

    @Test
    public void arrayListToString_ShouldSerializeCorrectly_NormativeCase() {
        ArrayList<Integer> arrList = new ArrayList<Integer>();
        arrList.add(1);
        arrList.add(2);
        assertEquals("1\t2\t", WordUtilities.arrayListToString(arrList));
    }

    @Test
    public void stringToIntegerArrayList_ShouldGenerateArrayListCorrectly_NormativeCase() {
        String str = "1\t2\t";
        ArrayList<Integer> actualArrList = new ArrayList<Integer>();
        actualArrList.add(1);
        actualArrList.add(2);
        assertEquals(actualArrList, WordUtilities.stringToIntegerArrayList(str));
    }

    @Test
    public void stringToStringArrayList_ShouldGenerateArrayListCorrectly_NormativeCase() {
        String str = "1\t2\t";
        ArrayList<String> actualArrList = new ArrayList<String>();
        actualArrList.add("1");
        actualArrList.add("2");
        assertEquals(actualArrList, WordUtilities.stringToStringArrayList(str));
    }
}
