package comp4321.group2.searchengine.repositories;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;
import comp4321.group2.searchengine.utils.WordUtilities;


class InvertedIndexTest {
    @Test
    public void getValueCorrectly_NormativeCase() throws RocksDBException {
//        InvertedIndex.connect();
//        InvertedIndex.deleteAll();
//        ArrayList<Integer> locationsOne = new ArrayList<Integer>();
//        ArrayList<Integer> locationsTwo = new ArrayList<Integer>();
//        ArrayList<Integer> locationsThree = new ArrayList<Integer>();
//        ArrayList<Integer> locationsFour = new ArrayList<Integer>();
//        locationsOne.add(1);
//        locationsOne.add(2);
//        locationsTwo.add(3);
//        locationsTwo.add(4);
//        locationsThree.add(5);
//        locationsFour.add(6);
//        InvertedIndex.addEntry("00000001@0".getBytes(), WordUtilities.arrayListToString(locationsOne).getBytes());
//        InvertedIndex.addEntry("00000001@2".getBytes(), WordUtilities.arrayListToString(locationsTwo).getBytes());
//        InvertedIndex.addEntry("00000002@4".getBytes(), WordUtilities.arrayListToString(locationsThree).getBytes());
//        InvertedIndex.addEntry("00000002@6".getBytes(), WordUtilities.arrayListToString(locationsFour).getBytes());
//        InvertedIndex.printAll();
//        HashMap<String, ArrayList<Integer>> map = InvertedIndex.getValue("00000001@".getBytes());
//        for (Entry<String, ArrayList<Integer>> i : map.entrySet()) {
//            System.out.println(i.getKey());
//            System.out.println(i.getValue());
//        }
//        InvertedIndex.closeConnection();
    }
}
