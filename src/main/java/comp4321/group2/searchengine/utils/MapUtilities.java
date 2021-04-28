package comp4321.group2.searchengine.utils;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

public class MapUtilities {
    public static <K, V extends Comparable<V>> V maxUsingStreamAndMethodReference(Map<K, V> map) {
        Optional<Entry<K, V>> maxEntry = map.entrySet()
            .stream()
            .max(Entry.comparingByValue());
        return maxEntry.get()
            .getValue();
    }
}
