package comp4321.group2.searchengine.utils;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Comparator;

public class MapUtilities {
    public static <K, V extends Comparable<V>> V maxUsingStreamAndMethodReference(Map<K, V> map) {
        Optional<Entry<K, V>> maxEntry = map.entrySet()
            .stream()
            .max(Comparator.comparing(Map.Entry::getValue));
        return maxEntry.get()
            .getValue();
    }
}
