package comp4321.group2.searchengine.utils;

import java.util.*;
import java.util.Map.Entry;

public class MapUtilities {
    public static <K, V extends Comparable<V>> V maxUsingStreamAndMethodReference(Map<K, V> map) {
        Optional<Entry<K, V>> maxEntry = map.entrySet()
            .stream()
            .max(Entry.comparingByValue());
        return maxEntry.get()
            .getValue();
    }

    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map, boolean asc) {
        List<Entry<K, V>> list = new ArrayList<>(map.entrySet());

        if (!asc) {
            list.sort(Collections.reverseOrder(Entry.comparingByValue()));
        } else {
            list.sort(Entry.comparingByValue());
        }

        Map<K, V> result = new LinkedHashMap<>();
        for (Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }

        return result;
    }
}
