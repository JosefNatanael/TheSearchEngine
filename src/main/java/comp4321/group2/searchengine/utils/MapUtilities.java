package comp4321.group2.searchengine.utils;

import java.util.*;
import java.util.Map.Entry;

import static java.util.stream.Collectors.toMap;

public class MapUtilities {
    public static <K, V extends Comparable<V>> V maxUsingStreamAndMethodReference(Map<K, V> map) {
        Optional<Entry<K, V>> maxEntry = map.entrySet()
            .stream()
            .max(Entry.comparingByValue());
        return maxEntry.get()
            .getValue();
    }

    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map, boolean asc, int limit) {
        List<Entry<K, V>> list = new ArrayList<>(map.entrySet());

        if (!asc) {
            list.sort(Collections.reverseOrder(Entry.comparingByValue()));
        } else {
            list.sort(Entry.comparingByValue());
        }

        int i = 0;
        Map<K, V> result = new LinkedHashMap<>();
        for (Entry<K, V> entry : list) {
            if (i >= limit) break;
            result.put(entry.getKey(), entry.getValue());
            i++;
        }

        return result;
    }

    public static <K,V> Map<K,V> getFirstEntries(Map<K,V> sortedMap, int elementsToReturn) {
        return sortedMap.entrySet()
            .stream()
            .limit(elementsToReturn)
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
