package comp4321.group2.searchengine.utils;

import org.apache.commons.lang3.tuple.MutablePair;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class StopStemTest {

    @Test
    void getStopUnstemStemPair() {
        String dirtyString = "The HKUST President's Cup is an annual event for undergraduate students of the University to compete for awards based on outstanding achievements in research and innovation";
        MutablePair<ArrayList<String>, ArrayList<String>> pair = StopStem.getStopUnstemStemPair(dirtyString);
        String[] expUnstem = {"The", "HKUST", "President\'s", "Cup", "is", "an", "annual", "event", "for", "undergraduate", "students", "of", "the", "University", "to", "compete", "for", "awards", "based", "on", "outstanding", "achievements", "in", "research", "and", "innovation"};
        String[] expStem = {"the", "hkust", "presid", "cup", "annual", "event", "undergradu", "student", "univers", "compet", "award", "base", "outstand", "achiev", "innov"};
        ArrayList<String> expUnstemList = new ArrayList<>(Arrays.asList(expUnstem));
        ArrayList<String> expStemList = new ArrayList<>(Arrays.asList(expStem));
        assertEquals(expUnstemList, pair.getLeft());
        assertEquals(expStemList, pair.getRight());
    }
}
