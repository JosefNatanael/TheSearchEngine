package comp4321.group2.searchengine.repositories;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import comp4321.group2.searchengine.utils.WordUtilities;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PropertiesGetterTest {

    File directory = new File("./src/main/java/searchengine/repositories/properties/pageIdToData.properties");
    String absPathDir = directory.getAbsolutePath();
    Properties props = new Properties();

    @BeforeEach
    public void init() throws FileNotFoundException, IOException {
        props.load(new FileInputStream(absPathDir));
    }

    @Test
    public void correctlyGetExistingProperty() {
        assertEquals("./src/main/java/tables/PageIdToData", props.getProperty("rocksdb.dataFolder"));
    }

    @Test
    public void correctlyGetNonExistKeyProperty() {
        assertEquals("", props.getProperty("do_not_fill"));
    }

    @Test
    public void correctlyGetNothingProperty() {
        assertNull(props.getProperty("king_dragon_sends_his_regards"));
    }
}

