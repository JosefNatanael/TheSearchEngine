package comp4321.group2.searchengine.repositories;

import java.io.File;
import org.junit.jupiter.api.Test;

class MetadataTest {
    @Test
    public void debugPurposesOnly() {
        File directory = new File("./src/main/java/tables");
        System.out.println(directory.getAbsolutePath());
        String[] pathnames = directory.list();

        for (String pathname : pathnames) {
            System.out.println(pathname);
        }
    }
}
