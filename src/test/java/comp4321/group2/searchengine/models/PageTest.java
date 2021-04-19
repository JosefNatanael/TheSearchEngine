package comp4321.group2.searchengine.models;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;
import comp4321.group2.searchengine.RocksDBApi;
import comp4321.group2.searchengine.repositories.PageIdToData;
import comp4321.group2.searchengine.repositories.URLToPageId;

class PageTest {
    @Test
    public void pageConstructor_parsesDateTimeCorrectly_NormativeCase() {
        Page page = new Page("title", "url", 10, "Fri, 20 Mar 2020 03:23:35 +0800", 100);
        ZonedDateTime lastModified = page.getLastModified();
        assertEquals("FRIDAY", lastModified.getDayOfWeek().toString());
        assertEquals(20, lastModified.getDayOfMonth());
        assertEquals(3, lastModified.getMonthValue());
        assertEquals(2020, lastModified.getYear());
        assertEquals(3, lastModified.getHour());
        assertEquals(23, lastModified.getMinute());
        assertEquals(35, lastModified.getSecond());
        assertEquals("+08:00", lastModified.getOffset().toString());
    }

    @Test
    public void pageConstructor_handlesWrongDateTimeInput() {
        Page page = new Page("title", "url", 10, "Fri", 100);
        ZonedDateTime lastModified = page.getLastModified();
        assertNull(lastModified);
    }

    @Test
    public void serializePage_NormativeCase() throws RocksDBException, IOException, ClassNotFoundException {
//        Page newPage = new Page("title", "url", 10, "Fri, 20 Mar 2020 03:23:35 GMT", 100);
//        RocksDBApi.connect();
//        RocksDBApi.reset();
//        RocksDBApi.addPageData(newPage, "url");
//        byte[] byteArr = PageIdToData.getValue(URLToPageId.getValue("url"));
//        Page page = Page.deserialize(byteArr);
//        System.out.println(page.getTitle());
//
//        assertEquals(page.getTitle(), newPage.getTitle());
//        RocksDBApi.closeAllDBConnections();
    }

    @Test
    public void correctlyParseAndFormatZonedDateTime() {
        String lastModified = "Fri, 20 Mar 2020 03:23:35 GMT";
        ZonedDateTime parsed = ZonedDateTime.parse(lastModified, DateTimeFormatter.RFC_1123_DATE_TIME);
        DateTimeFormatter dateformat = DateTimeFormatter.RFC_1123_DATE_TIME;
        String formatted = parsed.format(dateformat);
        assertEquals(lastModified, formatted);
    }
}
