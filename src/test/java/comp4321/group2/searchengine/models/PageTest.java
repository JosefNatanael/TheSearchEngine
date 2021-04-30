package comp4321.group2.searchengine.models;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class PageTest {
    @Test
    public void pageConstructor_parsesDateTimeCorrectly_NormativeCase() {
        Page page = new Page("title", "url", 10, "Fri, 20 Mar 2020 03:23:35 +0800", 100, "url");
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
        Page page = new Page("title", "url", 10, "Fri", 100, "url");
        ZonedDateTime lastModified = page.getLastModified();
        assertNull(lastModified);
    }

    @Test
    public void correctlyParseAndFormatZonedDateTime() {
        String lastModified = "Fri, 20 Mar 2020 03:23:35 GMT";
        ZonedDateTime parsed = ZonedDateTime.parse(lastModified, DateTimeFormatter.RFC_1123_DATE_TIME);
        DateTimeFormatter dateformat = DateTimeFormatter.RFC_1123_DATE_TIME;
        String formatted = parsed.format(dateformat);
        assertEquals(lastModified, formatted);
    }

    @Test
    public void serializeDeserializeTest() throws IOException, ClassNotFoundException {
        Page before = new Page("Some title", "url1, url2", 1, "Fri, 20 Mar 2020 03:23:35 GMT", 1, "url");
        Page after = Page.deserialize((Page.serialize(before)));
        assertEquals("Some title", after.getTitle());
        assertEquals("url1, url2", after.getChildUrls());
        assertEquals(1, after.getSize());
        assertEquals(1, after.getTfmax());
        assertEquals("url", after.getUrl());
    }
}
