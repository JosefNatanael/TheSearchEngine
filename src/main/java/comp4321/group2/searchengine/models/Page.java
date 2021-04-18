package comp4321.group2.searchengine.models;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class Page implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private String title = "";
    private String childUrls = "";
    private int size;
    private ZonedDateTime lastModified = null;

    public Page() {}

    /**
     * Constructor for Page. If lastModified cannot be parsed, the instance variable this.lastModified will be null.
     * @param title
     * @param url
     * @param lastModified
     * @param size
     */
    public Page(String title, String url, int size, String lastModified) {
        this.title = title;
        this.childUrls = url;
        this.size = size;
        try {
            if (lastModified != null) {
                this.lastModified = ZonedDateTime.parse(lastModified, DateTimeFormatter.RFC_1123_DATE_TIME);
            } else {
                this.lastModified = null;
            }
        } catch (Exception e) {
            this.lastModified = null;
        }
    }

    public String getTitle() {
        return title;
    }

    public String getChildUrls() {
        return childUrls;
    }

    public ZonedDateTime getLastModified() {
        return lastModified;
    }

    public int getSize() {
        return size;
    }

    public void print() {
        System.out.println("Title: " + this.title);
        System.out.println("Child Urls: " + this.childUrls);
        System.out.println("Size: " + this.size);
        System.out.println("Last Modified: " + this.lastModified);
    }

    /**
     * Serialize Page object to array of bytes
     * @param page
     * @return array of bytes
     * @throws IOException
     */
    public static byte[] serialize(Page page) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(page);
        oos.flush();
        return bos.toByteArray();
    }

    /**
     * Deserialize array of bytes to Page object
     * @param buf
     * @return Page object
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static Page deserialize(byte[] buf) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(buf);
        ObjectInputStream ois = new ObjectInputStream(bis);
        Page page = (Page) ois.readObject();
        ois.close();
        return page;
    }
}
