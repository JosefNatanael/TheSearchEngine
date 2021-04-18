package comp4321.group2.searchengine.exceptions;

import java.lang.Exception;

public class InvalidWordIdConversionException extends Exception {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public InvalidWordIdConversionException(String wordId) {
        super("WordId length exceeded 8 for WordId: " + wordId);
    }
}
