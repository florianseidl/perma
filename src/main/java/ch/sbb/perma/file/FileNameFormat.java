/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.file;

/**
 * String from file name.
 *
 * @author u206123 (Florian Seidl)
 * @since 6.2, 2018.
 */
public class FileNameFormat {
    private final static String UNCOMPRESSED_FILE_FORMAT = "%s_%d_%d.perma";
    private final static String GZIP_FILE_FORMAT = UNCOMPRESSED_FILE_FORMAT + ".gzip";

    final static FileNameFormat UNCOMPRESSED_FILE = new FileNameFormat(UNCOMPRESSED_FILE_FORMAT);
    final static FileNameFormat GZIP_FILE = new FileNameFormat(GZIP_FILE_FORMAT);

    private final String format;

    private FileNameFormat(String format) {
        this.format = format;
    }

    public String format(String permaName, int fullFileNumber, int deltaFileNumber) {
        return String.format(format, permaName, fullFileNumber, deltaFileNumber);
    }
}
