/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.datastore;

/**
 * Create database file names.
 *
 * @author u206123 (Florian Seidl)
 * @since 6.2, 2018.
 */
public class FileNameFormat {
    private final static String FILE_FORMAT = "%s_%d_%d.perma";
    private final static String GZIP_FILE_FORMAT = FILE_FORMAT + ".gzip";

    public final static FileNameFormat UNCOMPRESSED_FILE = new FileNameFormat(FILE_FORMAT);
    public final static FileNameFormat GZIP_FILE = new FileNameFormat(GZIP_FILE_FORMAT);

    public class FileNameTempate {
        private final String name;

        private FileNameTempate(String name) {
            this.name = name;
        }

        public String format(int fullFileNumber, int deltaFileNumber) {
            return String.format(format, name, fullFileNumber, deltaFileNumber);
        }
    }
    private final String format;

    private FileNameFormat(String format) {
        this.format = format;
    }

    public FileNameTempate template(String name) {
        return new FileNameTempate(name);
    }
}
