/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.file;

import ch.sbb.perma.datastore.Compression;
import ch.sbb.perma.datastore.GZipCompression;
import ch.sbb.perma.datastore.NoCompression;

/**
 * Create database file names.
 *
 * @author u206123 (Florian Seidl)
 * @since 6.2, 2018.
 */
public class FileNameFormat {
    private final static String FILE_FORMAT = "%s_%d_%d.perma";
    private final static String GZIP_FILE_FORMAT = FILE_FORMAT + ".gzip";

    public final static FileNameFormat UNCOMPRESSED_FILE = new FileNameFormat(FILE_FORMAT, new NoCompression());
    public final static FileNameFormat GZIP_FILE = new FileNameFormat(GZIP_FILE_FORMAT, new GZipCompression());

    private final String format;
    private final Compression compression;

    private FileNameFormat(String format, Compression compression) {
        this.format = format;
        this.compression = compression;
    }

    public FileName fullFile(String permaName, int fullFileNumber) {
        return FileName.fullFile(compression, format, permaName, fullFileNumber);
    }
}
