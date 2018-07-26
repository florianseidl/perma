/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.datastore;

import ch.sbb.perma.file.FileNameFormat;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Compress or decompress as configured or as determined from the existing files.
 *
 * @author u206123 (Florian Seidl)
 * @since 6.2, 2018.
 */
public interface Compression {
    OutputStream compress(OutputStream out) throws IOException;
    InputStream decompress(InputStream in) throws IOException;
    FileNameFormat fileNameFormat();

}
