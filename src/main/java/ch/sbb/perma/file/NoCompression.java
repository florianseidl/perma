/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.file;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Empty implementation for uncompressed files.
 *
 * @author u206123 (Florian Seidl)
 * @since 6.2, 2018.
 */
public class NoCompression implements Compression {

    @Override
    public OutputStream compress(OutputStream out) {
        return out;
    }

    @Override
    public InputStream decompress(InputStream in) {
        return in;
    }

    @Override
    public FileNameFormat fileNameFormat() {
        return FileNameFormat.UNCOMPRESSED_FILE;
    }
}
