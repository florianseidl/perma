/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.datastore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Compress or deflate GZip files.
 *
 * @author u206123 (Florian Seidl)
 * @since 6.2, 2018.
 */
public class GZipCompression implements Compression {

    @Override
    public OutputStream compress(OutputStream out) throws IOException {
        return new GZIPOutputStream(out, true);
    }

    @Override
    public InputStream deflate(InputStream in) throws IOException {
        return new GZIPInputStream(in);
    }

}
