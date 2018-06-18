/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.datastore;

import java.io.InputStream;
import java.io.OutputStream;

public class NoCompression implements Compression {

    @Override
    public OutputStream compress(OutputStream out) {
        return out;
    }

    @Override
    public InputStream deflate(InputStream in) {
        return in;
    }
}
