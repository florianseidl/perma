/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.datastore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface Compression {
    OutputStream compress(OutputStream out) throws IOException;
    InputStream deflate(InputStream in) throws IOException;

}
