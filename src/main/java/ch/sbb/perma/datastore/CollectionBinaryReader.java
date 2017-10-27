/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.datastore;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Read multiple items from bytes.
 *
 * @author u206123 (Florian Seidl)
 * @since 2.1, 2017.
 */
public class CollectionBinaryReader {

    private final BinaryReader reader;

    public CollectionBinaryReader(byte[] bytes) {
        reader = new BinaryReader(new ByteArrayInputStream(bytes));
    }

    public byte[] readNext() {
        try {
            return reader.readWithLength();
        }
        catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public int readLength() {
        try {
            return reader.readInt();
        }
        catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

}
