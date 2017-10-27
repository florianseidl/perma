/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.datastore;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Write multiple items to bytes.
 *
 * @author u206123 (Florian Seidl)
 * @since 2.1, 2017.
 */
public class CollectionBinaryWriter {

    private final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    private final BinaryWriter writer = new BinaryWriter(bos);

    public void writeNext(byte[] bytes) {
        try {
            writer.writeWithLength(bytes);
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public void writeLength(int length) {
        try {
            writer.writeInt(length);
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public byte[] toByteArray() {
        return bos.toByteArray();
    }
}
