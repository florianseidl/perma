/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.datastore;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Checksum;

/**
 * Helper methods to write and read bytes from and to a stream.
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
class BinaryWriter {
    private final static int NULL_LENGTH = -1;

    private final OutputStream out;
    private final Checksum checksum;

    public BinaryWriter(OutputStream out, Checksum crc32) {
        this.out = out;
        this.checksum = crc32;
    }

    public BinaryWriter(OutputStream out) {
        this.out = out;
        this.checksum = NoChecksum.INSTANCE;
    }

    void writeWithLength(byte[] bytes) throws IOException {
        if(bytes == null) {
            writeInt(NULL_LENGTH);
            return;
        }
        writeInt(bytes.length);
        write(bytes);
    }

    void writeByte(int value) throws IOException {
        checksum.update(value);
        out.write(value);
    }

    void writeInt(int value) throws IOException {
        write(Ints.toByteArray(value));
     }

    void writeLong(long value) throws IOException {
        write(Longs.toByteArray(value));
    }

    void write(byte[] bytes) throws IOException {
        checksum.update(bytes, 0 , bytes.length);
        out.write(bytes);
    }

    void writeChecksum() throws IOException {
        new BinaryWriter(out).writeLong(checksum.getValue());
    }
}
