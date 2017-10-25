/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.datastore;

import java.util.zip.Checksum;

/**
 * An empty implementation of checksum for usage if no crc32 checksum is desired.
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
class NoChecksum implements Checksum {

    static final Checksum INSTANCE = new NoChecksum();

    private NoChecksum() {
    }

    @Override
    public void update(int i) {
    }

    @Override
    public void update(byte[] bytes, int i, int i1) {
    }

    @Override
    public long getValue() {
        return 0;
    }

    @Override
    public void reset() {
    }

}
