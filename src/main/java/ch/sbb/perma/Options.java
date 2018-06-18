/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma;

import ch.sbb.perma.datastore.Compression;
import ch.sbb.perma.datastore.GZipCompression;
import ch.sbb.perma.datastore.NoCompression;

public class Options {
    public static class Builder {
        private boolean compress = false;
        private int compactThresholdPercent = 33;

        public Builder compress(boolean compress) {
            this.compress = compress;
            return this;
        }

        public Builder compactThresholdPercent(int compactThresholdPercent) {
            this.compactThresholdPercent = compactThresholdPercent;
            return this;
        }

        public Options build() {
            if(compactThresholdPercent < 0 || compactThresholdPercent > 100) {
                throw new IllegalArgumentException(String.format("Invalid percent value for compactThresholdPercent: %d", compactThresholdPercent));
            }
            return new Options(compress, compactThresholdPercent);
        }
    }
    private final boolean compress;
    private final int compactThresholdPercent;

    private Options(boolean compress, int compactThresholdPercent) {
        this.compress = compress;
        this.compactThresholdPercent = compactThresholdPercent;
    }

    public static Options defaults() {
        return new Builder().build();
    }

    static Options illegal() {
        return new Options(false, -1) {
            Compression compression() {
                throw new IllegalStateException("Not allowed to get Compression from options");
            }
        };
    }

    Compression compression() {
        if(compress) {
            return new GZipCompression();
        }
        return new NoCompression();
    }
}
