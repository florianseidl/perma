/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma;

import ch.sbb.perma.file.Compression;
import ch.sbb.perma.file.GZipCompression;
import ch.sbb.perma.file.NoCompression;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

/**
 * API to configure optional features in perma.
 * <ul>
 * <li>Compress: Switch on or off GZip compression of files. Default is off (false)</li>
 * </ul>
 *
 * @author u206123 (Florian Seidl)
 * @since 6.2, 2018.
 */
public class Options {
    public static class Builder {
        private boolean compress = false;
        private int compactThresholdPercent = 34;

        private Builder() {
        }

        Builder compress(boolean compress) {
            this.compress = compress;
            return this;
        }

        Builder compactThresholdPercent(int compactThresholdPercent) {
            this.compactThresholdPercent = compactThresholdPercent;
            return this;
        }

        public Options build() {
            Preconditions.checkArgument(
                    compactThresholdPercent >= 0 && compactThresholdPercent <= 100,
                    String.format("Invalid percent value for compactThresholdPercent: %d", compactThresholdPercent));
            return new Options(compress, compactThresholdPercent);
        }
    }

    private final boolean compress;
    private final int compactThresholdPercent;

    private Options(boolean compress, int compactThresholdPercent) {
        this.compress = compress;
        this.compactThresholdPercent = compactThresholdPercent;
    }

    public static Options compressed() {
        return new Builder().compress(true).build();
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

    public static Builder builder() {
        return new Builder();
    }

    Compression compression() {
        if (compress) {
            return GZipCompression.GZIP_COMPRESSION;
        }
        return NoCompression.NO_COMPRESSION;
    }

    CompactionThreshold compactionStrategy() {
        return new ChangedRemovedCompactionThreshold(compactThresholdPercent);
    }

    @Override
    public String toString() {
        return MoreObjects
                .toStringHelper(this)
                .add("compress", compress)
                .add("compactThresholdPercent", compactThresholdPercent)
                .toString();
    }
}
