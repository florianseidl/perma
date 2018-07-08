/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.file;

import ch.sbb.perma.datastore.Compression;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;

import java.util.Objects;

public final class FileName implements Comparable<FileName> {
    private final Compression compression;
    private final String format;
    private final String permaName;
    private final int fullFileNumber;
    private final int deltaFileNumber;

    private FileName(Compression compression, String format, String permaName, int fullFileNumber, int deltaFileNumber) {
        this.compression = compression;
        this.format = format;
        this.permaName = permaName;
        this.fullFileNumber = fullFileNumber;
        this.deltaFileNumber = deltaFileNumber;
    }

    static FileName fullFile(Compression compression, String format, String permaName, int fullFileNumber) {
        return new FileName(compression, format, permaName, fullFileNumber, 0);
    }

    public FileName nextDelta() {
        return new FileName(compression, format, permaName, fullFileNumber, deltaFileNumber + 1);
    }

    public FileName nextFull(Compression compression) {
        return compression.fileNameFormat().fullFile(permaName, fullFileNumber + 1);
    }

    public FileName delta(int nr) {
        return new FileName(compression, format, permaName, fullFileNumber, nr);
    }

    public Compression compression() {
        return compression;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        FileName otherFileName = (FileName) other;
        return Objects.equals(permaName, otherFileName.permaName) &&
                fullFileNumber == otherFileName.fullFileNumber &&
                deltaFileNumber == otherFileName.deltaFileNumber;
    }

    @Override
    public int hashCode() {
        return Objects.hash(permaName, fullFileNumber, deltaFileNumber);
    }

    @Override
    public int compareTo(FileName other) {
        Preconditions.checkArgument(permaName.equals(other.permaName), "Can only compare within the same perma");
        return ComparisonChain.start()
                .compare(fullFileNumber, other.fullFileNumber)
                .compare(deltaFileNumber, other.deltaFileNumber)
                .result();
    }

    @Override
    public String toString() {
        return String.format(format, permaName, fullFileNumber, deltaFileNumber);
    }
}
