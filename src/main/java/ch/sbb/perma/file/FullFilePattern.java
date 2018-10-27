/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.file;

import com.google.common.base.Preconditions;

import java.io.File;
import java.util.Comparator;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * File name from String.
 *
 * @author u206123 (Florian Seidl)
 * @since 6.2, 2018.
 */
class FullFilePattern {
    private final static String FULL_FILE_NAME_PATTERN_TEMPLATE = "%s_(\\d+)_0\\.perma(\\.gzip)?";
    private final static Pattern GZIP_FILE_NAME_PATTERN = Pattern.compile(".*\\.perma\\.gzip");

    private final Pattern pattern;
    private final String permaName;

    FullFilePattern(String permaName) {
        this.permaName = permaName;
        this.pattern = Pattern.compile(String.format(FULL_FILE_NAME_PATTERN_TEMPLATE, permaName));
    }

    Optional<PermaFile> latestFullFile(File dir) {
        return new Directory(dir)
                .listDir(this::accept)
                .stream()
                .map(fileName -> parse(dir, fileName))
                .max(Comparator.naturalOrder());
    }

    private PermaFile parse(File dir, String fileName) {
        Matcher matcher = pattern.matcher(fileName);
        Preconditions.checkArgument(
                matcher.find(),
                String.format("Invalid file name %s", fileName));
        return PermaFile.fullFile(compressionOf(fileName), dir, permaName, parseFileNumber(matcher));
    }

    private boolean accept(File dir, String name) {
        return pattern.matcher(name).matches();
    }

    private Compression compressionOf(String filename) {
        return GZIP_FILE_NAME_PATTERN.matcher(filename).matches() ? new GZipCompression() : new NoCompression();
    }

    private int parseFileNumber(Matcher matcher) {
        return Integer.parseInt(matcher.group(1));
    }
}
