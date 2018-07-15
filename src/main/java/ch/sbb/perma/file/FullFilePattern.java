/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.file;

import ch.sbb.perma.datastore.Compression;
import ch.sbb.perma.datastore.GZipCompression;
import ch.sbb.perma.datastore.NoCompression;
import com.google.common.base.Preconditions;

import java.io.File;
import java.io.FilenameFilter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * File name from String.
 *
 * @author u206123 (Florian Seidl)
 * @since 6.2, 2018.
 */
public class FullFilePattern implements FilenameFilter {
    private final static String FULL_FILE_NAME_PATTERN_TEMPLATE = "%s_(\\d+)_0\\.perma(\\.gzip)?";
    private final static Pattern GZIP_FILE_NAME_PATTERN = Pattern.compile(".*\\.perma\\.gzip");

    private final Pattern pattern;
    private final String permaName;

    public FullFilePattern(String permaName) {
        this.permaName = permaName;
        this.pattern = Pattern.compile(String.format(FULL_FILE_NAME_PATTERN_TEMPLATE, permaName));
    }

    public PermaFile parse(File dir, String fileName) {
        Matcher matcher = pattern.matcher(fileName);
        Preconditions.checkArgument(
                matcher.find(),
                String.format("Invalid file name %s", fileName));
        return PermaFile.fullFile(compressionOf(fileName), dir, permaName, parseFileNumber(matcher));
    }

    @Override
    public boolean accept(File dir, String name) {
        return pattern.matcher(name).matches();
    }

    private Compression compressionOf(String filename) {
        return GZIP_FILE_NAME_PATTERN.matcher(filename).matches() ? new GZipCompression() : new NoCompression();
    }

    private int parseFileNumber(Matcher matcher) {
        return Integer.parseInt(matcher.group(1));
    }
}
