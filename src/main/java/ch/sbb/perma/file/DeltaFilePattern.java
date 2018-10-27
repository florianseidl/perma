/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.file;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class DeltaFilePattern {

    private final static String DELTA_FILE_NAME_PATTERN_TEMPLATE = "%s_%d_([1-9]\\d*)\\.perma(\\.gzip)?";
    private final PermaFile fullFileName;
    private final Pattern pattern;

    DeltaFilePattern(PermaFile fullFileName, String permaName, int fullFileNumber) {
        this.fullFileName = fullFileName;
        this.pattern = Pattern.compile(String.format(DELTA_FILE_NAME_PATTERN_TEMPLATE, permaName, fullFileNumber));
    }

    ImmutableList<PermaFile> listDeltaFiles(File dir) {
        return new Directory(dir).listDir(this::accept)
                .stream()
                .map(this::parse)
                .sorted()
                .collect(ImmutableList.toImmutableList());
    }

    private boolean accept(File dir, String name) {
        return pattern.matcher(name).matches();
    }

    private PermaFile parse(String fileName) {
        Matcher matcher = pattern.matcher(fileName);
        Preconditions.checkArgument(
                matcher.find(),
                String.format("Invalid file name %s", fileName));
        return fullFileName.delta(parseFileNumber(matcher));
    }

    private int parseFileNumber(Matcher matcher) {
        return Integer.parseInt(matcher.group(1));
    }
}
