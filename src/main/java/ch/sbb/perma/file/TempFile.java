/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.file;

import java.io.File;
import java.util.UUID;

public class TempFile {
    private final static String TEMP_FILE_FORMAT = "%s-%s.perma.temp";
    private final static String TEMP_FILE_PATTERN_TEMPLATE = String.format(TEMP_FILE_FORMAT, "%s", ".+");

    private final String permaName;
    private final File file;

    public TempFile(String permaName, File file) {
        this.permaName = permaName;
        this.file = file;
    }

    public static TempFile create(File dir, String permaName) {
        return new TempFile(
                permaName,
                new File(dir, String.format(
                        TEMP_FILE_FORMAT,
                        permaName,
                        UUID.randomUUID())));
    }

}
