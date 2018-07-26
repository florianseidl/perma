/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.file;

import ch.sbb.perma.FileRenameException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;

class TempFile {
    private final static String TEMP_FILE_FORMAT = "%s-%s.perma.temp";
    private final static String TEMP_FILE_PATTERN_TEMPLATE =
            String.format(
                    TEMP_FILE_FORMAT
                            .replace(".", "\\.")
                            .replace("-", "\\-"),
                    "%s", ".+");

    private final File dir;
    private final String permaName;
    private final File file;

    TempFile(File dir, String permaName) {
        this.dir = dir;
        this.permaName = permaName;
        this.file = new File(dir, String.format(
                        TEMP_FILE_FORMAT,
                        permaName,
                        UUID.randomUUID()));
    }

    void moveTo(File targetFile) throws FileRenameException {
        if(!file.renameTo(targetFile)) {
            throw new FileRenameException(String.format("Could not rename temporary file %s to perma set file %s",
                    file,
                    targetFile));
        }
    }

    <R> R withOutputStream(IOFunction<OutputStream, R> function) throws IOException {
        try(OutputStream out = new FileOutputStream(file)) {
            return function.apply(out);
        }
    }

    void deleteStaleTempFiles() {
        String pattern = String.format(TEMP_FILE_PATTERN_TEMPLATE, permaName);
        new Directory(dir).listDir((d, fileName) -> fileName.matches(pattern))
                .forEach(fileName -> new File(dir, fileName).delete());
    }

    @Override
    public String toString() {
        return file.toString();
    }
}
