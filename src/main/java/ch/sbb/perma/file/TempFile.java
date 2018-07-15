/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.file;

import ch.sbb.perma.FileRenameException;
import ch.sbb.perma.datastore.Compression;

import java.io.*;
import java.util.UUID;

public class TempFile {
    private final static String TEMP_FILE_FORMAT = "%s-%s.perma.temp";
    private final static String TEMP_FILE_PATTERN_TEMPLATE = String.format(TEMP_FILE_FORMAT, "%s", ".+");

    private final File targetFile;
    private final Compression compression;
    private final File file;

    public TempFile(File targetFile, Compression compression, File file) {
        this.targetFile = targetFile;
        this.compression = compression;
        this.file = file;
    }

    static TempFile create(File targetFile, Compression compression, File dir, String permaName) {
        return new TempFile(
                targetFile,
                compression,
                new File(dir, String.format(
                        TEMP_FILE_FORMAT,
                        permaName,
                        UUID.randomUUID())));
    }

    public OutputStream outputStream() throws IOException {
        return compression.compress(new FileOutputStream(file));
    }

    public void moveToTarget() throws FileRenameException {
        if(!file.renameTo(targetFile)) {
            throw new FileRenameException(String.format("Could not rename temporary file %s to perma set file %s",
                    file,
                    targetFile));
        }
    }

}
