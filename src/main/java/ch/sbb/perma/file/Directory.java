/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.file;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class Directory {
    private final File dir;

    public Directory(File dir) {
        this.dir = dir;
    }

    public List<String> listDir(FilenameFilter filenameFilter) {
        String[] list = dir.list(filenameFilter);
        return list != null ? Arrays.asList(list) : Collections.emptyList();
    }

}
