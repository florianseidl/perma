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

    Directory(File dir) {
        this.dir = dir;
    }

    List<String> listDir(FilenameFilter filenameFilter) {
        String[] list = dir.list(filenameFilter);
        return list != null ? Arrays.asList(list) : Collections.emptyList();
    }

}
