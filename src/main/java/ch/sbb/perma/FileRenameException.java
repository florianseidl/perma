/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma;

import java.io.IOException;

public class FileRenameException extends IOException {
    public FileRenameException(String s) {
        super(s);
    }
}
