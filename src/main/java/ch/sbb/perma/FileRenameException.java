/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma;

import java.io.IOException;

/**
 * File rename did return false for some reason (file lock,...).
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public class FileRenameException extends IOException {
    public FileRenameException(String s) {
        super(s);
    }
}
