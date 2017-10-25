/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma;

import java.io.IOException;

/**
 * A map or set can be written to a file.
 * <p>
 *     Persist writes to a delta or a full file and
 *     can be compacted to a full file
 * </p>
 *
 * @author u206123 (Florian Seidl)
 * @since 1.2, 2017.
 */
public interface Writeable<K,V> {
    void persist() throws IOException;
    void compact() throws IOException;
}
