/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma;

/**
 * Decide if compaction or delta peristence is performed.
 *
 * @author u206123 (Florian Seidl)
 * @since 6.2, 2018.
 */
public interface CompactionThreshold {
    boolean triggerCompaction(int entriesAdded, int entriesRemoved, int totalSize);
}
