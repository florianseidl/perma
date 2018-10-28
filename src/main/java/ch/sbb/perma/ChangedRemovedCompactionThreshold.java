/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma;

/**
 * Compress if the given percentage of the currenty present records has been removed and changed.
 *
 * @author u206123 (Florian Seidl)
 * @since 6.2, 2018.
 */
public class ChangedRemovedCompactionThreshold implements CompactionThreshold {
    private final double compactThreshold;

    ChangedRemovedCompactionThreshold(int compactThresholdPercent) {
        this.compactThreshold = compactThresholdPercent / 100.0;
    }

    @Override
    public boolean triggerCompaction(int entriesRemoved, int entriesUpdated, int totalSizeOld) {
        return ((double)entriesRemoved + entriesUpdated) > (totalSizeOld * compactThreshold);
    }

}
