/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma;

import java.util.Set;

/**
 * A container for a persistent set.
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public interface PerMaSet<T> {
    Set<T> set();
}
