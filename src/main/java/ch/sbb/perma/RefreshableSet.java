/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma;

import java.util.Set;

/**
 * A set that can reload changes from its files.
 *
 * @author u206123 (Florian Seidl)
 * @since 5.3, 2018.
 */
public interface RefreshableSet<T> extends Refreshable, Set<T> {
}