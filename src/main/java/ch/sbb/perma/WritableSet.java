/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma;

import java.util.Set;

/**
 * A set that can be written to a file.
 *
 * @author u206123 (Florian Seidl)
 * @since 5.3, 2018.
 */
public interface WritableSet<T> extends Writable<T, Object>, Set<T> {
}