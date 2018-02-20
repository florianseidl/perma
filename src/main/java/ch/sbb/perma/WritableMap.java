/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma;

import java.util.Map;

/**
 * A map that can be written to a file.
 *
 * @author u206123 (Florian Seidl)
 * @since 5.3, 2018.
 */
public interface WritableMap<K,V> extends Writable<K,V>, Map<K,V> {
}