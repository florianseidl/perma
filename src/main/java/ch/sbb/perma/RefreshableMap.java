/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma;

import java.util.Map;

/**
 * A map that can reload changes from its files.
 *
 * @author u206123 (Florian Seidl)
 * @since 5.3, 2018.
 */
public interface RefreshableMap<K,V> extends Refreshable, Map<K,V> {
}