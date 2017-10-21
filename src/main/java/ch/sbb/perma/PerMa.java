/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma;

import java.util.Map;

/**
 * A container for a persistent map.
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public interface PerMa<K,V> {
    Map<K,V> map();
}
