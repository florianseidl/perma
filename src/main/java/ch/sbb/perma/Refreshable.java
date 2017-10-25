/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma;

import java.io.IOException;

/**
 * Reload changes from the files of the persisted map.
 *
 * @author u206123 (Florian Seidl)
 * @since 1.2, 2017.
 */
public interface Refreshable {
   void refresh() throws IOException;
}
