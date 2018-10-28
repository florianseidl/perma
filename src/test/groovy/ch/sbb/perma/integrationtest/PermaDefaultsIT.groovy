/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.integrationtest

import ch.sbb.perma.Options

/**
 * Test the performance and memory usage with real load and default settings (without compression).
 * <p>
 * Warning: will require 10GB ram and disk space. Disabled by default in POM.
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0 , 2017.
 */
class PermaDefaultsIT extends AbstractPermaIT {
    @Override
    Options options() {
        return Options.defaults()
    }
}