/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.integrationtest

import ch.sbb.perma.Options

/**
 * Test the performance and memory usage with real load using compression.
 * <p>
 * Warning: will require 10GB ram and a bit less disk space. Disabled by default in POM.
 *
 * @author u206123 (Florian Seidl)
 * @since 6.2, 2018.
 */
class PermaWithCompressionIT extends AbstractPermaIT {
    @Override
    Options options() {
        return Options.compressed()
    }
}