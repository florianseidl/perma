/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma

import spock.lang.Specification

class SpecificationWithTempDir extends Specification {
    protected File tempDir

    def setup() {
        tempDir = File.createTempDir()
    }

    def cleanup() {
        tempDir.deleteDir()
    }
}
