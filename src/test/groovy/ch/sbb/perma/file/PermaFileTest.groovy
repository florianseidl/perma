/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.file

import ch.sbb.perma.SpecificationWithTempDir

class PermaFileTest extends SpecificationWithTempDir {
    def "write"() {
        given:
        def fullFile = PermaFile.fullFile(NoCompression.NO_COMPRESSION, tempDir, 'foo', 1)

        when:
        fullFile.withOutputStream({out -> out.write('something'.bytes)})

        then:
        new File(fullFile.toString()).exists()
    }

    def "no remaining tempfile"() {
        given:
        def fullFile = PermaFile.fullFile(NoCompression.NO_COMPRESSION, tempDir, 'foo', 1)

        when:
        fullFile.withOutputStream({out -> out.write('something'.bytes)})

        then:
        tempDir.list({d, n -> n.endsWith('.perma.temp')}).size() == 0
    }

    def "write and read"() {
        given:
        def fullFile = PermaFile.fullFile(NoCompression.NO_COMPRESSION, tempDir, 'foo', 1)
        def bytes = 'something'.bytes

        when:
        fullFile.withOutputStream({out -> out.write(bytes)})
        def reread = fullFile.withInputStream({input -> input.getBytes()})

        then:
        reread == bytes
    }
}
