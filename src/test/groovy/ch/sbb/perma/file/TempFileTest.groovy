/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.file

import ch.sbb.perma.SpecificationWithTempDir

class TempFileTest extends SpecificationWithTempDir {
    def "create"() {
        given:
        def tempFile = new TempFile(tempDir, 'foo')

        when:
        tempFile.withOutputStream({out -> out.write('something'.bytes)})

        then:
        new File(tempFile.toString()).exists()
    }

    def "move"() {
        given:
        def tempFile = new TempFile(tempDir, 'foo')
        tempFile.withOutputStream({out -> out.write('something'.bytes)})

        when:
        tempFile.moveTo(new File(tempDir, 'moved'))

        then:
        !new File(tempFile.toString()).exists()
    }

    def "delete old temp file"() {
        given:
        def staleTempFile = new TempFile(tempDir, 'foo')
        staleTempFile.withOutputStream({out -> out.write('oldcontent'.bytes)})

        when:
        new TempFile(tempDir, 'foo').deleteStaleTempFiles()

        then:
        !new File(staleTempFile.toString()).exists()
    }

}
