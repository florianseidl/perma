/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma

import com.google.common.collect.MapDifference
import com.google.common.collect.Maps
import spock.lang.Specification
import spock.lang.Timeout
import spock.lang.Unroll

import java.util.concurrent.TimeUnit

import static ch.sbb.perma.datastore.KeyOrValueSerializer.STRING
import static java.util.UUID.randomUUID
import static org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY

/**
 * Test the performance and memory usage with real load.
 * <p>
 * Warning: will require 10GB ram and disk space. Disabled by default in POM.
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0 , 2017.
 */
class PerMaIT extends Specification {

    File tempDir

    def setupSpec() {
        System.setProperty(DEFAULT_LOG_LEVEL_KEY, "TRACE");
    }

    def setup() {
        tempDir = createTempDir()
        println("Using temp dir: $tempDir")
    }

    def createTempDir() {
        String configuredTempDir = System.getProperty('tempDir')
        if (configuredTempDir == null) {
            return File.createTempDir()

        }
        File dir = new File(configuredTempDir, "perma_${randomUUID()}")
        dir.mkdirs()
        return dir;
    }

    def cleanup() {
        tempDir.deleteDir()
    }

    @Unroll
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    def "write #writes times map of #mapSize random entries in #threads thread"() {
        given:
        def perMa = WritabePerMa.loadOrCreate(tempDir, "bigmap", STRING, STRING)

        when:
        final int writesInThread = writes / threads
        final mapSizeInThread = mapSize
        def runable = new Runnable() {
            @Override
            void run() {
                (1..writesInThread).forEach {
                    (1..mapSizeInThread).forEach {
                        perMa.map().put(randomUUID().toString(),
                                randomUUID().toString().multiply(50 * (it % 5 + 1)))
                    }
                    println("write $it, mapSize ${perMa.map().size()}")
                    perMa.persist();
                }
            }
        }
        def threadList = (1..threads).collect {
            new Thread(runable)
        }
        println("Starting write in ${threadList.size()} threads")
        threadList.forEach { it.start() }
        println("Waiting for threads to join... ")
        threadList.forEach { it.join() }
        println("all written, mapSize ${perMa.map().size()}")
        def perMaSize = perMa.map().size()
        def perMaReread = ReadOnlyPerMa.load(tempDir, "bigmap", STRING, STRING)
        def reReadSize = perMaReread.map().size()
        println("reread, mapSize ${perMaReread.map().size()}")
        MapDifference diff = Maps.difference(perMa.map(), perMaReread.map())

        then:
        reReadSize == perMaSize
        // simple spock map comparision fails here due to excessive memory usage
        diff.entriesDiffering().isEmpty()
        diff.entriesOnlyOnLeft().isEmpty()
        diff.entriesOnlyOnRight().isEmpty()

        where:
        writes | mapSize | threads
        500    | 500     | 1
        50     | 5000    | 1
        5      | 50000   | 1
        30     | 50      | 3
        50     | 5000    | 3
        50     | 5000    | 50
        5      | 500     | 50
    }
}