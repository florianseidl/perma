/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma

import com.google.common.collect.Maps
import spock.lang.Specification
import spock.lang.Timeout
import spock.lang.Unroll

import java.util.concurrent.TimeUnit

import static ch.sbb.perma.serializers.KeyOrValueSerializer.STRING
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
        def perMa = WritablePerMa.loadOrCreate(tempDir, "bigmap", STRING, STRING)

        when:
        final int writesInThread = writes / threads
        final mapSizeInThread = mapSize
        def runnable = new Runnable() {
            @Override
            void run() {
                (1..writesInThread).forEach {
                    (1..mapSizeInThread).forEach {
                        perMa.put(randomUUID().toString(),
                                randomUUID().toString().multiply(50 * (it % 5 + 1)))
                    }
                    println("write $it, mapSize ${perMa.size()}")
                    perMa.persist();
                }
            }
        }
        runMultithreaded(perMa, threads, runnable)
        def perMaReRead = reRead()
        def lastSize = perMa.size()
        def perMaReReadSize = perMaReRead.size()
        def diff = diff(perMa, perMaReRead)

        then:
        lastSize == perMaReReadSize
        diffIsEmpty(diff)

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

    @Unroll
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    def "write remove #writes times map of #mapSize entries in #threads thread"() {
        given:
        def perMa = WritablePerMa.loadOrCreate(tempDir, "bigmap", STRING, STRING)

        when:
        final int writesInThread = writes / threads
        final mapSizeInThread = mapSize
        def runnable = new Runnable() {
            @Override
            void run() {
                (1..writesInThread).forEach {
                    def writeNr = it
                    (1..mapSizeInThread).forEach {
                        if(it*writeNr % 3 == 0) {
                            perMa.remove("entry_$it")
                        }
                        else {
                            perMa.put("entry_$it" as String, randomUUID().toString())
                        }
                    }
                    println("write $it, mapSize ${perMa.size()}")
                    perMa.persist();
                }
            }
        }
        runMultithreaded(perMa, threads, runnable)
        def perMaReRead = reRead()
        def lastSize = perMa.size()
        def perMaReReadSize = perMaReRead.size()
        def diff = diff(perMa, perMaReRead)

        then:
        lastSize == perMaReReadSize
        diffIsEmpty(diff)

        where:
        writes | mapSize | threads
        50     | 500     | 1
        50     | 500     | 10
        250    | 250     | 10
    }


    @Unroll
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    def "write #writes times and read #reads times map of #mapSize entries in #writeThreads write threads #readThreads read threads"() {
        given:
        def perMa = WritablePerMa.loadOrCreate(tempDir, "bigmap", STRING, STRING)
        def perMaReadOnly = ReadOnlyPerMa.load(tempDir, "bigmap", STRING, STRING)

        when:
        final int writesInThread = writes / writeThreads
        final mapSizeInThread = mapSize
        def writer = new Runnable() {
            @Override
            void run() {
                (1..writesInThread).forEach {
                    def writeNr = it
                    (1..mapSizeInThread).forEach {
                        perMa.put("entry_${writeNr}_$it" as String, randomUUID().toString())
                    }
                    println("write $it, mapSize ${perMa.size()}")
                    perMa.persist();
                }
            }
        }
        final readsInThread = reads / readThreads
        def reader = new Runnable() {
            @Override
            void run() {
                (1..readsInThread).forEach {
                    perMaReadOnly.refresh()
                }
            }
        }
        runMultithreaded(perMa, writeThreads, writer)
        runMultithreaded(perMa, readThreads, reader)
        def writableSize = perMa.size()
        def readOnlySize = perMaReadOnly.size()
        def diff = diff(perMa, perMaReadOnly)

        then:
        writableSize == readOnlySize
        diffIsEmpty(diff)

        where:
        writes | reads | mapSize | writeThreads | readThreads
        50     | 50    | 500     | 1            | 1
        50     | 10    | 500     | 10           | 1
        50     | 50    | 500     | 10           | 10
        250    | 10    | 250     | 25           | 5
        10     | 250   | 250     | 5            | 25
        250    | 250   | 500     | 25           | 25
    }

    @Unroll
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    def "write comapct #threads write threads #compactThreads compact threads"() {
        given:
        def perMa = WritablePerMa.loadOrCreate(tempDir, "bigmap", STRING, STRING)

        when:
        def writer = new Runnable() {
            @Override
            void run() {
                (1..250).forEach {
                    def writeNr = it
                    (1..250).forEach {
                        if(it*writeNr % 3 == 0) {
                            perMa.remove("entry_$it")
                        }
                        else {
                            perMa.put("entry_$it" as String, randomUUID().toString())
                        }
                    }
                    println("write $it, mapSize ${perMa.size()}")
                    perMa.persist();
                }
            }
        }
        def compactor = new Runnable() {
            @Override
            void run() {
                (1..250).forEach {
                    perMa.compact()
                }
            }
        }
        runMultithreaded(perMa, threads, writer)
        runMultithreaded(perMa, threads, compactor)
        def perMaReRead = reRead()
        def lastSize = perMa.size()
        def perMaReReadSize = perMaReRead.size()
        def diff = diff(perMa, perMaReRead)

        then:
        lastSize == perMaReReadSize
        diffIsEmpty(diff)

        where:
        threads | compactThreads
        1       | 1
        10      | 1
        1       | 10
        10      | 10
    }

    def runMultithreaded(perMa, threads, Runnable runable) {
        def threadList = (1..threads).collect {
            new Thread(runable)
        }
        println("Starting ${threadList.size()} threads")
        threadList.forEach { it.start() }
        println("Waiting for threads to join... ")
        threadList.forEach { it.join() }
        println("all threads finished, mapSize ${perMa.size()}")
    }

    def reRead() {
        def perMaReread = ReadOnlyPerMa.load(tempDir, "bigmap", STRING, STRING)
        println("reread, mapSize ${perMaReread.size()}")
        return perMaReread
    }

    def diff(Map perMa, Map perMaReread) {
        return Maps.difference(perMa, perMaReread)
    }

    def diffIsEmpty(diff) {
        // simple spock map comparision fails here due to excessive memory usage
        return diff.entriesDiffering().isEmpty() &&
                diff.entriesOnlyOnLeft().isEmpty() &&
                diff.entriesOnlyOnRight().isEmpty()
    }

}