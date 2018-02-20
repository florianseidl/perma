/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.datastore;

/**
 * Parent of all exceptions related to the perma file and its data.
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public class PermaDatastoreException extends RuntimeException {
    public PermaDatastoreException(String s) {
        super(s);
    }
}
