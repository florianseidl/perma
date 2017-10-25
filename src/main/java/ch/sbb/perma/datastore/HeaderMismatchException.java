/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.datastore;

/**
 * Header does not match the expectation.
 * <ul>
 *     <li>Sequence of delta files is not correct</li>
 *     <li>Delta file does not belong to the given full file</li>
 * </ul>
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public class HeaderMismatchException extends PerMaDatastoreException {
    public HeaderMismatchException(String s) {
        super(s);
    }
}
