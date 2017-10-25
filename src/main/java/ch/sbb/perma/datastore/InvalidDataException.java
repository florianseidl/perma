/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.datastore;

/**
 * File format of read PerMa file is not valid.
 * <ul>
 *     <li>Invalid file or record marker</li>
 *     <li>CRC32 Checksum error in file header</li>
 *     <li>CRC32 Checksum error in record header</li>
 * </ul>
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public class InvalidDataException extends PerMaDatastoreException {
    public InvalidDataException(String s) {
        super(s);
    }
}
