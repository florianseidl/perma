/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.serializers;

import java.util.Date;

/**
 * @author u206123 (Florian Seidl)
 * @since 3.1, 2017.
 */
public class DateSerializer implements KeyOrValueSerializer<Date> {
    @Override
    public byte[] toByteArray(Date date) {
        return LONG.toByteArray(date.getTime());
    }

    @Override
    public Date fromByteArray(byte[] bytes) {
        return new Date(LONG.fromByteArray(bytes));
    }
}
