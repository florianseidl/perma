/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.serializers;

import java.util.Date;

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
