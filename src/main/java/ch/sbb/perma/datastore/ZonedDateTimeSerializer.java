/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.datastore;

import com.google.common.primitives.Bytes;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;

public class ZonedDateTimeSerializer implements KeyOrValueSerializer<ZonedDateTime> {
    private final int lengthOfLocalDateTimeAsBytes;

    ZonedDateTimeSerializer() {
        lengthOfLocalDateTimeAsBytes = LOCAL_DATE_TIME.toByteArray(LocalDateTime.MAX).length;
    }

    @Override
    public byte[] toByteArray(ZonedDateTime zonedDateTime) {
        return Bytes.concat(
                LOCAL_DATE_TIME.toByteArray(zonedDateTime.toLocalDateTime()),
                STRING.toByteArray(zonedDateTime.getZone().getId())
        );
    }

    @Override
    public ZonedDateTime fromByteArray(byte[] bytes) {
        byte[] zoneBytes = Arrays.copyOfRange(bytes, lengthOfLocalDateTimeAsBytes, bytes.length);
        return ZonedDateTime.of(
                LOCAL_DATE_TIME.fromByteArray(bytes),
                ZoneId.of(STRING.fromByteArray(zoneBytes)));
    }
}
