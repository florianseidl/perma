/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.serializers;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Serialize non-null-Strings. Can be constructed with a charset to use, default is UTF-8 (recomended).
 * <p>
 *     The serialized form of Strings is UTF-8 if no charset is given. Else the given charset is used.
 *     <br>
 *     If your characters are not efficently represented in UTF-8 (little use of latin charactes and punctiation),
 *     you should consider using UTF-16BE instead (be aware that endianess is relevant for UTF-16 encoding).
 *     <br>
 *     Any Charset from java.nio.charset.Charset can be used.
 * </p>
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public class StringSerializer implements KeyOrValueSerializer<String> {
    public static final Charset UTF_8 = StandardCharsets.UTF_8;
    public static final Charset UTF_16BE = StandardCharsets.UTF_16BE;

    private final Charset charset;

    public StringSerializer(Charset charset) {
        this.charset = charset;
    }

    public StringSerializer() {
        this(UTF_8);
    }

    @Override
    public byte[] toByteArray(String string) {
        return string.getBytes(charset);
    }

    @Override
    public String fromByteArray(byte[] bytes) {
        return new String(bytes, charset);
    }
}
