# perma

A library for simple persistent maps and sets.

* Does allways keep the Map in Memory. Everything is loaded once at the start.
* Does persist when told so by calculating the difference and writing diff files.

Advantage: very simple, fast read after initial access
Disadvantage: slow write, slow startup

## Usage

The public API is
* WritablePerma: A mutable persistent map
* WritablePermaSet: A mutable persistent set
* ReadOnlyPerma: An immutable but updatable map
* ReadOnlyPermaSet: An immutable but updatable set

In addition to that there are KeyOrValueSerializers.

#### Examples
```java
WritablePerma perma = WritabePerma.loadOrCreate(tempDir, 
                                                "mymap", 
                                                KeyOrValueSerializer.INTEGER, 
                                                KeyOrValueSerializer.STRING)
perma.put(...)
....
perma.persist()
```
```java
ReadOnlyPerma perma = ReadOnlyPerma.loadStringMap(tempDir, 
                                                  "mymap")
perma.get(...)
....
perma.update()
```

Persist is the operation persisting the data to the disk (similar ot a database commit). 
Data for a writable disk is only loaded initially.

If Persist is performed, a snapshot of the current state of the map (or set) is taken and persisted.

For a readonly map or set, update is the only operation accessing the disk after the intial load.

## Serializers

Perma comes with a selection of serializers and allows for simple addition of custom serilaizers.
Serializers aim for a relatively efficient serialization of values.

### Plain Serializers

Class                                     | Serializer                                                     | Remark
----------------------------------------- | -------------------------------------------------------------- | ---------------------------------------- 
String                                    | KeyOrValueSerializer.STRING                                    | Encoded to UTF-8
Optional<String>                          | KeyOrValueSerializer.OPTIONAL_STRING                           | Nullable String
Integer, Long, Short, Byte, Double, Float | KeyOrValueSerializer.INTEGER, LONG, SHORT, BYTE, DOUBLE, FLOAT | Ecoded using Guava
BigDecimal, BigInteger                    | KeyOrValueSerializer.BIG_DECIMAL, BIG_INTEGER                  |
Character                                 | KeyOrValueSerializer.CHARACTER                                 |
Object                                    | KeyOrValueSerializer.JAVA_OBJECT                               | Uses Java Serialization. Not recomended.
LocalDateTime, LocalDate, LocalTime       | KeyOrValueSerializer.LOCAL_DATE_TIME, LOCAL_DATE, LOCAL_TIME   | java.time (Java 8 date)
ZonedDateTime, OffsetDateTime             | KeyOrValueSerializer.ZONED_DATE_TIME, OFFSET_DATE_TIME         | java.time with Offset/Zone (Java 8)
DateSerializer                            | KeyOrValueSerializer.DATE                                      | java.util (Java legacy date)

### String Serializer

KeyOrValueSerializer.STRING or new StringSerializer() encodes to UTF-8. This is the recomended format for most chases.

If that is not desired (as it is inefficient for instance for chinese characters), StringSerializer can be constructed with an encoding as string parameter. 
The recomended alternative is UTF-16 Big Endian and available as constant in String Serializer: new StringSerializer(StringSerializer.UTF-16BE). 
Alternatively, any supported Charset can be used.

### Collection Serializers

There are immutable (Guava) collection serializers. It is recomended to use immutable collections.

All Collection Serializers require an item Serializer (to serialize the individual items of the collection)

* ImmutableListSerialzer(itemSerializer)
* ImmutableSetSerialzer(itemSerializer)

If you require other collection serializers, extend ImmutableCollectionSerializer or MutableCollectionSerializer accordlingly.

### Tuple Serializers

Perma supports the Javatuples library for Tuple Serializers. Warning: Javatuples are mutable.

Tuple Serializers require one inner serializer per element. 

* UnitSerializer(innerSeriaizer) for Unit (can be used to wrap nullable values)
* PairSerializer(innerSerializer1, innerSerializer2)
* TripletSerializer(innerSerializer1, innerSerializer2, innerSerialzer3)

Only Unit, Pair and Triplet are implemented, if you require other Tuples extend TupleSerializer accordlingly.

### Enum Serializer

There is an enum Serializer. It requires the enum class as parameter.

* EnumSerializer(enumClass)

### Array Serializers

There are two array serializers:

* ObjectArraySerializer(elementClass, elementSerializer)
* StringArraySerializer an ObjectArraySerializer provided for convenience

For the Object array seriaizer, you have to provide the element class and a serializer for individual elements.

### Own Serilaizers

Implement the Interface KeyOrValueSerializer.

## Configuration Options

There are two configurable options:
* compress: Use GZip Compression. Default: false (no compression)
* compactThresholdPercent: The threshold, at which percentage of deleted or changed records a compact instead 
of a delta persit is automatically performed. Default: 34 (34% or 0.34 of the current map size)

Configuration is performed programatically by using the class ch.sbb.perma.Options (using the Builder provided).

## Spring Boot Integration

Perma can easily be integrated into Spring Boot as shown in the following example:
```
@Configuration
public class PermaConfig {
    @Bean
    @Profile({"dev", "e2e", "prod"})
    public File permaDir(@Value("${tourienv.consistency.perma.directory}") String dirPath) {
        File dir = new File(dirPath);
        dir.mkdirs();
        return dir;
    }

    @Bean("permaDir")
    @Profile({"embedded", "local"})
    public File permaDirEmbedded() {
        return Files.createTempDir();
    }

    @Bean
    public WritablePerma<String, StoreType> storeTypePerma(File permaDir) throws IOException {
        return WritablePerma.loadOrCreate(permaDir, "storetype",
                                            KeyOrValueSerializer.STRING,
                                            new EnumSerializer<>(StoreType.class));
    }
}
```
## Files

Perma stores data in many files, one full file and many delta files. Only the latest full File and its deltas are used.

File permaName structure: 
```
<map permaName>_<full file number>_<0 for full file or delta file number>.perma
```

Perma files are immutable. Once written, they will never change (and the actual write is to a temporary file).
It is safe to copy Perma files for backup at any time.

Perma files use a proprietary binary format.

Each file starts with a header:

```
5 bytes  | 2 bytes | 1 byt e  | 16 bytes  | 4 bytes     | permaName length bytes | 8 bytes  | 4 bytes
---------|---------|-----------------------------------------------------------------------------
marker(1)| version | UUID (3) | number(4) | permaName length | permaName in UTF-8     | CRC32(5) | map size
```
(1): File marker: "Perma" in UTF-8
(2): Full (Wert 0)or Delta (Wert 1)
(3): Die UUID marks the full file this file belongs to
(4): The sequence number of the update file (0 for a full file)
(5): CRC32 of (version, uuid, number, permaName length, permaName in utf-8) as bytes 

And then many records:

```
1 byte   | 1 byte  | 4 bytes    | key length bytes | 4 bytes      | value lenght bytes | 8 bytes
---------|---------|-----------------------------------------------------------------------------
marker(a)| type(b) | key length | serialzed key    | value length | serialzed value    | CRC32(c)
```
(a): Record marker (Wert F5 Hex)
(b): Record type new/updated (Wert 0) or deleted (Wert 1)
(c): CRC32 of (type, key length, key, value length, value) as bytes 

A length of -1 is translated to a null value (relevant only for value length)
