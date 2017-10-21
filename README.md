# perma

A library for simple persistent maps and sets.

* Does allways keep the Map in Memory. Everything is loaded once at the start.
* Does persist when told so by calculating the difference and writing diff files.

Advantage: very simple, fast read after initial access
Disadvantage: slow write, slow startup

## Usage

The public API is
* WritablePerMa: A mutable persistent map
* WritablePerMaSet: A mutable persistent set

In addition to that there are KeyOrValueSerializers.

Examples
```java
WritablePerMa perMa = WritabePerMa.loadOrCreate(tempDir, 
                                                "mymap", 
                                                KeyOrValueSerializer.INTEGER, 
                                                KeyOrValueSerializer.STRING)
perMa.map().put(...)
....
perma.persist()
```

Persist is the operation persisting the data to the disk (similar ot a database commit).

If Persist is performed, a snapshot of the current state of the map (or set) is taken and persisted.

## Files

PerMa stores data in many files, one full file and many delta files. Only the latest full File and its deltas are used.

File name structure: 
```
<map name>_<full file number>_<0 for full file or delta file number>.perma
```

PerMa files are immutable. Once written, they will never change (and the actual write is to a temporary file).
It is safe to copy PerMa Filees for backup at any time.

PerMa Files use a proprietary binary format.

Each File starts with a header:

5 bytes  | 1 byte  | 16 bytes | 4 bytes   | 4 bytes     | name lenght bytes | 8 bytes
---------|---------|-----------------------------------------------------------------
Marker(1)| Type(2) | UUID (3) | number(4) | name length | name in UTF-8     | CRC32

(1): File Marker: "PerMa" in UTF-8
(2): Full (Wert 0)or Delta (Wert 1)
(3): Die UUID marks the full file this file belongs to
(4): The sequence number of the update file (0 for a full file)

And then many records:

1 byte   | 1 byte  | 4 bytes    | key lenght bytes | 4 bytes      | value lenght bytes | 8 bytes
---------|---------|----------------------------------------------------------------------------
Marker(a)| Type(b) | key length | serialzed key    | value length | serialzed value    | CRC32

(a): Record Marker (Wert F5 Hex)
(b): Record Type new/updated (Wert 0) or deleted (Wert 1)

A length of -1 is translated to a null value (relevant only for value length)