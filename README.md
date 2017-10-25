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
* ReadOnlyPerMa: An immutable but updatable map
* ReadOnlyPerMaSet: An immutable but updatable set

In addition to that there are KeyOrValueSerializers.

#### Examples
```java
WritablePerMa perMa = WritabePerMa.loadOrCreate(tempDir, 
                                                "mymap", 
                                                KeyOrValueSerializer.INTEGER, 
                                                KeyOrValueSerializer.STRING)
perMa.put(...)
....
perma.persist()
```
```java
ReadOnlyPerMa perMa = ReadOnlyPerMa.loadStringMap(tempDir, 
                                                  "mymap")
perMa.get(...)
....
perma.update()
```

Persist is the operation persisting the data to the disk (similar ot a database commit). 
Data for a writeable disk is only loaded initially.

If Persist is performed, a snapshot of the current state of the map (or set) is taken and persisted.

For a readonly map or set, update is the only operation accessing the disk after the intial load.

## Files

PerMa stores data in many files, one full file and many delta files. Only the latest full File and its deltas are used.

File name structure: 
```
<map name>_<full file number>_<0 for full file or delta file number>.perma
```

PerMa files are immutable. Once written, they will never change (and the actual write is to a temporary file).
It is safe to copy PerMa files for backup at any time.

PerMa files use a proprietary binary format.

Each file starts with a header:

```
5 bytes  | 2 bytes | 1 byt e  | 16 bytes  | 4 bytes     | name length bytes | 8 bytes  | 4 bytes
---------|---------|-----------------------------------------------------------------------------
marker(1)| version | UUID (3) | number(4) | name length | name in UTF-8     | CRC32(5) | map size
```
(1): File marker: "PerMa" in UTF-8
(2): Full (Wert 0)or Delta (Wert 1)
(3): Die UUID marks the full file this file belongs to
(4): The sequence number of the update file (0 for a full file)
(5): CRC32 of (version, uuid, number, name length, name in utf-8) as bytes 

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
