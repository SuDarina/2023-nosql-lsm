package ru.vk.itmo.supriadkinadaria;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;
import ru.vk.itmo.supriadkinadaria.utils.MemorySegmentComparator;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static ru.vk.itmo.supriadkinadaria.Sstable.*;

public class SstableStorage {

    public static AtomicInteger index = new AtomicInteger(0);
    private List<Sstable> sstables = new ArrayList<>();
    private List<MemorySegment> fileMSs = new ArrayList<>();
    private List<MemorySegment> offsetMSs = new ArrayList<>();
    private Sstable currentSstableToWriteIn;
    private MemorySegmentComparator memorySegmentComparator;

    public SstableStorage(Config config) {
        memorySegmentComparator = new MemorySegmentComparator();
        int i = 0;
        while (true) {
                Path nextFile = config.basePath().resolve(FILENAME + i + FILE_EXT);
                Path nextOffset = config.basePath().resolve(OFFSET_NAME + i + OFFSET_EXT);
                if (Files.exists(nextFile)) {
                    sstables.add(new Sstable(i,  config.basePath()));
                    try (FileChannel channel = FileChannel.open(nextFile, StandardOpenOption.TRUNCATE_EXISTING,
                            StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
                         FileChannel offsetChannel = FileChannel.open(nextOffset, StandardOpenOption.TRUNCATE_EXISTING,
                                 StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
                        fileMSs.add(channel.map(READ_ONLY, 0, Files.size(nextFile), Arena.ofShared()));
                        offsetMSs.add(offsetChannel.map(READ_ONLY, 0, Files.size(nextOffset), Arena.ofShared()));
                        i++;
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    index.set(i);
                    currentSstableToWriteIn = new Sstable(index.get(), config.basePath());
                    break;
                }
            }

        Collections.reverse(sstables);
        Collections.reverse(fileMSs);
        Collections.reverse(offsetMSs);
    }

    public BaseEntry<MemorySegment> getFromSSTables(MemorySegment key) throws IOException {
        for (int i = 0; i < fileMSs.size(); i++) {
            BaseEntry<MemorySegment> entry = getFromSSTable(key, fileMSs.get(i), offsetMSs.get(i));
            if (entry != null) {
                return entry;
            }
        }
        return null;
    }

    private synchronized BaseEntry<MemorySegment> getFromSSTable(MemorySegment key, MemorySegment storageSegment, MemorySegment offsetSegment) {
        long start = 0;
        long end = offsetSegment.byteSize() / Long.BYTES;
        long offset;
        while (start <= end) {
            long mid = (start + end) >>> 1;
            offset = offsetSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, mid * Long.BYTES);
            long keySize = storageSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            MemorySegment keyRestored = storageSegment.asSlice(offset, keySize);
            if (memorySegmentComparator.compare(keyRestored, key) < 0) {
                start = mid + 1;
            } else if (memorySegmentComparator.compare(keyRestored, key) > 0) {
                end = mid - 1;
            } else {
                offset += keySize;
                long valueSize = storageSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;
                if (valueSize == 0 && offset < storageSegment.byteSize() && storageSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset) == 0) {
                    return null;
                }
                return new BaseEntry<>(key, storageSegment.asSlice(offset, valueSize));
            }
        }
        return null;
    }

    public synchronized void writeToFile(Map<MemorySegment, Entry<MemorySegment>> storage) throws IOException {
        Path filePath = currentSstableToWriteIn.getFilePath();
        Path offsetPath = currentSstableToWriteIn.getOffsetPath();
        try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
             FileChannel offsetsChannel = FileChannel.open(offsetPath, StandardOpenOption.TRUNCATE_EXISTING,
                     StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            long size = Long.BYTES * 2L * storage.size();
            for (Entry<MemorySegment> entry : storage.values()) {
                size += entry.key().byteSize() + (entry.value() == null ? Long.BYTES : entry.value().byteSize());
            }
            MemorySegment storageSegment = channel.map(READ_WRITE, 0, size, Arena.ofShared());
            MemorySegment offsetSegment = offsetsChannel.map(READ_WRITE, 0, (long) storage.values().size() * Long.BYTES, Arena.ofShared());
            long offset = 0;
            long offsets = 0;
            for (Entry<MemorySegment> entry : storage.values()) {
                storageSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.key().byteSize());
                offsetSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offsets, offset);
                offsets += Long.BYTES;
                offset += Long.BYTES;
                storageSegment.asSlice(offset, entry.key().byteSize()).copyFrom(entry.key());
                offset += entry.key().byteSize();
                if (entry.value() != null) {
                    storageSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.value().byteSize());
                    offset += Long.BYTES;
                    storageSegment.asSlice(offset, entry.value().byteSize()).copyFrom(entry.value());
                    offset += entry.value().byteSize();
                } else {
                    storageSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, 0);
                    offset += Long.BYTES;
                    storageSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, 0);
                    offset += Long.BYTES;
                }
            }
        }
    }

    private Iterator<Entry<MemorySegment>> iterate(MemorySegment sstableSegmrnt, MemorySegment keyFrom, MemorySegment keyTo) {
        long keyFromPos = 0;
//                greaterOrEqualEntryIndex(sstable, keyFrom);
        long keyToPos = 0;
//                greaterOrEqualEntryIndex(sstable, keyTo);

        return new Iterator<>() {
            long pos = keyFromPos;

            @Override
            public boolean hasNext() {
                return pos < keyToPos;
            }

            @Override
            public Entry<MemorySegment> next() {
                Entry<MemorySegment> entry = null;
//                        entryAt(sstable, pos);
                pos++;
                return entry;
            }
        };
    }
}
