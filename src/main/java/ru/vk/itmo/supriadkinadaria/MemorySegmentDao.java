package ru.vk.itmo.supriadkinadaria;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.supriadkinadaria.utils.MemorySegmentComparator;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;


public class MemorySegmentDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> inMemoryStorage =
            new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    private final SstableStorage sstableStorage;

    public MemorySegmentDao(Config config) {
        this.sstableStorage = new SstableStorage(config);
    }

    private Iterator<Entry<MemorySegment>> getMemoryIterator(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return inMemoryStorage.values().iterator();
        } else if (to == null) {
            return inMemoryStorage.tailMap(from).values().iterator();
        } else if (from == null) {
            return inMemoryStorage.headMap(to).values().iterator();
        }
        return inMemoryStorage.subMap(from, to).values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return null;
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        try {
            if (inMemoryStorage.containsKey(key)) {
                if (inMemoryStorage.get(key).value() == null) {
                    return null;
                }
                return inMemoryStorage.get(key);
            }
            return sstableStorage.getFromSSTables(key);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        inMemoryStorage.put(entry.key(), entry);
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException("operation not supported");
    }

    @Override
    public void close() throws IOException {
        sstableStorage.writeToFile(inMemoryStorage);
    }
}
