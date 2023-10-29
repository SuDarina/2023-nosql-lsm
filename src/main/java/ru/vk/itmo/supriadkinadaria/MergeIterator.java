package ru.vk.itmo.supriadkinadaria;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.*;

public class MergeIterator implements Iterator<Entry<MemorySegment>> {

    private final PriorityQueue<IndexPeekIterator> iterators;
    private final Comparator<Entry<MemorySegment>> comparator;

    private MergeIterator(PriorityQueue<IndexPeekIterator> iterators, Comparator<Entry<MemorySegment>> comparator) {
        this.iterators = iterators;
        this.comparator = comparator;
    }

    // iterators are strictly ordered by comparator (previous element always < next element)
    public static Iterator<Entry<MemorySegment>> of(List<IndexPeekIterator> iterators, Comparator<Entry<MemorySegment>> comparator) {
        switch (iterators.size()) {
            case 0:
                return Collections.emptyIterator();
            case 1:
                return iterators.get(0);
        }
        PriorityQueue<IndexPeekIterator> queue = new PriorityQueue<>(iterators.size(), (o1, o2) -> {
            int result = comparator.compare(o1.peek(), o2.peek());
            if (result != 0) {
                return result;
            }
            return Integer.compare(o1.index(), o2.index());
        });

        for (IndexPeekIterator iterator : iterators) {
            if (iterator.hasNext()) {
                queue.add(iterator);
            }
        }

        return new MergeIterator(queue, comparator);
    }


    @Override
    public boolean hasNext() {
        return !iterators.isEmpty();
    }

    @Override
    public Entry<MemorySegment> next() {
        IndexPeekIterator iterator = iterators.remove();
        Entry<MemorySegment> next = iterator.next();

        while (!iterators.isEmpty()) {
            IndexPeekIterator candidate = iterators.peek();
            if (comparator.compare(next, candidate.peek()) != 0) {
                break;
            }

            iterators.remove();
            candidate.next();
            if (candidate.hasNext()) {
                iterators.add(candidate);
            }
        }

        if (iterator.hasNext()) {
            iterators.add(iterator);
        }

        return next;
    }
}