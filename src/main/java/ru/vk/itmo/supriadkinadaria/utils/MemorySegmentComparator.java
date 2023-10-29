package ru.vk.itmo.supriadkinadaria.utils;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public class MemorySegmentComparator implements Comparator<MemorySegment> {
    @Override
    public int compare(MemorySegment o1, MemorySegment o2) {
        long mismatch = o1.mismatch(o2);
        if (mismatch == -1) {
            return 0;
        } else if (o1.byteSize() == mismatch) {
            return -1;
        } else if (o2.byteSize() == mismatch) {
            return 1;
        }
        return o1.get(JAVA_BYTE, mismatch) - o2.get(JAVA_BYTE, mismatch);
    }
}
