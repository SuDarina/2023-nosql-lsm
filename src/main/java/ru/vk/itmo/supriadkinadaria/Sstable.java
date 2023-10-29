package ru.vk.itmo.supriadkinadaria;

import ru.vk.itmo.Config;

import java.nio.file.Path;

public class Sstable {
    public static final String FILENAME = "sstable";
    public static final String OFFSET_NAME = "offset";
    public static final String FILE_EXT = ".db";
    public static final String OFFSET_EXT = ".meta";
    private int index;
    private Path basePath;
    private Path filePath;
    private Path offsetPath;

    public Sstable(int index, Path path) {
        this.index = index;
        this.basePath = path;
        this.filePath = basePath.resolve(getFileName());
        this.offsetPath = basePath.resolve(getOffsetName());
    }

    public String getFileName() {
        return FILENAME + index + FILE_EXT;
    }

    public String getOffsetName() {
        return OFFSET_NAME + index + OFFSET_EXT;
    }

    public Path getFilePath() {
       return filePath;
    }

    public Path getOffsetPath() {
        return offsetPath;
    }

    public int getIndex() {
        return this.index;
    }
}
