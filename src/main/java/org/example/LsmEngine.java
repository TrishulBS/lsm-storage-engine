package org.example;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentSkipListMap;

public class LsmEngine {

    private final ConcurrentSkipListMap<String, StorageEntry> memTable = new ConcurrentSkipListMap<>();
    private final FileChannel walChannel;
    private final Path walPath;

    public LsmEngine(String dataDir) throws IOException {
        Files.createDirectories(Paths.get(dataDir));
        this.walPath = Paths.get(dataDir, "wal.log");

        this.walChannel = FileChannel.open(walPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);

        recover();
    }


    public synchronized void put(String key, String value) throws IOException {
        StorageEntry entry = new StorageEntry(key, value, false);

        byte[] data = entry.serialize();
        walChannel.write(ByteBuffer.wrap(data));

        walChannel.force(false);

        memTable.put(key, entry);
    }

    public String get(String key) {
        StorageEntry entry = memTable.get(key);
        if(entry == null || entry.isTombstone()) return null;
        return entry.value();
    }

    private void recover() throws IOException {
        long fileSize = walChannel.size();
        if (fileSize == 0) return;

        ByteBuffer buffer = ByteBuffer.allocate((int) fileSize);
        walChannel.position(0);
        walChannel.read(buffer);
        buffer.flip();

        while (buffer.hasRemaining()) {
            try {
                StorageEntry entry = StorageEntry.deserialize(buffer);
                memTable.put(entry.key(), entry);
            } catch (Exception e) {
                break;
            }
        }

        walChannel.position(walChannel.size());
    }

}
