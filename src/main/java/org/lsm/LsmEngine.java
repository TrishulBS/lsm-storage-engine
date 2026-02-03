package org.lsm;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class LsmEngine {

    private final AtomicReference<DbState> state = new AtomicReference<>(
            new DbState(new ArrayList<>(), new HashMap<>(), new HashMap<>())
    );

    private static final int MAX_MEMTABLE_SIZE = 1000;
    private final AtomicInteger sstableCount = new AtomicInteger(0);

    private final List<Path> sstablePaths = new ArrayList<>();
    private volatile ConcurrentSkipListMap<String, StorageEntry> memTable = new ConcurrentSkipListMap<>();
    private volatile ConcurrentSkipListMap<String, StorageEntry> immutableMemTable = null;

    private final ReentrantLock writeLock = new ReentrantLock();
    private final FileChannel walChannel;
    private final Path walPath;
    private final java.util.Map<String, List<IndexEntry>> sstableIndexes = new java.util.HashMap<>();
    private static final int INDEX_INTERVAL = 100;
    private final java.util.Map<String, BloomFilter> sstableFilters = new java.util.HashMap<>();

    public LsmEngine(String dataDir) throws IOException {
        Path dirPath = Paths.get(dataDir);
        Files.createDirectories(dirPath);
        this.walPath = dirPath.resolve("wal.log");

        this.walChannel = FileChannel.open(walPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);

        // Scan for existing SSTables and add to our list
        try (var stream = Files.list(dirPath)) {
            stream.filter(path -> path.toString().endsWith(".sst"))
                    .sorted((p1, p2) -> {
                        // Extract number to sort 10.sst after 2.sst
                        int n1 = extractIndex(p1);
                        int n2 = extractIndex(p2);
                        return Integer.compare(n2, n1); // Descending (newest first)
                    })
                    .forEach(path -> {
                        sstablePaths.add(path);
                        try {
                            rebuildIndexForFile(path);
                        } catch (IOException e) {
                            System.err.println("Failed to index: " + path);
                        }
                    });
        }

        // Update the counter so the next flush starts at the correct number
        if (!sstablePaths.isEmpty()) {
            int maxIndex = extractIndex(sstablePaths.get(0));
            this.sstableCount.set(maxIndex + 1);
        }

        recover();
    }

    private int extractIndex(Path path) {
        String name = path.getFileName().toString();
        return Integer.parseInt(name.substring(0, name.lastIndexOf('.')));
    }

    public void put(String key, String value) throws IOException {
        writeLock.lock();
        try {
            StorageEntry entry = new StorageEntry(key, value, false);
            byte[] data = entry.serialize();

            // 2. Persist to WAL (Still on the user thread for durability)
            walChannel.write(ByteBuffer.wrap(data));
            walChannel.force(false);

            // 3. Update the ACTIVE MemTable
            memTable.put(key, entry);

            // 4. Check if we need to rotate tables
            if (memTable.size() >= MAX_MEMTABLE_SIZE) {

                // BACKPRESSURE: If the previous background flush is still running,
                // we must wait here so we don't overwhelm the system memory.
                while (immutableMemTable != null) {
                    Thread.onSpinWait(); // High-performance wait for modern CPUs
                }

                // ATOMIC SWAP: Move active to immutable
                immutableMemTable = memTable;
                memTable = new ConcurrentSkipListMap<>();

                // Reset WAL for the new active MemTable
                // In a pro system, you'd switch to a NEW WAL file here.
                walChannel.truncate(0);

                // 5. TRIGGER BACKGROUND FLUSH
                // This starts a new thread to do the slow disk work.
                // The user thread exits 'put' immediately after this!
                CompletableFuture.runAsync(() -> {
                    try {
                        backgroundFlush();
                    } catch (IOException e) {
                        System.err.println("Critical Error in Background Flush: " + e.getMessage());
                    }
                });
            }
        } finally {
            writeLock.unlock();
        }
    }

    public String get(String key) {
        // 1. Check the active MemTable (ConcurrentSkipListMap handles the concurrency)
        StorageEntry memEntry = memTable.get(key);
        if (memEntry != null) return memEntry.isTombstone() ? null : memEntry.value();

        ConcurrentSkipListMap<String, StorageEntry> frozen = immutableMemTable;
        if (frozen != null) {
            StorageEntry entry = frozen.get(key);
            if (entry != null) return entry.isTombstone() ? null : entry.value();
        }

        // 2. CAPTURE THE SNAPSHOT
        // Even if compaction deletes files 1ms after this line,
        // this 'view' of the paths and indexes remains valid for this thread.
        DbState snapshot = state.get();

        for (Path path : snapshot.sstablePaths) {
            String fileName = path.getFileName().toString();
            BloomFilter filter = snapshot.sstableFilters.get(fileName);

            if (filter != null && !filter.mightContain(key)) continue;

            try {
                // We use the index FROM THE SNAPSHOT
                SearchResult result = searchWithIndex(path, key, snapshot.sstableIndexes.get(fileName));
                if (result.found()) return result.value();
            } catch (IOException e) { e.printStackTrace(); }
        }
        return null;
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

    private SearchResult searchWithIndex(Path path, String key, List<IndexEntry> index) throws IOException {
        if (index == null || index.isEmpty()) return null;

        // 1. Binary Search the Index in RAM to find the correct block
        int blockIdx = -1;
        for (int i = 0; i < index.size(); i++) {
            if (key.compareTo(index.get(i).firstKey()) >= 0) {
                blockIdx = i;
            } else {
                break;
            }
        }

        if (blockIdx == -1) return new SearchResult(false, null); // Key is smaller than the first key in file

        // 2. Seek and Scan only that block
        long startOffset = index.get(blockIdx).offset();
        long endOffset = (blockIdx + 1 < index.size())
                ? index.get(blockIdx + 1).offset()
                : Files.size(path);

        try (FileChannel fc = FileChannel.open(path, StandardOpenOption.READ)) {
            fc.position(startOffset);
            ByteBuffer buffer = ByteBuffer.allocate((int) (endOffset - startOffset));
            fc.read(buffer);
            buffer.flip();

            while (buffer.hasRemaining()) {
                StorageEntry entry = StorageEntry.deserialize(buffer);
                if (entry.key().equals(key)) {
                    // WE FOUND THE KEY!
                    // Even if it's a tombstone, we tell the engine to STOP searching older files.
                    return new SearchResult(true, entry.isTombstone() ? null : entry.value());
                }
                if (entry.key().compareTo(key) > 0) break;
            }
        }
        return new SearchResult(false, null); // Key definitely not in this file
    }

    private void rebuildIndexForFile(Path path) throws IOException {
        // 10 bits per entry is the standard for 1% false positive rate
        BloomFilter filter = new BloomFilter(MAX_MEMTABLE_SIZE);
        List<IndexEntry> currentIndex = new ArrayList<>();
        String fileName = path.getFileName().toString();

        try (FileChannel fc = FileChannel.open(path, StandardOpenOption.READ)) {
            ByteBuffer buffer = ByteBuffer.allocate((int) fc.size());
            fc.read(buffer);
            buffer.flip();

            int count = 0;
            while (buffer.hasRemaining()) {
                long currentOffset = buffer.position();

                StorageEntry entry = StorageEntry.deserialize(buffer);

                filter.add(entry.key());

                if (count % INDEX_INTERVAL == 0) {
                    currentIndex.add(new IndexEntry(entry.key(), currentOffset));
                }
                count++;
            }
        }
        sstableFilters.put(fileName, filter);
        sstableIndexes.put(fileName, currentIndex);
    }

    public void compact() throws IOException {
        // 1. Capture the current snapshot of files to merge
        DbState snapshot = state.get();
        if (snapshot.sstablePaths.size() < 2) return;

        System.out.println("Compaction started in background...");

        // 2. Perform the merge into a Map (The slow part)
        TreeMap<String, StorageEntry> mergedData = new TreeMap<>();
        // Iterate from oldest to newest so newest versions overwrite
        List<Path> filesToCompact = snapshot.sstablePaths;
        for (int i = filesToCompact.size() - 1; i >= 0; i--) {
            readFullFileIntoMap(filesToCompact.get(i), mergedData);
        }

        // 3. Write the new compacted file
        String compactName = "compacted_" + System.currentTimeMillis() + ".sst";
        File compactFile = walPath.getParent().resolve(compactName).toFile();
        SSTableResult result = writeToSSTableInternal(compactFile, mergedData.values());

        // 4. ATOMIC SWAP
        // We create a brand new state that REPLACES the old files with the NEW one
        while (true) {
            DbState current = state.get();

            // Build the new path list: New file + any files that arrived DURING compaction
            List<Path> newPaths = new ArrayList<>();
            newPaths.add(compactFile.toPath());

            for (Path p : current.sstablePaths) {
                if (!filesToCompact.contains(p)) {
                    newPaths.add(p); // Keep files that weren't part of this compaction
                }
            }

            // Build new Metadata maps
            Map<String, List<IndexEntry>> newIndexes = new HashMap<>(current.sstableIndexes);
            Map<String, BloomFilter> newFilters = new HashMap<>(current.sstableFilters);

            // Add new metadata, remove old
            newIndexes.put(compactName, result.index());
            newFilters.put(compactName, result.filter());
            for (Path old : filesToCompact) {
                newIndexes.remove(old.getFileName().toString());
                newFilters.remove(old.getFileName().toString());
            }

            DbState newState = new DbState(newPaths, newIndexes, newFilters);

            // Use Compare-And-Set to ensure no other thread changed the state while we were merging
            if (state.compareAndSet(current, newState)) {
                break;
            }
        }

        // 5. Cleanup the disk
        for (Path old : filesToCompact) {
            Files.deleteIfExists(old);
        }
        System.out.println("Compaction finished. Merged into: " + compactName);
    }



    private void readFullFileIntoMap(Path path, Map<String, StorageEntry> map) throws IOException {
        try (FileChannel fc = FileChannel.open(path, StandardOpenOption.READ)) {
            // For compaction, we read the whole file to perform the merge
            ByteBuffer buffer = ByteBuffer.allocate((int) fc.size());
            fc.read(buffer);
            buffer.flip();

            while (buffer.hasRemaining()) {
                StorageEntry entry = StorageEntry.deserialize(buffer);
                // Since we iterate from oldest file to newest,
                // the newest version of the key will naturally overwrite the old one in the map.
                map.put(entry.key(), entry);
            }
        }
    }

    public synchronized void delete(String key) throws IOException {

        StorageEntry entry = new StorageEntry(key, "", true); // true = isTombstone
        byte[] data = entry.serialize();

        // 1. Write to WAL
        walChannel.write(ByteBuffer.wrap(data));
        walChannel.force(false);

        // 2. Add to MemTable
        memTable.put(key, entry);

        if (memTable.size() >= MAX_MEMTABLE_SIZE) {
            backgroundFlush();
        }
    }

    private void backgroundFlush() throws IOException {
        ConcurrentSkipListMap<String, StorageEntry> dataToFlush = immutableMemTable;
        if (dataToFlush == null) return;

        // 1. Prepare the new file
        int id = sstableCount.getAndIncrement();
        String fileName = id + ".sst";
        File sstableFile = walPath.getParent().resolve(fileName).toFile();

        // 2. Write to disk (Slow I/O happens here, but user thread is already free!)
        SSTableResult result = writeToSSTableInternal(sstableFile, dataToFlush.values());

        // 3. Prepare the NEW State
        // We take a snapshot of the current state and create a modified version
        DbState currentState = state.get();

        List<Path> newPaths = new ArrayList<>(currentState.sstablePaths);
        newPaths.add(0, sstableFile.toPath()); // Add new file to the front (newest)

        Map<String, List<IndexEntry>> newIndexes = new HashMap<>(currentState.sstableIndexes);
        newIndexes.put(fileName, result.index());

        Map<String, BloomFilter> newFilters = new HashMap<>(currentState.sstableFilters);
        newFilters.put(fileName, result.filter());

        // 4. THE ATOMIC SWAP
        // This is a single pointer update. Instant and thread-safe.
        state.set(new DbState(newPaths, newIndexes, newFilters));

        // 5. Release the waiting room
        // Setting this to null allows the 'put' method to start the next flush
        immutableMemTable = null;
    }

    private SSTableResult writeToSSTableInternal(File file, Collection<StorageEntry> entries) throws IOException {
        List<IndexEntry> currentIndex = new ArrayList<>();
        BloomFilter filter = new BloomFilter(entries.size());

        try (FileChannel out = new FileOutputStream(file).getChannel()) {
            long currentOffset = 0;
            int count = 0;

            for (StorageEntry entry : entries) {
                if (count % INDEX_INTERVAL == 0) {
                    currentIndex.add(new IndexEntry(entry.key(), currentOffset));
                }
                filter.add(entry.key());

                byte[] data = entry.serialize();
                ByteBuffer buf = ByteBuffer.wrap(data);
                currentOffset += buf.remaining();
                out.write(buf);
                count++;
            }
            out.force(true);
        }
        return new SSTableResult(currentIndex, filter);
    }
}