package org.lsm;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public class DbState {
    // Immutable views of our database metadata
    public final List<Path> sstablePaths;
    public final Map<String, List<IndexEntry>> sstableIndexes;
    public final Map<String, BloomFilter> sstableFilters;

    public DbState(List<Path> paths,
                   Map<String, List<IndexEntry>> indexes,
                   Map<String, BloomFilter> filters) {
        this.sstablePaths = List.copyOf(paths);
        this.sstableIndexes = Map.copyOf(indexes);
        this.sstableFilters = Map.copyOf(filters);
    }
}
