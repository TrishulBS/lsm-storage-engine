package org.lsm;

import java.util.List;

public record SSTableResult(List<IndexEntry> index, BloomFilter filter) {
}
