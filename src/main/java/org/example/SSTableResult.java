package org.example;

import java.util.List;

public record SSTableResult(List<IndexEntry> index, BloomFilter filter) {
}
