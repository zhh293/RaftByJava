package com.raft.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * File-based persistence for Raft durable state.
 * <p>
 * Persists two categories of data:
 * <ul>
 *   <li>Meta (currentTerm + votedFor) — written to meta.json on every change</li>
 *   <li>Log entries — appended to wal.log, one JSON line per entry</li>
 * </ul>
 * Thread-confined to the Raft core thread — no synchronization needed.
 */
public class PersistenceManager {
    private static final Logger log = LoggerFactory.getLogger(PersistenceManager.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path dataDir;
    private final Path metaFile;
    private final Path walFile;

    public PersistenceManager(String dataDirPath) throws IOException {
        this.dataDir = Paths.get(dataDirPath);
        this.metaFile = dataDir.resolve("meta.json");
        this.walFile = dataDir.resolve("wal.log");
        Files.createDirectories(dataDir);
        log.info("PersistenceManager initialized at {}", dataDir.toAbsolutePath());
    }

    // ================================================================
    // Meta persistence (currentTerm + votedFor)
    // ================================================================

    /**
     * Save currentTerm and votedFor to meta.json with fsync.
     */
    public void saveMeta(int currentTerm, String votedFor) {
        try {
            MetaData meta = new MetaData(currentTerm, votedFor);
            byte[] bytes = MAPPER.writeValueAsBytes(meta);
            try (FileOutputStream fos = new FileOutputStream(metaFile.toFile())) {
                fos.write(bytes);
                fos.getFD().sync();
            }
        } catch (IOException e) {
            log.error("Failed to save meta", e);
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Load persisted meta. Returns null if no meta file exists.
     */
    public MetaData loadMeta() {
        if (!Files.exists(metaFile)) {
            return null;
        }
        try {
            return MAPPER.readValue(metaFile.toFile(), MetaData.class);
        } catch (IOException e) {
            log.error("Failed to load meta", e);
            throw new UncheckedIOException(e);
        }
    }

    // ================================================================
    // WAL persistence (log entries)
    // ================================================================

    /**
     * Append a single log entry as a JSON line to the WAL file.
     */
    public void appendEntry(LogEntry entry) {
        try (FileOutputStream fos = new FileOutputStream(walFile.toFile(), true)) {
            byte[] bytes = MAPPER.writeValueAsBytes(entry);
            fos.write(bytes);
            fos.write('\n');
            fos.getFD().sync();
        } catch (IOException e) {
            log.error("Failed to append entry to WAL", e);
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Load all log entries from the WAL file.
     */
    public List<LogEntry> loadEntries() {
        List<LogEntry> entries = new ArrayList<>();
        if (!Files.exists(walFile)) {
            return entries;
        }
        try (BufferedReader reader = Files.newBufferedReader(walFile)) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty()) {
                    entries.add(MAPPER.readValue(line, LogEntry.class));
                }
            }
        } catch (IOException e) {
            log.error("Failed to load WAL entries", e);
            throw new UncheckedIOException(e);
        }
        log.info("Loaded {} entries from WAL", entries.size());
        return entries;
    }

    /**
     * Truncate the WAL from the given index (inclusive). Rewrites the WAL file
     * containing only entries before the given index.
     */
    public void truncateFrom(int fromIndex) {
        List<LogEntry> all = loadEntries();
        List<LogEntry> kept = new ArrayList<>();
        for (LogEntry e : all) {
            if (e.getIndex() < fromIndex) {
                kept.add(e);
            }
        }
        rewriteWal(kept);
    }

    /**
     * Rewrite the WAL with the given entries (used after truncation or snapshot).
     */
    public void rewriteWal(List<LogEntry> entries) {
        try (FileOutputStream fos = new FileOutputStream(walFile.toFile(), false)) {
            for (LogEntry entry : entries) {
                byte[] bytes = MAPPER.writeValueAsBytes(entry);
                fos.write(bytes);
                fos.write('\n');
            }
            fos.getFD().sync();
        } catch (IOException e) {
            log.error("Failed to rewrite WAL", e);
            throw new UncheckedIOException(e);
        }
    }

    public Path getDataDir() {
        return dataDir;
    }

    // ================================================================
    // MetaData inner class
    // ================================================================

    /**
     * Simple POJO for currentTerm + votedFor serialization.
     */
    public static class MetaData {
        private int currentTerm;
        private String votedFor;

        public MetaData() {} // for Jackson

        public MetaData(int currentTerm, String votedFor) {
            this.currentTerm = currentTerm;
            this.votedFor = votedFor;
        }

        public int getCurrentTerm() { return currentTerm; }
        public void setCurrentTerm(int currentTerm) { this.currentTerm = currentTerm; }
        public String getVotedFor() { return votedFor; }
        public void setVotedFor(String votedFor) { this.votedFor = votedFor; }
    }
}
