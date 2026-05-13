package com.raft.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class LogEntry {
    private final int term;
    private final int index;
    private final String command;

    @JsonCreator
    public LogEntry(
            @JsonProperty("term") int term,
            @JsonProperty("index") int index,
            @JsonProperty("command") String command) {
        this.term = term;
        this.index = index;
        this.command = command;
    }

    public int getTerm() { return term; }
    public int getIndex() { return index; }
    public String getCommand() { return command; }

    /** A no-op entry (used when a new leader is elected). */
    public boolean isNoOp() {
        return command == null || command.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LogEntry)) return false;
        LogEntry entry = (LogEntry) o;
        return term == entry.term && index == entry.index && Objects.equals(command, entry.command);
    }

    @Override
    public int hashCode() {
        return Objects.hash(term, index, command);
    }

    @Override
    public String toString() {
        return "LogEntry{term=" + term + ", index=" + index + ", command='" + command + "'}";
    }
}
