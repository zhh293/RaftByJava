package com.raft.core;

import java.util.HashMap;
import java.util.Map;

/**
 * Simple in-memory key-value state machine.
 * Thread-confined to the Raft core thread.
 *
 * Supported commands:
 *   "set key=value"  — upsert a key
 *   "delete key"     — remove a key
 */
public class StateMachine {
    private final Map<String, String> store = new HashMap<>();

    /**
     * Apply a command string to the state machine.
     */
    public void apply(String command) {
        if (command == null || command.trim().isEmpty()) {
            return;
        }
        if (command.startsWith("set ")) {
            String[] parts = command.substring(4).split("=", 2);
            if (parts.length == 2) {
                store.put(parts[0], parts[1]);
            }
        } else if (command.startsWith("delete ")) {
            String key = command.substring(7);
            store.remove(key);
        }
        // Membership change commands (CONFIG:ADD/CONFIG:REMOVE) are handled
        // by RaftCore, not the KV state machine, so we silently ignore them here.
    }

    public String get(String key) {
        return store.get(key);
    }

    public boolean containsKey(String key) {
        return store.containsKey(key);
    }

    /**
     * Return a deep copy of the current state (for snapshot).
     */
    public Map<String, String> snapshot() {
        return new HashMap<>(store);
    }

    /**
     * Restore state machine from a snapshot. Replaces all existing state.
     */
    public void restoreFromSnapshot(Map<String, String> data) {
        store.clear();
        if (data != null) {
            store.putAll(data);
        }
    }

    public int size() {
        return store.size();
    }
}
