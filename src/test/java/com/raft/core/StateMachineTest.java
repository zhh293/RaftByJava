package com.raft.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StateMachineTest {
    private StateMachine sm;

    @BeforeEach
    void setUp() {
        sm = new StateMachine();
    }

    @Test
    void applySetStoresKeyValue() {
        sm.apply("set name=alice");
        assertEquals("alice", sm.get("name"));
    }

    @Test
    void applySetOverwritesExistingKey() {
        sm.apply("set x=1");
        sm.apply("set x=2");
        assertEquals("2", sm.get("x"));
    }

    @Test
    void applyDeleteRemovesKey() {
        sm.apply("set x=1");
        sm.apply("delete x");
        assertNull(sm.get("x"));
        assertEquals(0, sm.size());
    }

    @Test
    void applyBlankCommandDoesNothing() {
        sm.apply("");
        sm.apply(null);
        assertEquals(0, sm.size());
    }

    @Test
    void applyNoOpDoesNothing() {
        sm.apply("");
        assertEquals(0, sm.size());
    }

    @Test
    void snapshotReturnsCopy() {
        sm.apply("set a=1");
        var snap = sm.snapshot();
        snap.put("a", "modified");
        assertEquals("1", sm.get("a"));
    }
}
