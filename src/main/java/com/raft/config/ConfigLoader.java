package com.raft.config;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;

public class ConfigLoader {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static RaftConfig load(String configPath) throws IOException {
        RaftConfig config = MAPPER.readValue(new File(configPath), RaftConfig.class);
        validate(config);
        return config;
    }

    private static void validate(RaftConfig config) {
        if (config.getNodeId() == null || config.getNodeId().isBlank()) {
            throw new IllegalArgumentException("nodeId must not be blank");
        }
        if (config.getPeers() == null || config.getPeers().isEmpty()) {
            throw new IllegalArgumentException("peers must not be empty");
        }
        boolean selfInPeers = config.getPeers().stream()
                .anyMatch(p -> p.getNodeId().equals(config.getNodeId()));
        if (!selfInPeers) {
            throw new IllegalArgumentException("Self nodeId '" + config.getNodeId() + "' not found in peers list");
        }
        if (config.getElectionTimeoutMinMs() <= 0 || config.getElectionTimeoutMaxMs() <= 0) {
            throw new IllegalArgumentException("election timeout must be positive");
        }
        if (config.getElectionTimeoutMinMs() >= config.getElectionTimeoutMaxMs()) {
            throw new IllegalArgumentException("electionTimeoutMinMs must be < electionTimeoutMaxMs");
        }
        if (config.getHeartbeatIntervalMs() <= 0) {
            throw new IllegalArgumentException("heartbeatIntervalMs must be positive");
        }
    }
}
