package com.igot.cb.transactional.cassandrautils;



import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy;
import com.igot.cb.util.Constants;
import com.igot.cb.util.PropertiesCache;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author Mahesh RV
 * @author Ruksana
 * <p>
 * Manages Cassandra connections and sessions.
 */
@Service
@Slf4j
public class CassandraConnectionManagerImpl implements CassandraConnectionManager {
    private final Map<String, CqlSession> cassandraSessionMap = new ConcurrentHashMap<>(2);
    private CqlSession session;

    /**
     * Method invoked after bean creation for initialization
     */
    @PostConstruct
    private void initialize() {
        log.info("Initializing CassandraConnectionManager...");
        registerShutdownHook();
        createCassandraConnection();
        initializeSessions();
        log.info("CassandraConnectionManager initialized.");
    }

    /**
     * Retrieves a session for the specified keyspace.
     * If a session for the keyspace already exists, returns it; otherwise, creates a new session.
     *
     * @param keyspace The keyspace for which to retrieve the session.
     * @return The session object for the specified keyspace.
     */
    public CqlSession getSession(String keyspace) {
        return cassandraSessionMap.computeIfAbsent(keyspace, k ->
                CqlSession.builder()
                        .withKeyspace(keyspace)
                        .build());
    }


    /**
     * Creates a Cassandra connection based on properties
     */
    public void createCassandraConnection() {
        try {
            PropertiesCache cache = PropertiesCache.getInstance();
            String localDatacenter = cache.getProperty(Constants.LOCAL_DATACENTER);
            DriverConfigLoader loader = DriverConfigLoader.programmaticBuilder()
                    .withStringList(DefaultDriverOption.CONTACT_POINTS, Arrays.stream(StringUtils.split(cache.getProperty(Constants.CASSANDRA_CONFIG_HOST), ","))
                            .map(host -> host + ":9042").toList())
                    .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, Integer.parseInt(cache.getProperty(Constants.CORE_CONNECTIONS_PER_HOST_FOR_LOCAL)))
                    .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, Integer.parseInt(cache.getProperty(Constants.CORE_CONNECTIONS_PER_HOST_FOR_REMOTE)))
                    .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, Integer.parseInt(cache.getProperty(Constants.MAX_REQUEST_PER_CONNECTION)))
                    .withInt(DefaultDriverOption.HEARTBEAT_INTERVAL, Integer.parseInt(cache.getProperty(Constants.HEARTBEAT_INTERVAL))).build();
            CqlSessionBuilder builder = CqlSession.builder().withConfigLoader(loader).withLocalDatacenter(localDatacenter);
            session = builder.build();
            logClusterDetails(session.getMetadata());
        } catch (Exception e) {
            log.error("Error creating Cassandra connection", e);
            throw new RuntimeException("Internal Server Error", e);
        }
    }

    /**
     * Initializes sessions for predefined keyspaces
     */
    private void initializeSessions() {
        List<String> keyspacesList = Collections.singletonList(Constants.KEYSPACE_SUNBIRD);
        for (String keyspace : keyspacesList) {
            getSession(keyspace);
        }
    }

    /**
     * Registers a shutdown hook to clean-up resources
     */
    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::cleanupResources));
        log.info("Cassandra shutdown hook registered.");
    }

    /**
     * Cleans up Cassandra resources during shutdown
     */
    private void cleanupResources() {
        log.info("Starting resource cleanup for Cassandra...");
        cassandraSessionMap.values().forEach(CqlSession::close);
        if (session != null) {
            session.close();
        }
        log.info("Resource cleanup for Cassandra completed.");
    }

    private void logClusterDetails(Metadata metadata) {
        String clusterName = String.valueOf(metadata.getClusterName());
        log.info("Connected to cluster: {}", clusterName != null ? clusterName : "Unknown");

        metadata.getNodes().values().forEach(node ->
                log.info("Datacenter: {}; Host: {}; Rack: {}",
                        node.getDatacenter(),
                        node.getEndPoint().resolve(),
                        node.getRack() != null ? node.getRack() : "Unknown"));
    }

}