package org.example.Client;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.example.Utility.ExperimentConfig;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.connection.ClusterConnectionMode;

/**
 * Command-line replica set setup for the MongoDB transport, run by
 * cloudlab_remote_run.sh after the mongod processes are up and before the
 * WorkloadDriver starts (MONGODB_PORT.md, Milestone 0). It replaces mongosh,
 * which the experiment host does not need to have.
 *
 *   MongoClusterAdmin init <config.yaml>   initiate the replica set from cluster.serverHosts
 *                                          (member k tagged {node: "k"}; the preferred leader
 *                                          gets priority 10), then wait for a primary
 *   MongoClusterAdmin load <config.yaml>   insert workload.keySpace documents {_id: "user<i>", v: ""}
 *                                          with w:majority so the followers start caught up
 */
public final class MongoClusterAdmin {

    private static final long TIMEOUT_MS = 120_000;

    private MongoClusterAdmin() {
    }

    public static void main(String[] args) {
        if (args.length != 2 || !(args[0].equals("init") || args[0].equals("load"))) {
            System.err.println("Usage: MongoClusterAdmin init|load <config.yaml>");
            System.exit(64);
        }
        ExperimentConfig config = ExperimentConfig.load(Path.of(args[1]));
        if (!config.mongodbTransport()) {
            System.err.println("config transport is not mongodb");
            System.exit(64);
        }
        try {
            if (args[0].equals("init")) {
                init(config);
            } else {
                load(config);
            }
        } catch (RuntimeException e) {
            System.err.println("MongoClusterAdmin " + args[0] + " failed: " + e.getMessage());
            System.exit(1);
        }
    }

    private static MongoClient direct(String host, int port) {
        ServerAddress address = new ServerAddress(host, port);
        return MongoClients.create(MongoClientSettings.builder()
                .applyToClusterSettings(b -> b.hosts(List.of(address)).mode(ClusterConnectionMode.SINGLE)
                        .serverSelectionTimeout(5, TimeUnit.SECONDS))
                .build());
    }

    private static void init(ExperimentConfig config) {
        List<String> hosts = config.cluster.serverHosts;
        int port = config.mongodb.port;
        List<Document> members = new ArrayList<>();
        for (int node = 0; node < hosts.size(); node++) {
            members.add(new Document("_id", node)
                    .append("host", hosts.get(node) + ":" + port)
                    .append("priority", node == config.cluster.preferredLeaderId ? 10.0 : 1.0)
                    .append("tags", new Document("node", Integer.toString(node))));
        }
        Document rsConfig = new Document("_id", config.mongodb.replicaSet).append("members", members);
        try (MongoClient client = direct(hosts.get(0), port)) {
            try {
                client.getDatabase("admin").runCommand(new Document("replSetInitiate", rsConfig));
                System.out.println("replSetInitiate sent to " + hosts.get(0) + ":" + port);
            } catch (MongoException e) {
                if (e.getMessage() != null && e.getMessage().contains("already initialized")) {
                    System.out.println("replica set already initialized");
                } else {
                    throw e;
                }
            }
        }
        try (MongoClusterControl control = new MongoClusterControl(config)) {
            control.waitForReplicaSet(TIMEOUT_MS);
            int leader = control.findLeaderId();
            if (config.cluster.preferredLeaderId >= 0 && leader != config.cluster.preferredLeaderId) {
                // Priority takeover happens within a few election timeouts; wait for it so the
                // experiment starts with the leader where EXPERIMENT.md puts it.
                long deadline = System.currentTimeMillis() + TIMEOUT_MS;
                while (leader != config.cluster.preferredLeaderId && System.currentTimeMillis() < deadline) {
                    sleep(500);
                    leader = control.findLeaderId();
                }
                if (leader != config.cluster.preferredLeaderId) {
                    throw new IllegalStateException("primary is node " + leader + " but cluster.preferredLeaderId is "
                            + config.cluster.preferredLeaderId + " after " + TIMEOUT_MS + " ms");
                }
            }
            System.out.println("replica set ready, primary is node " + leader);
        }
    }

    private static void load(ExperimentConfig config) {
        int leader;
        try (MongoClusterControl control = new MongoClusterControl(config)) {
            control.waitForReplicaSet(TIMEOUT_MS);
            leader = control.findLeaderId();
        }
        int keySpace = config.workload.keySpace;
        try (MongoClient client = direct(config.cluster.serverHosts.get(leader), config.mongodb.port)) {
            MongoCollection<Document> collection = client.getDatabase(config.mongodb.database)
                    .getCollection(config.mongodb.collection)
                    .withWriteConcern(WriteConcern.MAJORITY);
            collection.drop();
            final int batch = 5000;
            List<Document> documents = new ArrayList<>(batch);
            for (int i = 0; i < keySpace; i++) {
                documents.add(new Document("_id", "user" + i).append("v", ""));
                if (documents.size() == batch) {
                    collection.insertMany(documents, new InsertManyOptions().ordered(false));
                    documents.clear();
                }
            }
            if (!documents.isEmpty()) {
                collection.insertMany(documents, new InsertManyOptions().ordered(false));
            }
            long count = collection.countDocuments();
            if (count != keySpace) {
                throw new IllegalStateException("loaded " + count + " documents, expected " + keySpace);
            }
            System.out.println("loaded " + count + " documents into " + config.mongodb.database + "."
                    + config.mongodb.collection + " with w:majority");
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
