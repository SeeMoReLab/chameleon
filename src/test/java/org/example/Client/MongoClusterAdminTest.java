package org.example.Client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;

import org.bson.Document;
import org.example.Utility.ExperimentConfig;
import org.junit.jupiter.api.Test;

/** The replica set configuration MongoClusterAdmin initiates: tags, priorities, and the seven-voter limit. */
class MongoClusterAdminTest {

    @Test
    void fiveMembersAllVoteAndThePreferredLeaderHasPriorityTen() {
        ExperimentConfig config = ExperimentConfig.load(Path.of("..", "config_local_mongodb.yaml"));
        List<Document> members = MongoClusterAdmin.members(config);
        assertEquals(5, members.size());
        for (int node = 0; node < 5; node++) {
            Document member = members.get(node);
            assertEquals(node, member.getInteger("_id"));
            assertEquals("localhost:27017", member.getString("host"));
            assertEquals(Integer.toString(node), member.get("tags", Document.class).getString("node"));
            assertEquals(node == 0 ? 10.0 : 1.0, member.getDouble("priority"));
            assertFalse(member.containsKey("votes"), "a voter keeps the default of one vote");
        }
    }

    @Test
    void membersBeyondTheSeventhAreNonVoting() {
        ExperimentConfig config = ExperimentConfig.load(Path.of("..", "config_scale_mongodb_9.yaml"));
        List<Document> members = MongoClusterAdmin.members(config);
        assertEquals(9, members.size());
        for (int node = 0; node < ExperimentConfig.MONGODB_MAX_VOTING_MEMBERS; node++) {
            assertFalse(members.get(node).containsKey("votes"));
            assertTrue(members.get(node).getDouble("priority") >= 1.0);
        }
        for (int node = ExperimentConfig.MONGODB_MAX_VOTING_MEMBERS; node < 9; node++) {
            assertEquals(0, members.get(node).getInteger("votes"), "MongoDB allows at most seven voters");
            assertEquals(0.0, members.get(node).getDouble("priority"), "a non-voting member cannot be primary");
            assertEquals(Integer.toString(node), members.get(node).get("tags", Document.class).getString("node"),
                    "reads still address it by tag");
        }
    }
}
