package org.example.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Random;

import org.junit.jupiter.api.Test;

class MajorityMatchIndexTest {

    /** The walk the leader used before: down from the highest match index to the first one a quorum holds. */
    private static int walkDown(int[] matches, int quorum) {
        int max = -1;
        for (int m : matches) {
            max = Math.max(max, m);
        }
        for (int idx = max; idx > -1; idx--) {
            int count = 0;
            for (int m : matches) {
                if (m >= idx) {
                    count++;
                }
            }
            if (count >= quorum) {
                return idx;
            }
        }
        return -1;
    }

    @Test
    void agreesWithTheWalkDownOnRandomSpreads() {
        Random random = new Random(42);
        for (int trial = 0; trial < 20_000; trial++) {
            int servers = 2 + random.nextInt(30);
            int[] matches = new int[servers];
            int base = random.nextInt(1_000);
            int spread = 1 + random.nextInt(trial % 2 == 0 ? 20 : 70_000);
            for (int i = 0; i < servers; i++) {
                matches[i] = base + random.nextInt(spread);
            }
            matches[random.nextInt(servers)] = -1; // the leader's own entry
            int quorum = servers / 2;
            assertEquals(walkDown(matches, quorum), ServerImpl.majorityMatchIndex(matches, quorum),
                    "servers " + servers + " spread " + spread);
        }
    }

    @Test
    void twentySevenServersCommitTheThirteenthFurthestFollower() {
        int[] matches = new int[27];
        matches[0] = -1; // leader
        for (int i = 1; i < 27; i++) {
            matches[i] = 1_000 * i; // follower 1 is furthest behind, 26 furthest ahead
        }
        // 13 followers (14..26) plus the leader are a majority of 27.
        assertEquals(14_000, ServerImpl.majorityMatchIndex(matches, 27 / 2));
    }

    @Test
    void rejectsAQuorumOutsideTheArray() {
        assertThrows(IllegalArgumentException.class, () -> ServerImpl.majorityMatchIndex(new int[] {-1}, 0));
        assertThrows(IllegalArgumentException.class, () -> ServerImpl.majorityMatchIndex(new int[] {-1, 3}, 3));
    }
}
