package ru.mail.polis.mburmistrov.util;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

public class GetServers {
    public static List<String> getNodesById(@NotNull String id,
                                            int from,
                                            @NotNull List<String> topology) {
        List<String> nodes = new ArrayList<>();
        int hash = Math.abs(id.hashCode());
        for (int i = 0; i < from; i++) {
            int idx = (hash + i) % topology.size();
            nodes.add(topology.get(idx));
        }
        return nodes;
    }
}
