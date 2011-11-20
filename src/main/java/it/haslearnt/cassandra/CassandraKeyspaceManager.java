/*
 * Copyright (c) (2005 - 2011) TouK sp. z o.o. s.k.a.
 * All rights reserved
 */

package it.haslearnt.cassandra;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.KeyspaceManager;
import org.scale7.cassandra.pelops.Pelops;

import java.util.Collections;

public class CassandraKeyspaceManager {
    public static void ensureKeyspace(Cluster cluster, final String keyspace) throws Exception {
        KeyspaceManager keyspaceManager = Pelops.createKeyspaceManager(cluster);
        if (hasNoMatchingKeyspaces(keyspace, keyspaceManager)) {
            KsDef ksDef = new KsDef(keyspace, "org.apache.cassandra.locator.SimpleStrategy", Collections.<CfDef>emptyList());
            ksDef.setStrategy_options(ImmutableMap.of("replication_factor", "1"));
            keyspaceManager.addKeyspace(ksDef);
        }
    }

    private static boolean hasNoMatchingKeyspaces(String keyspace, KeyspaceManager ksMgr) throws Exception {
        return Collections2.filter(ksMgr.getKeyspaceNames(), new KeyspaceMatchNamePredicate(keyspace)).isEmpty();
    }

    private static class KeyspaceMatchNamePredicate implements Predicate<KsDef> {
       private final String keyspace;

       public KeyspaceMatchNamePredicate(String keyspace) {
           this.keyspace = keyspace;
       }

       @Override
       public boolean apply(@javax.annotation.Nullable KsDef input) {
           return input != null && keyspace.equals(input.getName());
       }
   }
}
