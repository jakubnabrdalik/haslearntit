package com.haslearntit;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.ColumnFamilyManager;
import org.scale7.cassandra.pelops.KeyspaceManager;
import org.scale7.cassandra.pelops.Pelops;

import java.util.Collections;

public class CassandraUtils {

    public static void ensureKeyspace(Cluster cluster, final String keyspace) throws Exception {
        KeyspaceManager ksMgr = Pelops.createKeyspaceManager(cluster);
        if (Collections2.filter(ksMgr.getKeyspaceNames(), new Predicate<KsDef>() {
            @Override
            public boolean apply(@javax.annotation.Nullable KsDef input) {
                return input != null && keyspace.equals(input.getName());
            }
        }).isEmpty()) {
            ksMgr.addKeyspace(new KsDef(keyspace, "org.apache.cassandra.locator.SimpleStrategy", Collections.<CfDef>emptyList()).setStrategy_options(ImmutableMap.of("replication_factor", "1")));
        }

    }

    public static void ensureColumnFamily(Cluster cluster, String keyspace, final String columnFamily, String comparator, boolean clean) throws Exception {
        KeyspaceManager ksMgr = Pelops.createKeyspaceManager(cluster);
        KsDef ksDef = ksMgr.getKeyspaceSchema(keyspace);

        boolean cfExists = !Collections2.filter(ksDef.getCf_defs(), new Predicate<CfDef>() {
            @Override
            public boolean apply(@javax.annotation.Nullable CfDef input) {
                return input != null && columnFamily.equals(input.getName());
            }
        }).isEmpty();

        ColumnFamilyManager cfMgr = Pelops.createColumnFamilyManager(cluster, keyspace);
        if (cfExists && clean) {
            cfMgr.dropColumnFamily(columnFamily);
            cfMgr.addColumnFamily(new CfDef(keyspace, columnFamily).setComparator_type(comparator));
        } else if (!cfExists) {
            cfMgr.addColumnFamily(new CfDef(keyspace, columnFamily).setComparator_type(comparator));
        }
    }


    public static void cleanColumnFamilies(Cluster cluster, String keyspace) throws Exception {
        KeyspaceManager ksMgr = Pelops.createKeyspaceManager(cluster);
        ColumnFamilyManager cfMgr = Pelops.createColumnFamilyManager(cluster, keyspace);

        KsDef ksDef = ksMgr.getKeyspaceSchema(keyspace);

        for (CfDef cf : ksDef.getCf_defs()) {
            cfMgr.truncateColumnFamily(cf.getName());
        }

    }

}
