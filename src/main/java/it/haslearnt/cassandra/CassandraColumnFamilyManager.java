package it.haslearnt.cassandra;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.ColumnFamilyManager;
import org.scale7.cassandra.pelops.KeyspaceManager;
import org.scale7.cassandra.pelops.Pelops;

public class CassandraColumnFamilyManager {

    public static void recreateColumnFamily(Cluster cluster, String keyspace, final String columnFamily, String comparator) throws Exception {
        boolean hasMatchingColumnFamilies = hasMatchingColumnFamilies(columnFamily, cluster, keyspace);
        ColumnFamilyManager columnFamilyManager = Pelops.createColumnFamilyManager(cluster, keyspace);
        if (hasMatchingColumnFamilies) {
            columnFamilyManager.dropColumnFamily(columnFamily);
            columnFamilyManager.addColumnFamily(new CfDef(keyspace, columnFamily).setComparator_type(comparator));
        } else {
            columnFamilyManager.addColumnFamily(new CfDef(keyspace, columnFamily).setComparator_type(comparator));
        }
    }

    public static void addColumnFamilyIfNeeded(Cluster cluster, String keyspace, final String columnFamily, String comparator) throws Exception {
        boolean hasMatchingColumnFamilies = hasMatchingColumnFamilies(columnFamily, cluster, keyspace);
        ColumnFamilyManager columnFamilyManager = Pelops.createColumnFamilyManager(cluster, keyspace);
        if (!hasMatchingColumnFamilies) {
            columnFamilyManager.addColumnFamily(new CfDef(keyspace, columnFamily).setComparator_type(comparator));
        }
    }

    private static boolean hasMatchingColumnFamilies(String columnFamily, Cluster cluster, String keyspace) throws Exception {
        KeyspaceManager keyspaceManager = Pelops.createKeyspaceManager(cluster);
        KsDef keyspaceSchema = keyspaceManager.getKeyspaceSchema(keyspace);
        return !Collections2.filter(keyspaceSchema.getCf_defs(), new ColumnFamilyMatchNamePredicate(columnFamily)).isEmpty();
    }


    public static void cleanColumnFamilies(Cluster cluster, String keyspace) throws Exception {
        KeyspaceManager keyspaceManager = Pelops.createKeyspaceManager(cluster);
        ColumnFamilyManager columnFamilyManager = Pelops.createColumnFamilyManager(cluster, keyspace);
        KsDef ksDef = keyspaceManager.getKeyspaceSchema(keyspace);
        for (CfDef cf : ksDef.getCf_defs()) {
            columnFamilyManager.truncateColumnFamily(cf.getName());
        }
    }

    private static class ColumnFamilyMatchNamePredicate implements Predicate<CfDef> {
        private final String columnFamily;

        public ColumnFamilyMatchNamePredicate(String columnFamily) {
            this.columnFamily = columnFamily;
        }

        @Override
        public boolean apply(@javax.annotation.Nullable CfDef input) {
            return input != null && columnFamily.equals(input.getName());
        }
    }
}
