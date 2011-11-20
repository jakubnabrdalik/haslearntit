package it.haslearnt.cassandra;

import org.scale7.cassandra.pelops.spring.CommonsBackedPoolFactoryBean;

public class CassandraConnectionProvider extends CommonsBackedPoolFactoryBean {

    private boolean clean = false;

    @Override
    public void afterPropertiesSet() throws Exception {
        CassandraKeyspaceManager.ensureKeyspace(getCluster(), getKeyspace());
        if(clean) {
            CassandraColumnFamilyManager.recreateColumnFamily(getCluster(), getKeyspace(), "Notes", "LongType");
        } else {
            CassandraColumnFamilyManager.addColumnFamilyIfNeeded(getCluster(), getKeyspace(), "Notes", "LongType");
        }
        super.afterPropertiesSet();
    }


    public void setClean(boolean clean) {
        this.clean = clean;
    }
}
