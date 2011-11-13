package com.haslearntit;

import org.scale7.cassandra.pelops.spring.CommonsBackedPoolFactoryBean;

public class CassandraConnectionProvider extends CommonsBackedPoolFactoryBean {

    private boolean clean = false;

    @Override
    public void afterPropertiesSet() throws Exception {
        CassandraUtils.ensureKeyspace(getCluster(), getKeyspace());
        CassandraUtils.ensureColumnFamily(getCluster(), getKeyspace(), "Notes", "LongType", clean);

        super.afterPropertiesSet();
    }


    public void setClean(boolean clean) {
        this.clean = clean;
    }
}
