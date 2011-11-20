/*
 * Copyright (c) (2005 - 2011) TouK sp. z o.o. s.k.a.
 * All rights reserved
 */

package setup;

import it.haslearnt.CassandraUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.pool.IThriftPool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/embeddedCassandra.xml", "classpath:/ioc/backend/applicationContext.xml"})
@Ignore
public abstract class IntegrationTest {

    @Autowired
    protected IThriftPool pool;

    @Autowired
    protected Cluster cluster;

    @Before
    public void cleanUp() throws Exception {
        CassandraUtils.cleanColumnFamilies(cluster, pool.getKeyspace());
    }

}
