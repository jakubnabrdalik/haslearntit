/*
 * Copyright: this code is distributed under WTFPL version2
 * In short: You just DO WHAT THE FUCK YOU WANT TO.
 */

package setup;

import it.haslearnt.cassandra.CassandraColumnFamilyManager;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.pool.IThriftPool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/ioc/embeddedCassandra.xml", "classpath:/ioc/backend/applicationContext.xml"})
@Ignore
public abstract class IntegrationTest {

    @Autowired
    protected IThriftPool pool;

    @Autowired
    protected Cluster cluster;

    @Before
    public void cleanUp() throws Exception {
        CassandraColumnFamilyManager.cleanColumnFamilies(cluster, pool.getKeyspace());
    }

}
