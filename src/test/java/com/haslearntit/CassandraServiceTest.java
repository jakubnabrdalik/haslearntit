package com.haslearntit;

import org.apache.cassandra.contrib.utils.service.CassandraServiceDataCleaner;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Selector;
import org.scale7.cassandra.pelops.pool.IThriftPool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/ioc/backend/applicationContext.xml"})
public class CassandraServiceTest {

    private static EmbeddedCassandraService cassandra;

    @BeforeClass
    public static void setup() throws TTransportException, IOException, InterruptedException {
        System.setProperty("cassandra.config", "cassandra.yaml");

        CassandraServiceDataCleaner cleaner = new CassandraServiceDataCleaner();
        cleaner.prepare();
        cassandra = new EmbeddedCassandraService();
        cassandra.init();
        Thread t = new Thread(cassandra);
        t.setDaemon(true);
        t.start();
    }

    @Autowired
    private IThriftPool pool;

    @Autowired
    private Cluster cluster;

    @Before
    public void cleanUp() throws Exception {
        CassandraUtils.cleanColumnFamilies(cluster, pool.getKeyspace());
    }

    @Test
    public void testInProcessCassandraServer() throws UnsupportedEncodingException, TTransportException {
        Mutator m = pool.createMutator();
        m.writeColumn("Notes", "#1", m.newColumn(Bytes.fromLong(1), "test"));
        m.execute(ConsistencyLevel.ONE);

        Selector s = pool.createSelector();
        Column c = s.getColumnFromRow("Notes", "#1", Bytes.fromLong(1), ConsistencyLevel.ONE);
        Assert.assertNotNull(c);
        Assert.assertEquals("test", Bytes.toUTF8(c.value));
    }

}
