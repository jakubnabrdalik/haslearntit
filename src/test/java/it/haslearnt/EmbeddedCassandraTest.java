package it.haslearnt;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.junit.Assert;
import org.junit.Test;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Selector;
import setup.IntegrationTest;

public class EmbeddedCassandraTest extends IntegrationTest {
    @Test
    public void shouldConnectToEmbeddedCassandra() {
        //given
        Mutator m = pool.createMutator();
        Selector s = pool.createSelector();

        //when
        m.writeColumn("Notes", "#1", m.newColumn(Bytes.fromLong(1), "test"));
        m.execute(ConsistencyLevel.ONE);

        //then
        Column c = s.getColumnFromRow("Notes", "#1", Bytes.fromLong(1), ConsistencyLevel.ONE);
        Assert.assertNotNull(c);
        Assert.assertEquals("test", Bytes.toUTF8(c.value));
    }
}
