/*
 * Copyright: this code is distributed under WTFPL version2
 * In short: You just DO WHAT THE FUCK YOU WANT TO.
 */

package setup;

import org.apache.thrift.transport.TTransportException;

import java.io.IOException;

public class EmbeddedCassandraStarter {
    private CassandraDataCleaner cassandraCleaner;

    public EmbeddedCassandraStarter(CassandraDataCleaner cassandraCleaner) {
        this.cassandraCleaner = cassandraCleaner;
    }

    public EmbeddedCassandra startNewCassandra() throws IOException, TTransportException {
        cassandraCleaner.clean();
        EmbeddedCassandra cassandra = createEmbeddedCassandra();
        startNewThread(cassandra);
        return cassandra;
    }

    private EmbeddedCassandra createEmbeddedCassandra() throws TTransportException, IOException {
        System.setProperty("cassandra.config", "cassandra.yaml");
        EmbeddedCassandra cassandra = new EmbeddedCassandra();
        cassandra.init();
        return cassandra;
    }

    private void startNewThread(EmbeddedCassandra cassandra) {
        Thread t = new Thread(cassandra);
        t.setDaemon(true);
        t.start();
    }
}
