package setup;

import org.apache.cassandra.thrift.CassandraDaemon;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;

class EmbeddedCassandra implements Runnable {
    private CassandraDaemon cassandraDaemon;

    public void init() throws TTransportException, IOException {
        cassandraDaemon = new CassandraDaemon();
        cassandraDaemon.init(null);
    }

    public void run() {
        cassandraDaemon.start();
    }
}
