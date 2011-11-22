/*
 * Copyright: this code is distributed under WTFPL version2
 * In short: You just DO WHAT THE FUCK YOU WANT TO.
 */

package setup;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

class CassandraDataCleaner {
    public void clean() throws IOException {
        makeDirsIfNotExist();
        cleanupDataDirectories();
    }

    public void cleanupDataDirectories() throws IOException {
        for (String s : getDataDirs()) {
            cleanDir(s);
        }
    }

    public void makeDirsIfNotExist() throws IOException {
        for (String s : getDataDirs()) {
            mkdir(s);
        }
    }

    private Set<String> getDataDirs() {
        Set<String> dirs = new HashSet<String>();
        Collections.addAll(dirs, DatabaseDescriptor.getAllDataFileLocations());
        dirs.add(DatabaseDescriptor.getCommitLogLocation());
        return dirs;
    }

    private void mkdir(String dir) throws IOException {
        FileUtils.createDirectory(dir);
    }

    private void cleanDir(String dir) throws IOException {
        File dirFile = new File(dir);
        if (dirFile.exists() && dirFile.isDirectory()) {
            FileUtils.delete(dirFile.listFiles());
        }
    }
}
