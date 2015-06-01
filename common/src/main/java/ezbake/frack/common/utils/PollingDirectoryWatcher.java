/*   Copyright (C) 2013-2015 Computer Sciences Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */

package ezbake.frack.common.utils;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * The PollingDirectoryWatcher is a utility class which polls a directory with a given timeout and
 * returns all files in the directory if files are found. It is assumed that the user of PollingDirectoryWatcher
 * is deleting files after use, so each time files are found in the directory they are considered new. Sleeps are
 * used between each iteration of the poll call, with a default sleep time of 200 ms.
 */
public class PollingDirectoryWatcher {
    private static final Logger log = LoggerFactory.getLogger(PollingDirectoryWatcher.class);
    private static final long DEFAULT_SLEEP_MS = 200;
    private static final long DEFAULT_TIMEOUT_MS = 1000;
    private long sleepMs;
    private long timeout;
    private File dir;

    /**
     * Constructor which sets the sleep time and timeout to the default values.
     *
     * @param dirToWatch directory being watched for new files
     */
    public PollingDirectoryWatcher(File dirToWatch) throws IOException {
        this(DEFAULT_TIMEOUT_MS, dirToWatch);
    }

    /**
     * Constructor for PollingDirectoryWatcher. A timeout and directory are required. The timeout must be longer
     * than the sleep time between iterations in {@link PollingDirectoryWatcher#poll()}.
     *
     * @param timeoutMs timeout in milliseconds for calls to {@link PollingDirectoryWatcher#poll()}. Must be longer
     *                  than 200 ms
     * @param dirToWatch directory being watched for new files
     * @throws IOException
     */
    public PollingDirectoryWatcher(long timeoutMs, File dirToWatch) throws IOException {
        this(DEFAULT_SLEEP_MS, timeoutMs, dirToWatch);
    }

    /**
     * Constructor which sets the sleep time, timeout and directory to watch for this directory watcher.
     *
     * @param sleepMs sleep time in milliseconds between each iteration in the poll call
     * @param timeoutMs timeout in milliseconds for calls to {@link PollingDirectoryWatcher#poll()}. Must be longer
     *                  than sleepMs
     * @param dirToWatch directory being watched for new files
     * @throws IOException
     */
    public PollingDirectoryWatcher(long sleepMs, long timeoutMs, File dirToWatch) throws IOException {
        Preconditions.checkArgument(timeoutMs > sleepMs, "Timeout ms must be larger than " + sleepMs);
        Preconditions.checkNotNull(dirToWatch, "dirToWatch cannot be null");
        this.sleepMs = sleepMs;
        this.timeout = timeoutMs;
        this.dir = dirToWatch;

        if (!this.dir.exists()) {
            log.info("Directory {} did not exist, creating", dir.getAbsolutePath());
            if (!this.dir.mkdirs()) throw new IOException(String.format("Could not create directory %s for directory watcher", dir.getAbsolutePath()));
        }
    }

    /**
     * Polls the directory given in the constructor for new files. Each time files are returned to the caller, it is
     * assumed that the caller is managing the list of files in the directory or deleting files that have been
     * processed. For this reason, if any files are found in the directory then they are returned.
     *
     * @return all files from directory if any files are found
     * @throws IOException
     */
    public Optional<File[]> poll() throws IOException {
        Optional<File[]> result = Optional.absent();
        long endMillis = System.currentTimeMillis() + timeout;
        while(System.currentTimeMillis() < endMillis) {
            File[] files = dir.listFiles();
            if (files != null && files.length > 0) {
                result = Optional.of(files);
                break;
            } else {
                try {
                    log.debug("No files found in {}, sleeping for {} and trying again.", dir.getAbsolutePath(), sleepMs);
                    Thread.sleep(sleepMs);
                } catch (InterruptedException e) {
                    throw new IOException(String.format("Thread interrupted while polling %s", dir.getAbsolutePath()), e);
                }
            }
        }

        return result;
    }
}
