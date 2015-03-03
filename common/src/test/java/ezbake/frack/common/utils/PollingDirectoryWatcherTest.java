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
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Set;

/**
 * Created by eperry on 12/30/13.
 */
public class PollingDirectoryWatcherTest {

    @Test
    public void testPoll_NoFiles() throws IOException, InterruptedException {
        File directory = new File("test_dir");
        directory.mkdir();
        try {
            final PollingDirectoryWatcher watcher = new PollingDirectoryWatcher(2000, directory);
            Optional<File[]> result = watcher.poll();

            assertFalse("No files retrieved", result.isPresent());
        } finally {
            FileUtils.deleteDirectory(directory);
        }
    }

    @Test
    public void testPoll_FileExists() throws IOException, InterruptedException {
        File directory = new File("test_dir");
        directory.mkdir();
        try {
            final PollingDirectoryWatcher watcher = new PollingDirectoryWatcher(2000, directory);
            final Set<File> fileSet = Sets.newHashSet();
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    try {
                        Optional<File[]> files = watcher.poll();
                        if (files.isPresent() && files.get().length > 0) {
                            fileSet.add(files.get()[0]);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            };

            new Thread(runnable).start();

            Thread.sleep(500);
            File newFile = new File(directory, "test.txt");
            newFile.createNewFile();
            Thread.sleep(500);

            assertEquals("File was retrieved", 1, fileSet.size());
            File retrieved = Iterables.getFirst(fileSet, null);
            assertEquals("Correct file retrieved", "test.txt", retrieved.getName());
        } finally {
            FileUtils.deleteDirectory(directory);
        }
    }
}
