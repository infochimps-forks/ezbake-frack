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

package ezbake.frack.submitter.util;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

/**
 * Created by eperry on 12/11/13.
 */
public class JarUtil {
    private static Logger log = LoggerFactory.getLogger(JarUtil.class);
    private static final int BUFFER_SIZE = 65536;

    public static String getRepackagedJarName(String seed) {
        String basename = FilenameUtils.getBaseName(seed);
        return basename + "-repackaged.jar";
    }

    public static File addFilesToJar(File sourceJar, List<File> newFiles) throws IOException {
        JarOutputStream target = null;
        JarInputStream source;
        File outputJar = new File(sourceJar.getParentFile(), getRepackagedJarName(sourceJar.getAbsolutePath()));
        log.debug("Output file created at {}", outputJar.getAbsolutePath());
        outputJar.createNewFile();
        try {
            source = new JarInputStream(new FileInputStream(sourceJar));
            target = new JarOutputStream(new FileOutputStream(outputJar));
            ZipEntry entry = source.getNextEntry();
            while (entry != null) {
                String name = entry.getName();
                // Add ZIP entry to output stream.
                target.putNextEntry(new ZipEntry(name));
                // Transfer bytes from the ZIP file to the output file
                int len;
                byte[] buffer = new byte[BUFFER_SIZE];
                while ((len = source.read(buffer)) > 0) {
                    target.write(buffer, 0, len);
                }
                entry = source.getNextEntry();
            }
            source.close();
            for (File fileToAdd : newFiles) {
                add(fileToAdd, fileToAdd.getParentFile().getAbsolutePath(), target);
            }
        } finally {
            if (target != null) {
                log.debug("Closing output stream");
                target.close();
            }
        }
        return outputJar;
    }

    private static void add(File source, final String prefix, JarOutputStream target) throws IOException {
        BufferedInputStream in = null;
        log.debug("Adding file {} to jar", source.getName());
        try {
            String entryPath = source.getPath().replace("\\", "/").replace(prefix, "");
            if (entryPath.startsWith("/")) {
                entryPath = entryPath.substring(1);
            }
            if (source.isDirectory()) {
                if (!entryPath.isEmpty()) {
                    if (!entryPath.endsWith("/")) {
                        entryPath += "/";
                    }
                    JarEntry entry = new JarEntry(entryPath);
                    entry.setTime(source.lastModified());
                    target.putNextEntry(entry);
                    target.closeEntry();
                }
                for (File nestedFile: source.listFiles()) {
                    add(nestedFile, prefix, target);
                }
            } else {
                JarEntry entry = new JarEntry(entryPath);
                entry.setTime(source.lastModified());
                target.putNextEntry(entry);
                in = new BufferedInputStream(new FileInputStream(source));

                byte[] buffer = new byte[BUFFER_SIZE];
                int len;
                while ((len = in.read(buffer)) > 0) {
                    target.write(buffer, 0, len);
                }
                target.closeEntry();
            }
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }
}
