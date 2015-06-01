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

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.*;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by eperry on 12/16/13.
 */
public class JarUtilTest {

    @Test
    public void getRepackagedJarName() {
        String filePath = "/tmp/file/test.jar";
        String repackagedPath = JarUtil.getRepackagedJarName(filePath);
        assertEquals("Repackaged jar has the correct name", "test-repackaged.jar", repackagedPath);
    }

    @Test
    public void addFileToJar() throws IOException {
        File tmpDir = new File("jarUtilTest");
        try {
            tmpDir.mkdirs();

            // Push the example jar to a file
            byte[] testJar = IOUtils.toByteArray(this.getClass().getResourceAsStream("/example.jar"));
            File jarFile = new File(tmpDir, "example.jar");
            BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(jarFile));
            os.write(testJar);
            os.close();

            // Push the test content to a file
            byte[] testContent = IOUtils.toByteArray(this.getClass().getResourceAsStream("/test.txt"));
            String stringTestContent = new String(testContent);
            File testFile = new File(tmpDir, "test.txt");
            os = new BufferedOutputStream(new FileOutputStream(testFile));
            os.write(testContent);
            os.close();

            assertTrue(jarFile.exists());
            assertTrue(testFile.exists());

            // Add the new file to the jar
            File newJar = JarUtil.addFilesToJar(jarFile, Lists.newArrayList(testFile));
            assertTrue("New jar file exists", newJar.exists());
            assertTrue("New jar is a file", newJar.isFile());

            // Roll through the entries of the new jar and
            JarInputStream is = new JarInputStream(new FileInputStream(newJar));
            JarEntry entry = is.getNextJarEntry();
            boolean foundNewFile = false;
            String content = "";
            while (entry != null) {
                String name = entry.getName();
                if (name.endsWith("test.txt")) {
                    foundNewFile = true;
                    byte[] buffer = new byte[1];
                    while ((is.read(buffer)) > 0) {
                        content += new String(buffer);
                    }
                    break;
                }
                entry = is.getNextJarEntry();
            }
            is.close();
            assertTrue("New file was in repackaged jar", foundNewFile);
            assertEquals("Content of added file is the same as the retrieved new file", stringTestContent, content);
        } finally {
            FileUtils.deleteDirectory(tmpDir);
        }
    }

}
