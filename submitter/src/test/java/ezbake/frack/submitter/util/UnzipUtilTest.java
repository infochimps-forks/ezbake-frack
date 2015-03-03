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

import com.google.common.base.Optional;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Created by eperry on 12/13/13.
 */
public class UnzipUtilTest {

    @Test
    public void testUnzip() throws IOException {
        File unzipped = null;
        try {
            InputStream is = getClass().getResourceAsStream("/example.tar.gz");
            byte[] tarGzBytes = IOUtils.toByteArray(is);
            is.close();
            unzipped = UnzipUtil.unzip(new File(System.getProperty("user.dir")), ByteBuffer.wrap(tarGzBytes));
            assertTrue("Unzipped folder exists", unzipped.exists());
            assertTrue("Unzipped folder is a folder", unzipped.isDirectory());

            Optional<String> jarPath = UnzipUtil.getJarPath(unzipped);
            assertTrue("Jar path exists", jarPath.isPresent());

            File jarFile = new File(jarPath.get());
            assertEquals("Jar path has correct name", "example.jar", jarFile.getName());
            assertEquals("Jar has correct parent", "bin", jarFile.getParentFile().getName());

            Optional<String> confPath = UnzipUtil.getConfDirectory(unzipped);
            assertTrue("Conf path exists", confPath.isPresent());

            File confDir = new File(confPath.get());
            Optional<String> sslPath = UnzipUtil.getSSLPath(confDir);
            assertTrue("SSL path exists", sslPath.isPresent());

            File keyPath = UnzipUtil.findSubDir(confDir, "keys");
            assertTrue("Keys path exists", keyPath.exists());

        } finally {
            if (unzipped != null && unzipped.exists()) {
                FileUtils.deleteDirectory(unzipped);
            }
        }
    }
}
