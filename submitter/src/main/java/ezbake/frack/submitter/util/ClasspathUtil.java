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
import com.google.common.collect.Iterables;
import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Set;

/**
 * Created by eperry on 12/10/13.
 */
public class ClasspathUtil {
    private static final Logger log = LoggerFactory.getLogger(ClasspathUtil.class);

    public static Optional<String> findClassInJar(final File jarFile) throws MalformedURLException, ClassNotFoundException {
        log.debug("Looking for class in jar {}", jarFile.getAbsolutePath());
        Optional<String> className = Optional.absent();
        // Get the builder class, have to use a separate classloader
        URLClassLoader contextLoader = new URLClassLoader(new URL[]{jarFile.toURI().toURL()}, Thread.currentThread().getContextClassLoader());
        final String[] builderClass = new String[] {null};
        Thread finderThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    URL url = jarFile.toURI().toURL();
                    URLClassLoader loader = new URLClassLoader(new URL[]{url});
                    final Class pipelineBuilder = Thread.currentThread().getContextClassLoader().loadClass("ezbake.frack.api.PipelineBuilder");

                    ConfigurationBuilder builder = new ConfigurationBuilder();
                    builder.setUrls(jarFile.toURI().toURL());
                    builder.addClassLoader(loader);
                    Reflections reflections = new Reflections(builder);
                    Set<Class<?>> types = reflections.getSubTypesOf(pipelineBuilder);
                    if (types.size() > 0) {
                        Class<?> type = Iterables.getFirst(types, null);
                        if (type != null) {
                            builderClass[0] = type.getCanonicalName();
                        }
                    }
                } catch (MalformedURLException e) {
                    log.error("Could not access jar file", e);
                } catch (ClassNotFoundException e) {
                    log.error("Could not find builder class in provided jar file, ensure that the pipeline project is dependent on 'frack'");
                }
            }
        });
        finderThread.setContextClassLoader(contextLoader);
        finderThread.start();
        try {
            finderThread.join();
            if (builderClass[0] != null) {
                className = Optional.of(builderClass[0]);
            }
        } catch (InterruptedException e) {
            log.error(String.format("Thread interrupted while attempting to find PipelineBuilder class in %s", jarFile.getName()), e);
        }

        return className;
    }
}
