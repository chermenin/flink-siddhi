/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.siddhi.utils;

import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;

import java.lang.annotation.Annotation;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;

public class ReflectionUtils {

    private static URL[] getClasspathUrls() {
        ArrayList<URL> urls = new ArrayList<>();
        ClassLoader[] classLoaders = {
            ReflectionUtils.class.getClassLoader(),
            Thread.currentThread().getContextClassLoader()
        };

        for (ClassLoader classLoader : classLoaders) {
            if (classLoader instanceof URLClassLoader) {
                urls.addAll(Arrays.asList(((URLClassLoader) classLoader).getURLs()));
            } else {
                throw new RuntimeException("classLoader is not an instanceof URLClassLoader");
            }
        }

        return urls.toArray(new URL[]{});
    }

    public static Class<?>[] getAnnotatedClasses(Class<? extends Annotation> annotationClass) {
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.addUrls(getClasspathUrls());
        Reflections reflections = new Reflections(configurationBuilder);
        return reflections.getTypesAnnotatedWith(annotationClass).toArray(new Class[]{});
    }
}
