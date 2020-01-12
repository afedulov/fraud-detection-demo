/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.demo.backend.configurations;

import java.util.Arrays;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.AbstractEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.stereotype.Component;

@Component
public class PropertyLogger {

  @Autowired
  public PropertyLogger(ApplicationContext context) {
    logProperties(context);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(PropertyLogger.class);

  @EventListener
  public void handleContextRefresh(ContextRefreshedEvent event) {
    logProperties(event.getApplicationContext());
  }

  public void logProperties(ApplicationContext context) {
    final Environment env = context.getEnvironment();
    LOGGER.info("====== Environment and configuration ======");
    LOGGER.info("Active profiles: {}", Arrays.toString(env.getActiveProfiles()));
    final MutablePropertySources sources = ((AbstractEnvironment) env).getPropertySources();
    StreamSupport.stream(sources.spliterator(), false)
        .filter(ps -> ps instanceof EnumerablePropertySource)
        .map(ps -> ((EnumerablePropertySource) ps).getPropertyNames())
        .flatMap(Arrays::stream)
        .distinct()
        .filter(
            prop ->
                !(prop.contains("credentials")
                    || prop.contains("password")
                    || prop.contains("java.class.path")
                    || prop.contains("sun.boot.class.path")))
        .forEach(prop -> LOGGER.info("{}: {}", prop, env.getProperty(prop)));
    LOGGER.info("===========================================");
  }
}
