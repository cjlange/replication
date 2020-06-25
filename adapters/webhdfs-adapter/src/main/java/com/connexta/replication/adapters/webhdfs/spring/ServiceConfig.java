/**
 * Copyright (c) Connexta
 *
 * <p>This is free software: you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or any later version.
 *
 * <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details. A copy of the GNU Lesser General Public
 * License is distributed along with this program and can be found at
 * <http://www.gnu.org/licenses/lgpl.html>.
 */
package com.connexta.replication.adapters.webhdfs.spring;

import com.connexta.replication.adapters.webhdfs.WebHdfsNodeAdapter;
import com.connexta.replication.adapters.webhdfs.WebHdfsNodeAdapterFactory;
import com.connexta.replication.spring.ReplicationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;

/** A class for instantiating beans in this module */
@Configuration("webhdfs-adapter")
public class ServiceConfig {

    /**
     * Instantiates a {@link WebHdfsNodeAdapterFactory} bean.
     *
     * @param replicationProperties application properties containing the timeouts for any client this
     *     factory creates
     * @return A factory for creating {@link WebHdfsNodeAdapter}s
     */
    @Bean
    public WebHdfsNodeAdapterFactory webHdfsNodeAdapterFactory(ReplicationProperties replicationProperties) {
        return new WebHdfsNodeAdapterFactory(
                new SimpleClientHttpRequestFactory(),
                replicationProperties.getConnectionTimeout(),
                replicationProperties.getReceiveTimeout());
    }
}
