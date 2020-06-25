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
package com.connexta.replication.adapters.webhdfs;

import com.connexta.replication.api.NodeAdapter;
import com.connexta.replication.api.NodeAdapterFactory;
import com.connexta.replication.api.data.SiteType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.client.SimpleClientHttpRequestFactory;

import java.net.URL;

/**
 * Factory for creating {@link WebHdfsNodeAdapter} instances
 */
public class WebHdfsNodeAdapterFactory implements NodeAdapterFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(WebHdfsNodeAdapterFactory.class);

  /**
   * Factory used to create HTTP request objects
   */
  private final SimpleClientHttpRequestFactory requestFactory;

  /**
   * The connection timeout for any clients created by this factory
   */
  private final int connectionTimeout;

  /**
   * The receive timeout for any clients created by this factory
   */
  private final int receiveTimeout;

  /**
   * Creates a WebHdfsNodeAdapter
   *
   * @param requestFactory a factory used to create HTTP request objects
   * @param connectionTimeout the connection timeout for any clients created by this factory
   * @param receiveTimeout the receive timeout for any clients created by this factory
   */
  public WebHdfsNodeAdapterFactory(SimpleClientHttpRequestFactory requestFactory, int connectionTimeout, int receiveTimeout) {
    this.requestFactory = requestFactory;
    this.connectionTimeout = connectionTimeout;
    this.receiveTimeout = receiveTimeout;
    LOGGER.debug(
            "Created a WebHdfsNodeAdapterFactory with a connection timeout of {}ms and a receive timeout of {}ms",
            connectionTimeout,
            receiveTimeout);
  }

  @Override
  public NodeAdapter create(URL url) {
    return null;
  }

  @Override
  public SiteType getType() {
    return null;
  }
}
