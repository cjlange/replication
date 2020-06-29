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
import com.connexta.replication.api.data.CreateRequest;
import com.connexta.replication.api.data.CreateStorageRequest;
import com.connexta.replication.api.data.DeleteRequest;
import com.connexta.replication.api.data.Metadata;
import com.connexta.replication.api.data.QueryRequest;
import com.connexta.replication.api.data.QueryResponse;
import com.connexta.replication.api.data.ResourceRequest;
import com.connexta.replication.api.data.ResourceResponse;
import com.connexta.replication.api.data.UpdateRequest;
import com.connexta.replication.api.data.UpdateStorageRequest;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestOperations;

/** Interacts with a remote Hadoop instance through the webHDFS REST API */
public class WebHdfsNodeAdapter implements NodeAdapter {

  private static final Logger LOGGER = LoggerFactory.getLogger(WebHdfsNodeAdapter.class);

  private static final String HTTP_OPERATION_KEY = "op";
  private static final String HTTP_OPERATION_CHECK_ACCESS = "CHECKACCESS";

  private static final String HTTP_FILE_SYSTEM_ACTION_KEY = "fsaction";
  private static final String HTTP_FILE_SYSTEM_ACTION_ALL = "rwx";

  /** The address of the REST API for the Hadoop instance */
  private final URL webHdfsUrl;

  /** Interface defining a basic set of RESTful operations */
  private final RestOperations restOperations;

  /**
   * Adapter to interact with a Hadoop instance through the webHDFS REST API
   *
   * @param webHdfsUrl the address of the REST API for the Hadoop instance
   * @param restOperations an interface defining a basic set of RESTful operations
   */
  public WebHdfsNodeAdapter(URL webHdfsUrl, RestOperations restOperations) {
    this.webHdfsUrl = webHdfsUrl;
    this.restOperations = restOperations;
  }

  URL getWebHdfsUrl() {
    return webHdfsUrl;
  }

  @Override
  public boolean isAvailable() {
    LOGGER.info("Checking access to: {}", getWebHdfsUrl());

    Map<String, String> params = new HashMap<>();
    params.put(HTTP_OPERATION_KEY, HTTP_OPERATION_CHECK_ACCESS);
    params.put(HTTP_FILE_SYSTEM_ACTION_KEY, HTTP_FILE_SYSTEM_ACTION_ALL);

    ResponseEntity<String> response =
        restOperations.exchange(
            getWebHdfsUrl().toString(), HttpMethod.GET, null, String.class, params);

    if (response.getStatusCode() != HttpStatus.OK) {
      LOGGER.debug("Access to {} is not available.", getWebHdfsUrl());
      return false;
    }

    return true;
  }

  @Override
  public String getSystemName() {
    return null;
  }

  @Override
  public QueryResponse query(QueryRequest queryRequest) {
    return null;
  }

  @Override
  public boolean exists(Metadata metadata) {
    return false;
  }

  @Override
  public boolean createRequest(CreateRequest createRequest) {
    return false;
  }

  @Override
  public boolean updateRequest(UpdateRequest updateRequest) {
    return false;
  }

  @Override
  public boolean deleteRequest(DeleteRequest deleteRequest) {
    return false;
  }

  @Override
  public ResourceResponse readResource(ResourceRequest resourceRequest) {
    return null;
  }

  @Override
  public boolean createResource(CreateStorageRequest createStorageRequest) {
    return false;
  }

  @Override
  public boolean updateResource(UpdateStorageRequest updateStorageRequest) {
    return false;
  }

  @Override
  public void close() throws IOException {}
}
