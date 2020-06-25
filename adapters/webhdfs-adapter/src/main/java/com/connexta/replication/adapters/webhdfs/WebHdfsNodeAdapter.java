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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestOperations;

import java.io.IOException;
import java.net.URL;

/** Interacts with a remote Hadoop instance through the webHDFS REST API */
public class WebHdfsNodeAdapter implements NodeAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(WebHdfsNodeAdapter.class);

    /**
     * The address of the REST API for the Hadoop instance
     */
    private final URL webHdfsUrl;

    /**
     * Interface defining a basic set of RESTful operations
     */
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

    @Override
    public boolean isAvailable() {
        return false;
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
    public void close() throws IOException {

    }
}
