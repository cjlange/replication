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

import com.connexta.replication.adapters.webhdfs.filesystem.DirectoryListing;
import com.connexta.replication.adapters.webhdfs.filesystem.FileStatus;
import com.connexta.replication.adapters.webhdfs.filesystem.IterativeDirectoryListing;
import com.connexta.replication.api.AdapterException;
import com.connexta.replication.api.AdapterInterruptedException;
import com.connexta.replication.api.NodeAdapter;
import com.connexta.replication.api.ReplicationException;
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
import com.connexta.replication.data.MetadataAttribute;
import com.connexta.replication.data.MetadataImpl;
import com.connexta.replication.data.QueryResponseImpl;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.Nullable;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestOperations;
import org.springframework.web.util.UriComponentsBuilder;

/** Interacts with a remote Hadoop instance through the webHDFS REST API */
public class WebHdfsNodeAdapter implements NodeAdapter {

  private static final Logger LOGGER = LoggerFactory.getLogger(WebHdfsNodeAdapter.class);

  private static final String HTTP_OPERATION_KEY = "op";
  private static final String HTTP_OPERATION_CHECK_ACCESS = "CHECKACCESS";
  private static final String HTTP_OPERATION_LIST_STATUS_BATCH = "LISTSTATUS_BATCH";

  private static final String HTTP_FILE_SYSTEM_ACTION_KEY = "fsaction";
  private static final String HTTP_FILE_SYSTEM_ACTION_ALL = "rwx";

  private static final String HTTP_START_AFTER_KEY = "startAfter";

  private static final String METADATA_ATTRIBUTE_NAME_ID = "id";
  private static final String METADATA_ATTRIBUTE_NAME_TITLE = "title";
  private static final String METADATA_ATTRIBUTE_NAME_CREATED = "created";
  private static final String METADATA_ATTRIBUTE_NAME_MODIFIED = "modified";
  private static final String METADATA_ATTRIBUTE_NAME_RESOURCE_URI = "resource-uri";
  private static final String METADATA_ATTRIBUTE_NAME_RESOURCE_SIZE = "resource-size";
  private static final String METADATA_ATTRIBUTE_TYPE_STRING = "string";
  private static final String METADATA_ATTRIBUTE_TYPE_DATE = "date";

  private static final int UUID_VERSION_INDEX = 14;

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

  URI getWebHdfsUri() throws URISyntaxException {
    return webHdfsUrl.toURI();
  }

  @Override
  public boolean isAvailable() {
    LOGGER.info("Checking access to: {}", getWebHdfsUrl());

    MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
    params.add(HTTP_OPERATION_KEY, HTTP_OPERATION_CHECK_ACCESS);
    params.add(HTTP_FILE_SYSTEM_ACTION_KEY, HTTP_FILE_SYSTEM_ACTION_ALL);

    UriComponentsBuilder uriComponentsBuilder =
        UriComponentsBuilder.fromHttpUrl(getWebHdfsUrl().toString()).queryParams(params);

    ResponseEntity<String> response =
        restOperations.exchange(
            uriComponentsBuilder.toUriString(), HttpMethod.GET, null, String.class);

    if (response.getStatusCode() != HttpStatus.OK) {
      LOGGER.debug("Access to {} is not available.", getWebHdfsUrl());
      return false;
    }

    return true;
  }

  @Override
  public String getSystemName() {
    return "webHDFS";
  }

  @Override
  public QueryResponse query(QueryRequest queryRequest) {
    List<FileStatus> filesToReplicate = getFilesToReplicate(queryRequest.getModifiedAfter());

    filesToReplicate.sort(Comparator.comparing(FileStatus::getModificationTime));

    List<Metadata> results = new ArrayList<>();

    for (FileStatus file : filesToReplicate) {
      // TODO: 7/3/20 Need approach for when consuming data from HDFS into Ion
      results.add(createMetadata(file));
    }

    return new QueryResponseImpl(results, e -> wrapException("Failed to query remote system", e));
  }

  /**
   * Creates {@link MetadataAttribute}s to use in creating a {@link MetadataImpl} object for a file
   * being replicated.
   *
   * @param fileStatus information about the file being replicated
   * @return a {@link MetadataImpl} object for the file being replicated
   */
  Metadata createMetadata(FileStatus fileStatus) {
    // TODO: 7/3/20 This approach is for creation in DDF, not Ion

    Map<String, MetadataAttribute> metadataAttributes = new HashMap<>();

    String fileUrl = getWebHdfsUrl().toString() + fileStatus.getPathSuffix();
    LOGGER.debug("Creating Metadata object from file at: {}", fileUrl);

    Date modificationTime = fileStatus.getModificationTime();
    String id = getVersion4Uuid(fileUrl, modificationTime);

    MetadataAttribute idAttribute =
        new MetadataAttribute(
            METADATA_ATTRIBUTE_NAME_ID,
            METADATA_ATTRIBUTE_TYPE_STRING,
            Collections.singletonList(id),
            Collections.emptyList());
    metadataAttributes.put(METADATA_ATTRIBUTE_NAME_ID, idAttribute);

    MetadataAttribute titleAttribute =
        new MetadataAttribute(
            METADATA_ATTRIBUTE_NAME_TITLE,
            METADATA_ATTRIBUTE_TYPE_STRING,
            Collections.singletonList(fileStatus.getPathSuffix()),
            Collections.emptyList());
    metadataAttributes.put(METADATA_ATTRIBUTE_NAME_TITLE, titleAttribute);

    MetadataAttribute createdAttribute =
        new MetadataAttribute(
            METADATA_ATTRIBUTE_NAME_CREATED,
            METADATA_ATTRIBUTE_TYPE_DATE,
            Collections.singletonList(modificationTime.toString()),
            Collections.emptyList());
    metadataAttributes.put(METADATA_ATTRIBUTE_NAME_CREATED, createdAttribute);

    MetadataAttribute modifiedAttribute =
        new MetadataAttribute(
            METADATA_ATTRIBUTE_NAME_MODIFIED,
            METADATA_ATTRIBUTE_TYPE_DATE,
            Collections.singletonList(modificationTime.toString()),
            Collections.emptyList());
    metadataAttributes.put(METADATA_ATTRIBUTE_NAME_MODIFIED, modifiedAttribute);

    MetadataAttribute resourceUriAttribute =
        new MetadataAttribute(
            METADATA_ATTRIBUTE_NAME_RESOURCE_URI,
            METADATA_ATTRIBUTE_TYPE_STRING,
            Collections.singletonList(fileUrl),
            Collections.emptyList());
    metadataAttributes.put(METADATA_ATTRIBUTE_NAME_RESOURCE_URI, resourceUriAttribute);

    MetadataAttribute lengthAttribute =
        new MetadataAttribute(
            METADATA_ATTRIBUTE_NAME_RESOURCE_SIZE,
            METADATA_ATTRIBUTE_TYPE_STRING,
            Collections.singletonList(String.valueOf(fileStatus.getLength())),
            Collections.emptyList());
    metadataAttributes.put(METADATA_ATTRIBUTE_NAME_RESOURCE_SIZE, lengthAttribute);

    Metadata metadata = new MetadataImpl(metadataAttributes, Map.class, id, modificationTime);

    try {
      metadata.setResourceUri(new URI(fileUrl));
      metadata.setResourceModified(modificationTime);
      metadata.setResourceSize(fileStatus.getLength());

      return metadata;
    } catch (URISyntaxException e) {
      throw new ReplicationException("Unable to create a URI from the file's URL.", e);
    }
  }

  /**
   * Using the file URL and modification time as input, a version-3 UUID is generated which is then
   * modified to appear as a version-4 UUID by changing the version number in the UUID from 3 to 4.
   * The URL is not guaranteed to be unique if a file is removed and replaced by another file with
   * the same URL. Because of this, the URL is combined with the modification time to serve as input
   * for UUID generation.
   *
   * @param fileUrl the full URL of the file
   * @param modificationTime modification time of the file
   * @return a {@code String} representing a version-4 UUID
   */
  private String getVersion4Uuid(String fileUrl, Date modificationTime) {
    String input = String.format("%s_%d", fileUrl, modificationTime.getTime());
    String version3Uuid = UUID.nameUUIDFromBytes(input.getBytes()).toString();
    StringBuilder version4Uuid = new StringBuilder(version3Uuid);
    version4Uuid.setCharAt(UUID_VERSION_INDEX, '4');

    LOGGER.debug("UUID for {} is {}", fileUrl, version4Uuid);
    return version4Uuid.toString();
  }

  /**
   * Formulates a GET request to send to the HDFS instance to retrieve a file listing. The request
   * is repeated to retrieve additional results if the file system contains more results than can be
   * returned in a single response.
   *
   * @param filterDate specifies a point in time such that only files more recent are returned
   * @return a resulting {@code List} of {@link FileStatus} objects meeting the criteria
   */
  List<FileStatus> getFilesToReplicate(@Nullable Date filterDate) {

    List<FileStatus> filesToReplicate = new ArrayList<>();
    AtomicInteger remainingEntries = new AtomicInteger();
    AtomicReference<String> pathSuffix = new AtomicReference<>();

    do {
      try {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add(HTTP_OPERATION_KEY, HTTP_OPERATION_LIST_STATUS_BATCH);

        if (pathSuffix.get() != null) {
          params.add(HTTP_START_AFTER_KEY, pathSuffix.get());
        }

        UriComponentsBuilder uriComponentsBuilder =
            UriComponentsBuilder.fromHttpUrl(getWebHdfsUrl().toString()).queryParams(params);

        ResponseEntity<String> response =
            restOperations.exchange(
                uriComponentsBuilder.toUriString(), HttpMethod.GET, null, String.class);

        HttpStatus httpStatus = response.getStatusCode();
        LOGGER.debug("Response contains status code: {}", httpStatus);

        if (httpStatus == HttpStatus.OK) {

          String content = response.getBody();
          ObjectMapper objectMapper = new ObjectMapper();

          IterativeDirectoryListing iterativeDirectoryListing =
              objectMapper.readValue(content, IterativeDirectoryListing.class);

          DirectoryListing directoryListing = iterativeDirectoryListing.getDirectoryListing();

          remainingEntries.set(directoryListing.getRemainingEntries());

          List<FileStatus> results =
              directoryListing.getPartialListing().getFileStatuses().getFileStatusList();

          if (remainingEntries.intValue() > 0) {
            // start after pathSuffix of the last item for the next request
            pathSuffix.set(results.get(results.size() - 1).getPathSuffix());
          }

          filesToReplicate.addAll(getRelevantFiles(results, filterDate));

        } else {
          throw new ReplicationException(
              String.format(
                  "List Status Batch request failed with status code: %d",
                  response.getStatusCodeValue()));
        }

      } catch (JsonProcessingException e) {
        LOGGER.error("Error processing JSON.", e);
        return filesToReplicate;
      }
    } while (remainingEntries.intValue() > 0);

    LOGGER.info("Identified {} files to replicate.", filesToReplicate.size());
    return filesToReplicate;
  }

  /**
   * Returns the files meeting the criteria for replication by removing elements that: 1) are not of
   * type FILE or 2) have a modification time before or equal to the filter date, when the filter
   * date is specified
   *
   * @param files a {@code List} of all {@link FileStatus} objects returned by the GET request
   * @param filterDate specifies a point in time such that only files more recent are included; this
   *     value will be set to {@code null} during the first execution of replication
   * @return a resulting {@code List} of {@link FileStatus} objects meeting the criteria
   */
  List<FileStatus> getRelevantFiles(List<FileStatus> files, @Nullable Date filterDate) {
    files.removeIf(
        file ->
            !file.isFile()
                || (filterDate != null && !file.getModificationTime().after(filterDate)));

    return files;
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

  private AdapterException wrapException(String msg, Exception e) {
    Throwable t = e;

    if (t instanceof UndeclaredThrowableException) {
      t = ((UndeclaredThrowableException) t).getUndeclaredThrowable();
    }
    if (t instanceof InterruptedIOException) {
      return new AdapterInterruptedException((InterruptedIOException) t);
    } else if (t instanceof InterruptedException) {
      return new AdapterInterruptedException((InterruptedException) t);
    } else if (t instanceof AdapterInterruptedException) {
      return (AdapterInterruptedException) t;
    } else if (t instanceof Error) {
      throw (Error) t;
    } else if (t instanceof AdapterException) {
      // re-write the exception with our message while preserving its stack trace
      final AdapterException ae = new AdapterException(msg, t.getCause());

      ae.setStackTrace(t.getStackTrace());
      throw ae;
    }
    return new AdapterException(msg, (Exception) t);
  }

  @Override
  public void close() throws IOException {}
}
