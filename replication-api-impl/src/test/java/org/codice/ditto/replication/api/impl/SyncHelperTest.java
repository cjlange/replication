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
package org.codice.ditto.replication.api.impl;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ddf.catalog.content.operation.CreateStorageRequest;
import ddf.catalog.content.operation.UpdateStorageRequest;
import ddf.catalog.data.impl.MetacardImpl;
import ddf.catalog.data.impl.ResultImpl;
import ddf.catalog.data.types.Core;
import ddf.catalog.filter.FilterBuilder;
import ddf.catalog.filter.proxy.builder.GeotoolsFilterBuilder;
import ddf.catalog.operation.CreateRequest;
import ddf.catalog.operation.CreateResponse;
import ddf.catalog.operation.DeleteRequest;
import ddf.catalog.operation.QueryRequest;
import ddf.catalog.operation.SourceResponse;
import ddf.catalog.operation.UpdateRequest;
import ddf.catalog.operation.impl.ProcessingDetailsImpl;
import ddf.security.impl.SubjectImpl;
import java.net.URI;
import java.util.Collections;
import java.util.Date;
import java.util.Optional;
import org.apache.shiro.util.ThreadContext;
import org.codice.ditto.replication.api.ReplicationItemManager;
import org.codice.ditto.replication.api.ReplicationStatus;
import org.codice.ditto.replication.api.ReplicationStore;
import org.codice.ditto.replication.api.ReplicatorHistory;
import org.codice.ditto.replication.api.Status;
import org.codice.ditto.replication.api.data.ReplicatorConfig;
import org.codice.ditto.replication.api.impl.data.ReplicationStatusImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
public class SyncHelperTest {

  SyncHelper helper;

  @Mock ReplicationStore source;
  @Mock ReplicationStore destination;
  @Mock ReplicatorConfig config;
  @Mock ReplicationItemManager persistentStore;
  @Mock ReplicatorHistory history;

  ReplicationStatus status;

  @Before
  public void setUp() throws Exception {
    FilterBuilder builder = new GeotoolsFilterBuilder();
    status = new ReplicationStatusImpl("test");
    when(source.getRemoteName()).thenReturn("local");
    when(destination.getRemoteName()).thenReturn("remote");
    helper = new SyncHelper(source, destination, config, status, persistentStore, history, builder);
  }

  // verify a replication with no failures to retry doesn't try to query DDF with an empty list of
  // Ids
  @Test
  public void noFailureIds() throws Exception {
    setConfigDefaults();
    SourceResponse response = mock(SourceResponse.class);
    when(response.getResults()).thenReturn(Collections.emptyList());
    when(source.query(any(QueryRequest.class))).thenReturn(response);
    when(persistentStore.getFailureList(anyInt(), anyString(), anyString()))
        .thenReturn(Collections.emptyList());
    when(history.getReplicationEvents("test")).thenReturn(Collections.emptyList());
    helper.sync();

    // if only one query is made then we skipped the failedItems query and the test passes
    verify(source, times(1)).query(any(QueryRequest.class));
  }

  // verify we don't continue trying to replicate if we lose connection during the attempt to retry
  // failed items
  @Test
  public void connectionLostDuringFailedItemQuery() throws Exception {
    setConfigDefaults();
    SourceResponse response = mock(SourceResponse.class);
    when(response.getResults()).thenReturn(Collections.emptyList());
    when(persistentStore.getFailureList(anyInt(), anyString(), anyString()))
        .thenReturn(Collections.singletonList("someId"));
    when(source.query(any(QueryRequest.class)))
        .then(
            new Answer() {
              public Object answer(InvocationOnMock invocation) {
                status.setStatus(Status.CONNECTION_LOST);
                return response;
              }
            });
    // only called if we build the change set filter, which we shouldn't
    verify(history, never()).getReplicationEvents(any(String.class));
  }

  @Test
  public void canceledDuringFailedItemQuery() throws Exception {
    setConfigDefaults();
    SourceResponse response = mock(SourceResponse.class);
    when(response.getResults()).thenReturn(Collections.emptyList());
    when(persistentStore.getFailureList(anyInt(), anyString(), anyString()))
        .thenReturn(Collections.singletonList("someId"));
    when(source.query(any(QueryRequest.class)))
        .then(
            new Answer() {
              public Object answer(InvocationOnMock invocation) {
                helper.cancel();
                return response;
              }
            });
    // only called if we build the change set filter, which we shouldn't
    verify(history, never()).getReplicationEvents(any(String.class));
  }

  @Test
  public void cancel() throws Exception {
    helper.cancel();
    setConfigDefaults();
    SourceResponse response = mock(SourceResponse.class);
    MetacardImpl mcard = new MetacardImpl();
    mcard.setId("id");
    when(response.getResults()).thenReturn(Collections.singletonList(new ResultImpl(mcard)));
    when(source.query(any(QueryRequest.class))).thenReturn(response);
    when(persistentStore.getFailureList(anyInt(), anyString(), anyString()))
        .thenReturn(Collections.emptyList());
    when(history.getReplicationEvents("test")).thenReturn(Collections.emptyList());
    helper.sync();
    verify(destination, never()).create(any(CreateRequest.class));
    verify(destination, never()).create(any(CreateStorageRequest.class));
    verify(destination, never()).update(any(UpdateRequest.class));
    verify(destination, never()).update(any(UpdateStorageRequest.class));
    verify(destination, never()).delete(any(DeleteRequest.class));
  }

  @Test
  public void testSyncCreate() throws Exception {
    SubjectImpl subject = mock(SubjectImpl.class);
    ThreadContext.bind(subject);
    CreateResponse createResponse = mock(CreateResponse.class);
    when(createResponse.getProcessingErrors()).thenReturn(Collections.emptySet());
    setConfigDefaults();
    when(destination.create(any(CreateRequest.class))).thenReturn(createResponse);
    SourceResponse response = mock(SourceResponse.class);
    MetacardImpl mcard = new MetacardImpl();
    mcard.setId("id");
    mcard.setModifiedDate(new Date());
    mcard.setAttribute(Core.METACARD_MODIFIED, new Date());
    when(response.getResults()).thenReturn(Collections.singletonList(new ResultImpl(mcard)));
    when(source.query(any(QueryRequest.class))).thenReturn(response);
    when(persistentStore.getFailureList(anyInt(), anyString(), anyString()))
        .thenReturn(Collections.emptyList());
    when(persistentStore.getItem(anyString(), anyString(), anyString()))
        .thenReturn(Optional.empty());
    when(history.getReplicationEvents("test")).thenReturn(Collections.emptyList());
    helper.sync();
    assertThat(status.getPushCount(), is(1L));
    assertThat(status.getStatus(), is(Status.SUCCESS));
  }

  @Test
  public void testSyncCreateMetadataOnly() throws Exception {
    SubjectImpl subject = mock(SubjectImpl.class);
    ThreadContext.bind(subject);
    CreateResponse createResponse = mock(CreateResponse.class);
    when(createResponse.getProcessingErrors()).thenReturn(Collections.emptySet());
    setConfigDefaults();
    when(destination.create(any(CreateRequest.class))).thenReturn(createResponse);
    SourceResponse response = mock(SourceResponse.class);
    MetacardImpl mcard = new MetacardImpl();
    mcard.setId("id");
    mcard.setModifiedDate(new Date());
    mcard.setAttribute(Core.METACARD_MODIFIED, new Date());
    mcard.setResourceURI(URI.create("https://host/resource"));
    when(response.getResults()).thenReturn(Collections.singletonList(new ResultImpl(mcard)));
    when(source.query(any(QueryRequest.class))).thenReturn(response);
    when(persistentStore.getFailureList(anyInt(), anyString(), anyString()))
        .thenReturn(Collections.emptyList());
    when(persistentStore.getItem(anyString(), anyString(), anyString()))
        .thenReturn(Optional.empty());
    when(history.getReplicationEvents("test")).thenReturn(Collections.emptyList());
    when(config.isMetadataOnly()).thenReturn(true);
    helper.sync();
    assertThat(status.getPushCount(), is(1L));
    assertThat(status.getStatus(), is(Status.SUCCESS));
    verify(destination, never()).create(any(CreateStorageRequest.class));
  }

  @Test
  public void testSyncItemFailure() throws Exception {
    SubjectImpl subject = mock(SubjectImpl.class);
    ThreadContext.bind(subject);
    CreateResponse createResponse = mock(CreateResponse.class);
    when(createResponse.getProcessingErrors())
        .thenReturn(Collections.singleton(new ProcessingDetailsImpl()));
    setConfigDefaults();
    when(destination.create(any(CreateRequest.class))).thenReturn(createResponse);
    when(destination.isAvailable()).thenReturn(true);
    SourceResponse response = mock(SourceResponse.class);
    MetacardImpl mcard = new MetacardImpl();
    mcard.setId("id");
    mcard.setModifiedDate(new Date());
    mcard.setAttribute(Core.METACARD_MODIFIED, new Date());
    when(response.getResults()).thenReturn(Collections.singletonList(new ResultImpl(mcard)));
    when(source.query(any(QueryRequest.class))).thenReturn(response);
    when(source.isAvailable()).thenReturn(true);
    when(persistentStore.getFailureList(anyInt(), anyString(), anyString()))
        .thenReturn(Collections.emptyList());
    when(persistentStore.getItem(anyString(), anyString(), anyString()))
        .thenReturn(Optional.empty());
    when(history.getReplicationEvents("test")).thenReturn(Collections.emptyList());
    helper.sync();
    assertThat(status.getPushFailCount(), is(1L));
    assertThat(status.getStatus(), is(Status.SUCCESS));
  }

  @Test
  public void isCanceled() {
    assertThat(helper.isCanceled(), is(false));
    helper.cancel();
    assertThat(helper.isCanceled(), is(true));
  }

  private void setConfigDefaults() {
    when(config.getId()).thenReturn("1234");
    when(config.getName()).thenReturn("test");
    when(config.getFilter()).thenReturn("title like '*'");
    when(config.getFailureRetryCount()).thenReturn(5);
  }
}