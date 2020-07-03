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
package com.connexta.replication.adapters.ddf.csw;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.*;

import com.connexta.replication.adapters.ddf.DdfMetadata;
import com.connexta.replication.api.AdapterException;
import com.connexta.replication.api.Replication;
import com.connexta.replication.api.data.Metadata;
import com.connexta.replication.data.MetadataAttribute;
import com.connexta.replication.data.MetadataImpl;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.xml.XppReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Before;
import org.junit.Test;
import org.xmlpull.v1.XmlPullParserFactory;

public class MetacardMarshallerTest {

  private static final String expectedMetacardXml =
      "<metacard xmlns=\"urn:catalog:metacard\" xmlns:gml=\"http://www.opengis.net/gml\" xmlns:xlink=\"http://www.w3.org/1999/xlink\" xmlns:smil=\"http://www.w3.org/2001/SMIL20/\" xmlns:smillang=\"http://www.w3.org/2001/SMIL20/Language\" gml:id=\"123456789\">\n  <type>%s</type>\n  <source>source-id</source>\n  <stringxml name=\"metadata\">\n    <value><test><data>testing</data></test></value>\n  </stringxml>\n  <string name=\"metacard-tags\">\n    <value>tag</value>\n  </string>\n  <geometry xmlns:ns2=\"http://some/namespace\" name=\"location\">\n    <value><ns2:point>123,456</ns2:point></value>\n  </geometry>\n</metacard>";

  private static final String minimalMetacardXml =
      "<metacard xmlns=\"urn:catalog:metacard\" xmlns:gml=\"http://www.opengis.net/gml\" xmlns:xlink=\"http://www.w3.org/1999/xlink\" xmlns:smil=\"http://www.w3.org/2001/SMIL20/\" xmlns:smillang=\"http://www.w3.org/2001/SMIL20/Language\" gml:id=\"123456789\">\n  <type>ddf.metacard</type>\n</metacard>";

  private Map<String, MetadataAttribute> map;

  @Before
  public void setUp() throws Exception {
    map = new HashMap<>();
    map.put(Constants.METACARD_ID, new MetadataAttribute(Constants.METACARD_ID, null, "123456789"));
    map.put("type", new MetadataAttribute("type", null, "my-type"));
    map.put("source", new MetadataAttribute("source", null, "source-id"));
    map.put(
        Constants.METACARD_TAGS, new MetadataAttribute(Constants.METACARD_TAGS, "string", "tag"));
    map.put(
        Constants.METACARD_MODIFIED,
        new MetadataAttribute(Constants.METACARD_MODIFIED, "date", "1234-01-02T03:04:05.060"));
    map.put(
        "location",
        new MetadataAttribute(
            "location",
            "geometry",
            Collections.singletonList("<ns2:point>123,456</ns2:point>"),
            Collections.singletonList("xmlns:ns2=http://some/namespace")));
    map.put(Replication.ORIGINS, new MetadataAttribute(Replication.ORIGINS, "string", "otherhost"));
    map.put(
        Constants.RESOURCE_URI, new MetadataAttribute(Constants.RESOURCE_URI, "string", "my:uri"));
    map.put(
        Constants.RESOURCE_SIZE, new MetadataAttribute(Constants.RESOURCE_SIZE, "long", "1234"));
    map.put(
        Constants.MODIFIED,
        new MetadataAttribute(Constants.MODIFIED, "date", "1234-01-01T03:04:05.060"));
    map.put(
        Constants.DERIVED_RESOURCE_URI,
        new MetadataAttribute(Constants.DERIVED_RESOURCE_URI, "string", "my:derived:uri"));
    map.put(
        Constants.DERIVED_RESOURCE_DOWNLOAD_URL,
        new MetadataAttribute(
            Constants.DERIVED_RESOURCE_DOWNLOAD_URL, "string", "http://host/path"));
  }

  @Test
  public void marshal() {
    Map<String, MetadataAttribute> map = new HashMap<>();

    map.put(Constants.METACARD_ID, new MetadataAttribute(Constants.METACARD_ID, null, "123456789"));
    map.put("type", new MetadataAttribute("type", null, "my-type"));
    map.put("source", new MetadataAttribute("source", null, "source-id"));
    map.put(
        Constants.METACARD_TAGS, new MetadataAttribute(Constants.METACARD_TAGS, "string", "tag"));
    map.put(
        "location",
        new MetadataAttribute(
            "location",
            "geometry",
            Collections.singletonList("<ns2:point>123,456</ns2:point>"),
            Collections.singletonList("xmlns:ns2=http://some/namespace")));
    map.put(
        "metadata",
        new MetadataAttribute("metadata", "stringxml", "<test><data>testing</data></test>"));
    map.put("not-marshaled", new MetadataAttribute("not-marshaled", null, "not-marshaled-value"));

    String metacardxml =
        MetacardMarshaller.marshal(
            new DdfMetadata(minimalMetacardXml, String.class, "123456789", new Date(1), map));
    assertThat(metacardxml, is(String.format(expectedMetacardXml, "my-type")));
  }

  @Test
  public void unmarshal() throws Exception {
    String metacardXml =
        MetacardMarshaller.marshal(
            new DdfMetadata("mcard metadata", String.class, "123456789", new Date(1), map));

    Object obj =
        MetacardMarshaller.unmarshal(metacardXml, MetacardMarshaller.metacardNamespaceMap());
    assertThat(obj, instanceOf(Metadata.class));
    DdfMetadata metadata = (DdfMetadata) obj;
    assertThat(metadata.getId(), is("123456789"));
    assertThat(metadata.getTags().iterator().next(), is("tag"));
    assertThat(metadata.getLineage().get(0), is("otherhost"));
    assertThat(
        metadata.getMetadataModified(),
        is(
            ISODateTimeFormat.dateOptionalTimeParser()
                .parseDateTime("1234-01-02T03:04:05.060")
                .toDate()));
    assertThat(metadata.isDeleted(), is(false));
    assertThat(metadata.getResourceUri().toString(), is("my:uri"));
    assertThat(metadata.getResourceSize(), is(1234L));
    assertThat(
        metadata.getResourceModified(),
        is(
            ISODateTimeFormat.dateOptionalTimeParser()
                .parseDateTime("1234-01-01T03:04:05.060")
                .toDate()));
    Map<String, MetadataAttribute> map = metadata.getAttributes();
    assertThat(map.containsKey(Constants.DERIVED_RESOURCE_URI), is(false));
    assertThat(map.containsKey(Constants.DERIVED_RESOURCE_DOWNLOAD_URL), is(false));
    assertThat(map.get("location").getValue(), is("<ns2:point>123,456</ns2:point>"));
    assertThat(map.get("location").getXmlns().get(0), is("xmlns:ns2=http://some/namespace"));
  }

  @Test
  public void marshalNoType() {
    Map<String, MetadataAttribute> map = new HashMap<>();

    map.put(Constants.METACARD_ID, new MetadataAttribute(Constants.METACARD_ID, null, "123456789"));
    map.put("source", new MetadataAttribute("source", null, "source-id"));
    map.put(
        Constants.METACARD_TAGS, new MetadataAttribute(Constants.METACARD_TAGS, "string", "tag"));
    map.put(
        "location",
        new MetadataAttribute(
            "location",
            "geometry",
            Collections.singletonList("<ns2:point>123,456</ns2:point>"),
            Collections.singletonList("xmlns:ns2=http://some/namespace")));
    map.put(
        "metadata",
        new MetadataAttribute("metadata", "stringxml", "<test><data>testing</data></test>"));
    map.put("not-marshaled", new MetadataAttribute("not-marshaled", null, "not-marshaled-value"));

    String metacardxml =
        MetacardMarshaller.marshal(
            new DdfMetadata(minimalMetacardXml, String.class, "123456789", new Date(1), map));
    assertThat(metacardxml, is(String.format(expectedMetacardXml, "ddf.metacard")));
  }

  @Test
  public void unmarshalDeletedRecord() throws Exception {
    map.put(
        Constants.VERSIONED_ON,
        new MetadataAttribute(Constants.VERSIONED_ON, "date", "1234-01-01T01:04:05.060"));
    map.put(
        Constants.VERSION_OF_ID,
        new MetadataAttribute(Constants.VERSION_OF_ID, "string", "987654321"));
    map.put(Constants.ACTION, new MetadataAttribute(Constants.ACTION, "string", "Deleted"));

    String metacardXml =
        MetacardMarshaller.marshal(
            new DdfMetadata("mcard Metadata", String.class, "123456789", new Date(1), map));

    InputStream inStream = new ByteArrayInputStream(metacardXml.getBytes());
    HierarchicalStreamReader reader =
        new XppReader(
            new InputStreamReader(inStream, StandardCharsets.UTF_8),
            XmlPullParserFactory.newInstance().newPullParser());
    Object obj = MetacardMarshaller.unmarshal(reader, null);
    assertThat(obj, instanceOf(Metadata.class));
    Metadata metadata = (Metadata) obj;
    assertThat(metadata.getId(), is("987654321"));
    assertThat(metadata.getTags().iterator().next(), is("tag"));
    assertThat(metadata.getLineage().get(0), is("otherhost"));
    assertThat(
        metadata.getMetadataModified(),
        is(
            ISODateTimeFormat.dateOptionalTimeParser()
                .parseDateTime("1234-01-01T01:04:05.060")
                .toDate()));
    assertThat(metadata.isDeleted(), is(true));
    assertThat(metadata.getResourceUri().toString(), is("my:uri"));
    assertThat(metadata.getResourceSize(), is(1234L));
    assertThat(
        metadata.getResourceModified(),
        is(
            ISODateTimeFormat.dateOptionalTimeParser()
                .parseDateTime("1234-01-01T03:04:05.060")
                .toDate()));
  }

  @Test(expected = AdapterException.class)
  public void unmarshalNoModifiedDate() throws Exception {
    map.remove(Constants.METACARD_MODIFIED);
    map.remove(Constants.MODIFIED);
    String metacardXml =
        MetacardMarshaller.marshal(
            new DdfMetadata(Map.of("mcard", "metadata"), Map.class, "123456789", new Date(1), map));

    InputStream inStream = new ByteArrayInputStream(metacardXml.getBytes());
    HierarchicalStreamReader reader =
        new XppReader(
            new InputStreamReader(inStream, StandardCharsets.UTF_8),
            XmlPullParserFactory.newInstance().newPullParser());
    MetacardMarshaller.unmarshal(reader, null);
  }

  @Test(expected = AdapterException.class)
  public void marshalInvalidMetadataType() {
    MetacardMarshaller.marshal(
        new MetadataImpl("bad metadata", String.class, "123456789", new Date(1)));
  }

  @Test
  public void marshalEmptyMap() {
    String metacardxml =
        MetacardMarshaller.marshal(
            new DdfMetadata(
                minimalMetacardXml,
                String.class,
                "123456789",
                new Date(1),
                new HashMap<String, MetadataAttribute>()));
    assertThat(metacardxml, is(minimalMetacardXml));
  }

  @Test
  public void convertToDateBadFormat() {
    assertThat(
        MetacardMarshaller.convertToDate(new MetadataAttribute("att", "string", "notadate")),
        is(nullValue()));
  }

  @Test
  public void convertToDateNullValue() {
    assertThat(MetacardMarshaller.convertToDate(null), is(nullValue()));
  }
}
