/**
 * Copyright (c) 2013, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.science.avro.common;

import com.google.common.collect.ImmutableList;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.codehaus.jackson.node.NullNode;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class JsonConverterTest {

  private static Schema.Field sf(String name, Schema schema) {
    return new Schema.Field(name, schema, "", null);
  }

  private static Schema.Field sfnull(String name, Schema schema) {
    return new Schema.Field(name, schema, "", NullNode.getInstance());
  }

  private static Schema.Field sf(String name, Schema.Type type) {
    return sf(name, sc(type));
  }

  private static Schema sc(Schema.Type type) {
    return Schema.create(type);
  }

  private static Schema sr(Schema.Field... fields) {
    return Schema.createRecord(Arrays.asList(fields));
  }

  Schema.Field f1 = sf("field1", Type.LONG);
  Schema.Field f2 = sf("field2", Schema.createArray(sc(Type.BOOLEAN)));
  Schema.Field f3Map = sf("field3", Schema.createMap(sc(Type.STRING)));
  Schema.Field f3Rec = sf("field3", sr(sf("key", Type.STRING)));
  MockQualityReporter qr = new MockQualityReporter();

  @Before
  public void setUp() throws Exception {
    qr.reset();
  }

  @Test
  public void testBasicWithMap() throws Exception {
    JsonConverter jc = new JsonConverter(sr(f1, f2, f3Map), qr);
    String json = "{\"field1\": 1729, \"field2\": [true, true, false], \"field3\": {\"key\": \"value\"}}";
    GenericRecord r = jc.convert(json);
    assertEquals(json, r.toString());
    assertEquals(0L, qr.get(QualityReporter.TOP_LEVEL_GROUP, QualityReporter.TL_COUNTER_RECORD_HAS_MISSING_FIELDS));
  }

  @Test
  public void testBasicWithRecord() throws Exception {
    JsonConverter jc = new JsonConverter(sr(f1, f2, f3Rec), qr);
    String json = "{\"field1\": 1729, \"field2\": [true, true, false], \"field3\": {\"key\": \"value\"}}";
    GenericRecord r = jc.convert(json);
    assertEquals(json, r.toString());
    assertEquals(0L, qr.get(QualityReporter.TOP_LEVEL_GROUP, QualityReporter.TL_COUNTER_RECORD_HAS_MISSING_FIELDS));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testMissingRequiredField() throws Exception {
    JsonConverter jc = new JsonConverter(sr(f1, f2, f3Rec), qr);
    String json = "{\"field2\": [true, true, false], \"field3\": {\"key\": \"value\"}}";
    jc.convert(json);
  }

  @Test
  public void testMissingNullableField() throws Exception {
    Schema optional = Schema.createUnion(
            ImmutableList.of(Schema.create(Type.NULL), Schema.create(Type.DOUBLE)));
    Schema.Field f4 = sf("field4", optional);
    JsonConverter jc = new JsonConverter(sr(f1, f2, f3Rec, f4), qr);
    String json = "{\"field1\": 1729, \"field2\": [true, true, false], \"field3\": {\"key\": \"value\"}}";
    GenericRecord r = jc.convert(json);
    String expect = "{\"field1\": 1729, \"field2\": [true, true, false], \"field3\": {\"key\": \"value\"}, \"field4\": null}";
    assertEquals(expect, r.toString());
    assertEquals(1L, qr.get(QualityReporter.TOP_LEVEL_GROUP, QualityReporter.TL_COUNTER_RECORD_HAS_MISSING_FIELDS));
    assertEquals(0L, qr.get(QualityReporter.FIELD_MISSING_GROUP, "field1"));
    assertEquals(1L, qr.get(QualityReporter.FIELD_MISSING_GROUP, "field4"));
  }

  @Test
  public void testTreatNullAsMissing() throws Exception {
    Schema optional = Schema.createUnion(
            ImmutableList.of(Schema.create(Type.NULL), Schema.create(Type.DOUBLE)));
    Schema.Field f4 = sf("field4", optional);
    JsonConverter jc = new JsonConverter(sr(f1, f2, f4), qr);
    String json = "{\"field1\": 1729, \"field2\": [true, true, false]}";
    GenericRecord r = jc.convert(json);
    String expect = "{\"field1\": 1729, \"field2\": [true, true, false], \"field4\": null}";
    assertEquals(expect, r.toString());
    assertEquals(1L, qr.get(QualityReporter.TOP_LEVEL_GROUP, QualityReporter.TL_COUNTER_RECORD_HAS_MISSING_FIELDS));
    assertEquals(0L, qr.get(QualityReporter.FIELD_MISSING_GROUP, "field1"));
    assertEquals(1L, qr.get(QualityReporter.FIELD_MISSING_GROUP, "field4"));
  }

  @Test
  public void testNullDefaultValue() throws Exception {
    Schema optional = Schema.createUnion(
            ImmutableList.of(Schema.create(Type.NULL), Schema.create(Type.STRING)));
    Schema.Field f4 = sfnull("field4", optional);
    JsonConverter jc = new JsonConverter(sr(f1, f2, f4), qr);
    String json = "{\"field1\": 1729, \"field2\": [true, true, false]}";
    GenericRecord r = jc.convert(json);
    String expect = "{\"field1\": 1729, \"field2\": [true, true, false], \"field4\": null}";
    assertEquals(expect, r.toString());
    assertEquals(1L, qr.get(QualityReporter.TOP_LEVEL_GROUP, QualityReporter.TL_COUNTER_RECORD_HAS_MISSING_FIELDS));
    assertEquals(0L, qr.get(QualityReporter.FIELD_MISSING_GROUP, "field1"));
    assertEquals(1L, qr.get(QualityReporter.FIELD_MISSING_GROUP, "field4"));
  }

  @Test
  public void testConvertWithEnums() throws IOException {
    String strJson = "{\"header\":{\"schemaRevision\":\"5.0.0.995\",\"eventTimeStamp\":1512389511369,\"eventUniqueId\":\"NjYAx2CXRNCddkd29XgbUg\",\"globalSessionId\":\"\",\"globalUserId\":\"19bda4e85a77f2009d3b3d1b16a0f1f977934a17aff39eb4e902b51ca904a968\",\"accountId\":\"myAccount\",\"encrypted\":\"NONE\",\"platform\":\"AMS\",\"component\":\"AMS_USER_PROFILE\",\"eventType\":\"UserProfileUpdateEvent\"},\"participants\":[{\"participant\":\"CONSUMER\",\"header\":{\"participantDetails\":{\"consumerName\":\"MyConsumerName\",\"userDetails\":{\"userId\":\"testUserId\",\"connections\":[{\"appName\":\"MyApplication\",\"appVersion\":\"2.9.3.0\",\"deviceFamilyType\":\"MOBILE\",\"userAgent\":null,\"IP\":\"127.0.0.1\",\"OS\":\"IOS\",\"osVersion\":\"11.1\",\"integration\":\"MOBILE_SDK\",\"integrationVersion\":\"3.0.0.1\",\"timeZone\":\"sun.util.calendar.ZoneInfo[id=\\\"Asia/Jerusalem\\\",offset=7200000,dstSavings=3600000,useDaylight=true,transitions=143,lastRule=java.util.SimpleTimeZone[id=Asia/Jerusalem,offset=7200000,dstSavings=3600000,useDaylight=true,startYear=0,startMode=3,startMonth=2,startDay=23,startDayOfWeek=6,startTime=7200000,startTimeMode=0,endMode=2,endMonth=9,endDay=-1,endDayOfWeek=1,endTime=7200000,endTimeMode=0]]\",\"browser\":null,\"browserVersion\":null}]}}}}],\"platformSpecificHeader\":{\"header\":{\"node\":\"testNode\",\"version\":\"3.7.0.0-SNAPSHOT\",\"userId\":\"testUserId\"}},\"auditingHeader\":{\"hostname\":\"testHost\",\"bulkId\":1512389400000,\"sequenceId\":1081},\"metaDataHeader\":null,\"producerHeader\":null,\"eventBody\":{\"body\":{\"userData\":{\"firstName\":\"MyFirstName\",\"lastName\":\"MyLastName\",\"token\":null,\"phone\":\"0\",\"email\":null,\"avatarUrl\":{\"url\":\"\"},\"backgroungImgurl\":null,\"pnData\":{\"pnServiceName\":\"\",\"pnCertName\":\"\",\"pnToken\":\"\"}},\"participantType\":\"CONSUMER\"}}}";
    Schema schema = getSchema("Event.avcs");
    JsonConverter converter = new JsonConverter(schema, qr);
    GenericRecord record = converter.convert(strJson);
    assertEquals(schema, record.getSchema());
    GenericData.Record header = (GenericData.Record) record.get("header");
    assertEquals("5.0.0.995", header.get("schemaRevision"));
    GenericData.EnumSymbol eventTypeEnumItem = (GenericData.EnumSymbol) header.get("eventType");
    assertEquals("UserProfileUpdateEvent",eventTypeEnumItem.toString());

    List participants =  (List)record.get("participants");
    assertEquals(1,participants.size());
    GenericData.Record participantRecord = (GenericData.Record) participants.get(0);
    GenericData.EnumSymbol participant = (GenericData.EnumSymbol)participantRecord.get("participant");
    assertEquals("CONSUMER", participant.toString());
  }

  @Test
  public void testConvertWithSuitableItemInSchema() throws IOException {
    String strJson = "{\"header\":{\"schemaRevision\":\"5.0.0.995\",\"eventTimeStamp\":1512170029821,\"eventUniqueId\":\"tno8kTnyR9i0HQ620sTVXg\",\"globalSessionId\":\"\",\"globalUserId\":\"b7abc815-12ec-46a0-8be2-4f2c8de283a6\",\"accountId\":\"le62343885\",\"encrypted\":\"NONE\",\"platform\":\"AMS\",\"component\":\"AMS_CM_GENERAL\",\"eventType\":\"ParticipantAddEvent\"},\"participants\":[{\"participant\":\"CONSUMER\",\"header\":{\"participantDetails\":{\"consumerName\":\"Consumer1_fn Consumer1_ln\",\"userDetails\":{\"userId\":\"b7abc815-12ec-46a0-8be2-4f2c8de283a6\",\"connections\":[{\"appName\":\"\",\"appVersion\":null,\"deviceFamilyType\":null,\"userAgent\":null,\"IP\":\"127.0.0.1\",\"OS\":null,\"osVersion\":null,\"integration\":null,\"integrationVersion\":null,\"timeZone\":null,\"browser\":null,\"browserVersion\":null}]}}}}],\"platformSpecificHeader\":{\"header\":{\"node\":\"qtvr-jet1001.tlv.lpnet.com\",\"version\":\"3.7.0.0-SNAPSHOT\",\"userId\":\"b7abc815-12ec-46a0-8be2-4f2c8de283a6\"}},\"auditingHeader\":{\"hostname\":\"1@6856@qtvr-jet1001.tlv.lpnet.com\",\"bulkId\":1512169800000,\"sequenceId\":349},\"metaDataHeader\":null,\"producerHeader\":null,\"eventBody\":{\"body\":{\"convDetails\":{\"convId\":\"22dd03bb-0efb-463c-8fb7-f1bfd8007e0c\",\"dialogId\":null,\"skillId\":-1,\"interactiveTime\":-1,\"isConsumerActive\":false,\"startTime\":1512170029810,\"lastUpdated\":1512170029810,\"convState\":\"OPEN\",\"firstConversation\":\"TRUE\",\"lpContext\":null,\"campaignContext\":{\"campaignId\":874454,\"engagementId\":532086,\"goalId\":-1},\"interactionType\":\"ASYNC\",\"integrationSourceOrigin\":\"APP\",\"agentGroupId\":null,\"assignedAgentId\":null}}}}";
    Schema schema = getSchema("Event.avcs");
    JsonConverter converter = new JsonConverter(schema, qr);
    GenericRecord record = converter.convert(strJson, "ParticipantAddEvent");
    assertEquals(schema, record.getSchema());
    GenericData.Record header = (GenericData.Record) record.get("header");
    assertEquals("5.0.0.995", header.get("schemaRevision"));
    GenericData.EnumSymbol eventTypeEnumItem = (GenericData.EnumSymbol) header.get("eventType");
    assertEquals("ParticipantAddEvent",eventTypeEnumItem.toString());
    assertEquals(1512170029821L, header.get("eventTimeStamp"));
    assertEquals("tno8kTnyR9i0HQ620sTVXg", header.get("eventUniqueId"));
    assertEquals("", header.get("globalSessionId"));
    assertEquals("b7abc815-12ec-46a0-8be2-4f2c8de283a6", header.get("globalUserId"));
    assertEquals("le62343885", header.get("accountId"));
    GenericData.EnumSymbol encryptedEnumItem = (GenericData.EnumSymbol) header.get("encrypted");
    assertEquals("NONE", encryptedEnumItem.toString());
    assertEquals("AMS", header.get("platform").toString());
    assertEquals("AMS_CM_GENERAL", header.get("component").toString());

    List participants =  (List)record.get("participants");
    assertEquals(1,participants.size());
    GenericData.Record participantRecord = (GenericData.Record) participants.get(0);
    GenericData.EnumSymbol participant = (GenericData.EnumSymbol)participantRecord.get("participant");
    assertEquals("CONSUMER", participant.toString());
    GenericData.Record participantHeader = (GenericData.Record)participantRecord.get("header");
    GenericData.Record participantDetails = (GenericData.Record)participantHeader.get("participantDetails");
    assertEquals("Consumer1_fn Consumer1_ln", participantDetails.get("consumerName"));
    GenericData.Record userDetails = (GenericData.Record)participantDetails.get("userDetails");
    assertEquals("b7abc815-12ec-46a0-8be2-4f2c8de283a6", userDetails.get("userId"));
    List connections = (List)userDetails.get("connections");
    assertEquals(1, connections.size());
  }

  private  Schema getSchema(String fileName){

    File schemaFile = new File(getClass().getClassLoader().getResource(fileName).getFile());
    Schema schema = null;
    try {
      schema = new Schema.Parser().parse(schemaFile);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return schema;
  }

  @Test(expected = IOException.class)
  public void testConvertWithoutSuitableItemInSchema() throws IOException {
    String strJson = "{\"header\":{\"schemaRevision\":\"5.0.0.995\",\"eventTimeStamp\":1512170029821,\"eventUniqueId\":\"tno8kTnyR9i0HQ620sTVXg\",\"globalSessionId\":\"\",\"globalUserId\":\"b7abc815-12ec-46a0-8be2-4f2c8de283a6\",\"accountId\":\"le62343885\",\"encrypted\":\"NONE\",\"platform\":\"AMS\",\"component\":\"AMS_CM_GENERAL\",\"eventType\":\"ParticipantAddEvent\"},\"participants\":[{\"participant\":\"CONSUMER\",\"header\":{\"participantDetails\":{\"consumerName\":\"Consumer1_fn Consumer1_ln\",\"userDetails\":{\"userId\":\"b7abc815-12ec-46a0-8be2-4f2c8de283a6\",\"connections\":[{\"appName\":\"\",\"appVersion\":null,\"deviceFamilyType\":null,\"userAgent\":null,\"IP\":\"127.0.0.1\",\"OS\":null,\"osVersion\":null,\"integration\":null,\"integrationVersion\":null,\"timeZone\":null,\"browser\":null,\"browserVersion\":null}]}}}}],\"platformSpecificHeader\":{\"header\":{\"node\":\"qtvr-jet1001.tlv.lpnet.com\",\"version\":\"3.7.0.0-SNAPSHOT\",\"userId\":\"b7abc815-12ec-46a0-8be2-4f2c8de283a6\"}},\"auditingHeader\":{\"hostname\":\"1@6856@qtvr-jet1001.tlv.lpnet.com\",\"bulkId\":1512169800000,\"sequenceId\":349},\"metaDataHeader\":null,\"producerHeader\":null,\"eventBody\":{\"body\":{\"convDetails\":{\"convId\":\"22dd03bb-0efb-463c-8fb7-f1bfd8007e0c\",\"dialogId\":null,\"skillId\":-1,\"interactiveTime\":-1,\"isConsumerActive\":false,\"startTime\":1512170029810,\"lastUpdated\":1512170029810,\"convState\":\"OPEN\",\"firstConversation\":\"TRUE\",\"lpContext\":null,\"campaignContext\":{\"campaignId\":874454,\"engagementId\":532086,\"goalId\":-1},\"interactionType\":\"ASYNC\",\"integrationSourceOrigin\":\"APP\",\"agentGroupId\":null,\"assignedAgentId\":null}}}}";
    Schema schema = getSchema("Event.avcs");
    JsonConverter converter = new JsonConverter(schema, qr);
    GenericRecord record = converter.convert(strJson);
  }


}