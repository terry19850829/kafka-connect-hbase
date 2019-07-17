/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.svectors.hbase.parser;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author ravi.magham
 */
public class TestAvroEventParser {

    private AvroEventParser eventParser;

    @Before
    public void setup() {
        eventParser = new AvroEventParser();
    }

    @Test
    public void testParseValue() {
        final Schema valueSchema = SchemaBuilder.struct().name("record").version(1)
                .field("url", Schema.STRING_SCHEMA)
                .field("id", Schema.INT32_SCHEMA)
                .field("zipcode", Schema.INT32_SCHEMA)
                .field("status", Schema.BOOLEAN_SCHEMA)
                .field("age", Schema.INT8_SCHEMA)
                .field("ctime", Schema.INT64_SCHEMA)
                .build();

        String url = "google.com";
        int id = 1;
        int zipcode = 95051;
        boolean status = true;
        byte age = 12;
        long ctime = 1513934134060L;

        final Struct record = new Struct(valueSchema)
                .put("url", url)
                .put("id", id)
                .put("zipcode", zipcode)
                .put("status", status)
                .put("age", age)
                .put("ctime", ctime);


        final SinkRecord sinkRecord = new SinkRecord("test", 0, null, null, valueSchema, record, 0);

        Map<String, byte[]> result = eventParser.parseValue(sinkRecord);
        Assert.assertEquals(6, result.size());
        Assert.assertEquals(url, Bytes.toString(result.get("url")));
        Assert.assertEquals(id, Bytes.toInt(result.get("id")));
        Assert.assertEquals(zipcode, Bytes.toInt(result.get("zipcode")));
        Assert.assertEquals(status, Bytes.toBoolean(result.get("status")));
        Assert.assertEquals(age, Bytes.toInt(result.get("age")));
        Assert.assertEquals(ctime, Bytes.toLong(result.get("ctime")));
    }

    @Test
    public void testParseNullKey() {
        final SinkRecord sinkRecord = new SinkRecord("test", 0, null, null, null, null, 0);
        final Map<String, byte[]> keys = eventParser.parseKey(sinkRecord);
        Assert.assertTrue(keys.isEmpty());
    }
}
