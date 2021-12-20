/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.telnet;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * Netty Source Connector Config.
 */
@Data
@Accessors(chain = true)
public class TelnetSourceConfig implements Serializable {

    private static final long serialVersionUID = -7116130435021510496L;

    @FieldDoc(
            required = true,
            defaultValue = "VT100",
            help = "The network protocol to use")
    private String termType = "VT100";

    @FieldDoc(
            required = true,
            defaultValue = "127.0.0.1",
            help = "The host name or address that the source instance to listen on")
    private String host = "127.0.0.1";

    @FieldDoc(
            required = true,
            defaultValue = "10999",
            help = "The port that the source instance to listen on")
    private int port = 10999;

    @FieldDoc(
            required = true,
            defaultValue = "null",
            help = "The user of this connect")
    private String user = null;

    @FieldDoc(
            required = true,
            defaultValue = "null",
            help = "The password of this connect")
    private String password = null;

    @FieldDoc(
            required = true,
            defaultValue = "null",
            help = "The request of this connector")
    private String request = null;

    @FieldDoc(
            required = true,
            defaultValue = "null",
            help = "The request of this connector")
    private String topic = null;

    public static TelnetSourceConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), TelnetSourceConfig.class);
    }

}
