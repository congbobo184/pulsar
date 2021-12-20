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
package org.apache.pulsar.io.telnet.connection;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.telnet.TelnetClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Netty Server to accept incoming data via the configured type.
 */
@Slf4j
public class TelnetConnection extends TelnetConnectionState {

    private final TelnetClient client;
    private final String host;
    private final int port;
    private final String termType;
    private final String user;
    private final String password;
    private volatile InputStream inputStream;
    private volatile OutputStream outputStream;


    private TelnetConnection(Builder builder) {
        super(State.None);
        this.termType = builder.termType;
        this.host = builder.host;
        this.port = builder.port;
        this.user = builder.user;
        this.password = builder.password;
        this.client = new TelnetClient(termType);
    }

    public void connect() throws Exception {
        if (changeToConnectingState()) {
            try {
                this.client.connect(host, port);
                this.inputStream = this.client.getInputStream();
                this.outputStream = new PrintStream(this.client.getOutputStream());
                if (StringUtils.isNotBlank(user)) {
                    write(user);
                    write("\n");
                }

                if (StringUtils.isNotBlank(password)) {
                    write(password);
                }
                changeToConnectedState();

                byte[] bytes = new byte[4];

                read(bytes);
                String str = new String(bytes);
                System.out.println(str);
            } catch (Exception e) {
                log.error("Telnet connector connect error! host: {}, port: {}", host, port, e);
                changeToFailState();
                throw e;
            }
        } else {
            log.warn("Telnet connector are in connecting or closed state! don't reconnect again");
        }
    }

    public void read(byte[] bytes) throws IOException {
        inputStream.read(bytes);
    }

    public void write(String s) {
        try {
            outputStream.write(s.getBytes());
            outputStream.flush();
        } catch (Exception e) {
            log.error("Telnet connector connect write fail! host: {}, port: {}", host, port, e);
        }
    }

    public static class Builder {

        private String termType = "VT100";
        private String host;
        private int port = -1;
        private String user;
        private String password;

        public Builder setTermType(String termType) {
            this.termType = termType;
            return this;
        }

        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setUser(String user) {
            this.user = user;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public TelnetConnection build() {
            checkArgument(StringUtils.isNotBlank(host), "host cannot be blank/null");
            checkArgument(this.port >= 0, "port must be set equal or bigger than 0");

            return new TelnetConnection(this);
        }
    }

    public void shutdownGracefully() throws IOException {
        if (this.client != null) {
            this.client.disconnect();
        }
    }
}
