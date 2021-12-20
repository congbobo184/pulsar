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

import org.apache.commons.net.telnet.TelnetClient;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;

public class TestClass {

    public static void main(String[] args) throws IOException, InterruptedException {

        TelnetClient client = new TelnetClient();
        client.connect("10.45.49.235", 81);
        InputStream in = client.getInputStream();
        OutputStream outputStream = new PrintStream(client.getOutputStream());
        outputStream.write("get /42+X212MGPH0124+lin+administrator+01234567 /http/1.1\n".getBytes());
        outputStream.flush();
        byte[] bytes = new byte[1024];

        in.read(bytes);
        String str = new String(bytes);

        System.out.println(str);
        outputStream.write("get /21+X212MGPH0124+dat+0 /http/1.1".getBytes());
        outputStream.flush();
//        byte[] byte1 = new byte[20000];
//        in.read(byte1);
//        String str1 = new String(byte1);
//        System.out.println(str1);
        int unmber = 0;
        while (true) {
            byte[] byte1 = new byte[160];
            in.read(byte1);
            String str1 = new String(byte1);
            System.out.println("哈哈");
            System.out.println(str1);
//            if (str1.contains("ack")) {
//                System.out.println("哈哈");
//                break;
//            } else {
//                unmber += 20000;
//            }
        }
//        in.read(byte1);
//        String str1 = new String(byte1);
//        System.out.println(new String(byte1));
//
//        byte[] byte2 = new byte[5024];
//        in.read(byte2);
//        System.out.println(new String(byte2));
    }
}