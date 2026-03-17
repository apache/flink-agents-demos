/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.sample.jobs;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

public class TimestampDataGenerator implements GeneratorFunction<Long, Long> {
    private long currentTimestamp;
    private final long sleepMillis;

    public TimestampDataGenerator(long sleepMillis) {
        this(Long.MIN_VALUE, sleepMillis);
    }

    public TimestampDataGenerator(long currentTimestamp, long sleepMillis) {
        this.currentTimestamp = currentTimestamp;
        this.sleepMillis = sleepMillis;
    }

    @Override
    public void open(SourceReaderContext readerContext) throws Exception {
        GeneratorFunction.super.open(readerContext);

        if (currentTimestamp == Long.MIN_VALUE) {
            currentTimestamp = System.currentTimeMillis();
        }
    }

    @Override
    public Long map(Long value) throws Exception {
        long recordTimestamp = currentTimestamp + value * sleepMillis;
        long sleepTime = recordTimestamp - System.currentTimeMillis();
        if (sleepTime > 0) {
            Thread.sleep(sleepTime);
        }
        return recordTimestamp;
    }
}
