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

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.sample.jobs.excluded.ExcludedDependency;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class FailedStartupJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromSource(
                        new DataGeneratorSource<>(
                                new TimestampDataGenerator(1), Long.MAX_VALUE, Types.LONG),
                        WatermarkStrategy.<Long>forMonotonousTimestamps()
                                .withTimestampAssigner((ctx) -> new MyTimestampAssigner()),
                        "datagen")
                .disableChaining()
                .process(
                        new ProcessFunction<Long, Long>() {
                            @Override
                            public void processElement(
                                    Long value,
                                    ProcessFunction<Long, Long>.Context ctx,
                                    Collector<Long> out) {
                                // Reference excluded classes at startup to ensure immediate failure
                                String message = ExcludedDependency.getMessage();
                                System.out.println("Dependency message: " + message);

                                out.collect(value);
                            }
                        })
                .print();

        env.execute("Failed Startup Job");
    }

    public static class MyTimestampAssigner implements TimestampAssigner<Long> {

        @Override
        public long extractTimestamp(Long element, long recordTimestamp) {
            return element;
        }
    }
}
