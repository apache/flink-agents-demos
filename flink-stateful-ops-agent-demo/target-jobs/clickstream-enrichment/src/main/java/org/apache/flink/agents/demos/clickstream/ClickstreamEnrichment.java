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

package org.apache.flink.agents.demos.clickstream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.util.ParameterTool;

/**
 * Target job for the stateful ops demo: the job shape from the Flink "Balanced Tasks Scheduling"
 * docs. Vertex A (source + Enrich) runs at parallelism 6 and vertex B (SessionScore + sink) at 3,
 * in one slot sharing group. Both are CPU-bound, so the TaskManager that hosts the most tasks
 * becomes the bottleneck under the default scheduling strategy.
 *
 * <p>Arguments: --rate (records/s for the whole job, 0 = unbounded), --work-a, --work-b
 * (CPU iterations per record), --parallelism-a, --parallelism-b.
 */
public class ClickstreamEnrichment {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        double rate = params.getDouble("rate", 0);
        int workA = params.getInt("work-a", 20_000);
        int workB = params.getInt("work-b", 40_000);
        int parallelismA = params.getInt("parallelism-a", 6);
        int parallelismB = params.getInt("parallelism-b", 3);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataGeneratorSource<Click> source =
                new DataGeneratorSource<>(
                        index -> new Click(index % 100_000, index),
                        Long.MAX_VALUE,
                        rate > 0 ? RateLimiterStrategy.perSecond(rate) : RateLimiterStrategy.noOp(),
                        TypeInformation.of(Click.class));

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Clicks")
                .uid("clicks")
                .setParallelism(parallelismA)
                .map(new Enrich(workA))
                .name("Enrich")
                .uid("enrich")
                .setParallelism(parallelismA)
                .keyBy(click -> click.userId)
                .map(new SessionScore(workB))
                .name("SessionScore")
                .uid("session-score")
                .setParallelism(parallelismB)
                .sinkTo(new DiscardingSink<>())
                .name("Sink")
                .uid("sink")
                .setParallelism(parallelismB);

        env.execute("ClickstreamEnrichment");
    }

    /** One click event. */
    public static class Click {
        public long userId;
        public long seq;
        public long checksum;

        public Click() {}

        public Click(long userId, long seq) {
            this.userId = userId;
            this.seq = seq;
        }
    }

    /** Stand-in for CPU-heavy enrichment work (parsing, lookups, feature extraction). */
    static long burn(long seed, int iterations) {
        long x = seed | 1;
        for (int i = 0; i < iterations; i++) {
            x ^= x << 13;
            x ^= x >>> 7;
            x ^= x << 17;
        }
        return x;
    }

    static class Enrich extends RichMapFunction<Click, Click> {
        private final int work;

        Enrich(int work) {
            this.work = work;
        }

        @Override
        public Click map(Click click) {
            click.checksum = burn(click.seq, work);
            return click;
        }
    }

    /** Keyed and stateful, so the savepoint carries real state across redeploys. */
    static class SessionScore extends RichMapFunction<Click, Long> {
        private final int work;
        private transient ValueState<Long> clicksPerUser;

        SessionScore(int work) {
            this.work = work;
        }

        @Override
        public void open(OpenContext openContext) {
            clicksPerUser =
                    getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("clicks-per-user", Long.class));
        }

        @Override
        public Long map(Click click) throws Exception {
            Long count = clicksPerUser.value();
            long next = count == null ? 1 : count + 1;
            clicksPerUser.update(next);
            return burn(click.checksum ^ next, work);
        }
    }
}
