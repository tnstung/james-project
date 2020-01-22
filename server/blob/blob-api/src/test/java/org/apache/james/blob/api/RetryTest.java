/****************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one   *
 * or more contributor license agreements.  See the NOTICE file *
 * distributed with this work for additional information        *
 * regarding copyright ownership.  The ASF licenses this file   *
 * to you under the Apache License, Version 2.0 (the            *
 * "License"); you may not use this file except in compliance   *
 * with the License.  You may obtain a copy of the License at   *
 *                                                              *
 *   http://www.apache.org/licenses/LICENSE-2.0                 *
 *                                                              *
 * Unless required by applicable law or agreed to in writing,   *
 * software distributed under the License is distributed on an  *
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY       *
 * KIND, either express or implied.  See the License for the    *
 * specific language governing permissions and limitations      *
 * under the License.                                           *
 ****************************************************************/

package org.apache.james.blob.api;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuples;

public class RetryTest {

    @Test
    void howDoesItWork() {
        Flux.range(0, 2)
            .log()
            .doOnNext(System.out::println)
            .flatMap(c -> Mono.fromCallable(() -> {
                throw new IllegalStateException(String.valueOf(c));
            }).log())
            .retryWhen(errors ->  errors.index().flatMap(v -> Mono.fromCallable(() -> {
                System.out.println("running Mono on thread " + Thread.currentThread().getName());
                return v;
            }))
         //           .filter(v -> )
            ) //errors.filter(throwable -> Integer.valueOf(throwable.getMessage()) == 5).doOnNext(System.out::println))
            .doOnNext(System.out::println)
            .subscribeOn(Schedulers.newElastic("pool-1"))
            .blockLast(Duration.ofSeconds(10));
    }

}
