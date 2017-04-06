/*
 * Copyright (C) 2017-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.util.concurrent;

import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.google.common.base.Preconditions;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reconnection process that, if failed, is retried periodically according to the intervals
 * defined by a policy.
 *
 * <p>All the tasks run on a Netty event executor that is provided at construction time. Clients are
 * also expected to call the public methods on that thread.
 */
public class Reconnection {
  private static final Logger LOG = LoggerFactory.getLogger(Reconnection.class);

  private final EventExecutor executor;
  private final ReconnectionPolicy reconnectionPolicy;
  private final Callable<CompletionStage<Boolean>> reconnectionTask;

  private ReconnectionPolicy.ReconnectionSchedule reconnectionSchedule;
  private ScheduledFuture<CompletionStage<Boolean>> nextAttempt;

  /**
   * @param reconnectionTask the actual thing to try on a reconnection, returns if it succeeded or
   *     not.
   */
  public Reconnection(
      EventExecutor executor,
      ReconnectionPolicy reconnectionPolicy,
      Callable<CompletionStage<Boolean>> reconnectionTask) {
    this.executor = executor;
    this.reconnectionPolicy = reconnectionPolicy;
    this.reconnectionTask = reconnectionTask;
  }

  public boolean isRunning() {
    assert executor.inEventLoop();
    return nextAttempt != null;
  }

  /** @throws IllegalStateException if the reconnection is already running */
  public void start() {
    assert executor.inEventLoop();
    Preconditions.checkState(nextAttempt == null, "Already running");
    reconnectionSchedule = reconnectionPolicy.newSchedule();

    scheduleNextAttempt();
  }

  private void scheduleNextAttempt() {
    assert executor.inEventLoop();
    Duration nextInterval = reconnectionSchedule.nextDelay();
    LOG.debug("{} scheduling next reconnection in {}", this, nextInterval);
    nextAttempt = executor.schedule(reconnectionTask, nextInterval.toNanos(), TimeUnit.NANOSECONDS);
    nextAttempt.addListener(
        (Future<CompletionStage<Boolean>> f) -> {
          if (f.isSuccess()) {
            onNextAttemptStarted(f.getNow());
          } else {
            LOG.warn("Uncaught error while starting reconnection attempt", f.cause());
            scheduleNextAttempt();
          }
        });
  }

  // When the Callable runs this means the caller has started the attempt, we have yet to wait on
  // the CompletableFuture to find out if that succeeded or not.
  private void onNextAttemptStarted(CompletionStage<Boolean> futureOutcome) {
    assert executor.inEventLoop();
    futureOutcome
        .whenCompleteAsync(this::onNextAttemptCompleted, executor)
        .exceptionally(UncaughtExceptions::log);
  }

  private void onNextAttemptCompleted(Boolean success, Throwable error) {
    assert executor.inEventLoop();
    if (success) {
      LOG.debug("{} reconnection successful", this);
      stop();
    } else {
      if (error != null) {
        LOG.warn("Uncaught error while starting reconnection attempt", error);
      }
      if (isRunning()) {
        scheduleNextAttempt();
      }
    }
  }

  public void stop() {
    assert executor.inEventLoop();
    LOG.debug("{} stopping reconnection", this);
    if (nextAttempt != null) {
      nextAttempt.cancel(true);
    }
    nextAttempt = null;
    reconnectionSchedule = null;
  }
}
