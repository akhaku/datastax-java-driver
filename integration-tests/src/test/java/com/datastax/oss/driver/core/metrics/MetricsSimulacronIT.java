/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.driver.core.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import ch.qos.logback.classic.Level;
import com.codahale.metrics.Meter;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.DropwizardMetricsFactory;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.internal.core.util.LoggerTest;
import com.datastax.oss.driver.shaded.guava.common.base.Ticker;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.google.common.collect.Lists;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Duration;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@Category(ParallelizableTests.class)
@RunWith(DataProviderRunner.class)
public class MetricsSimulacronIT {

  @ClassRule
  public static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(1));

  @Before
  public void clearPrimes() {
    SIMULACRON_RULE.cluster().clearLogs();
    SIMULACRON_RULE.cluster().clearPrimes(true);
  }

  @Test
  public void should_remove_node_metrics_and_not_remove_session_metrics_after_eviction_time() {

    // given
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withStringList(
                DefaultDriverOption.METRICS_SESSION_ENABLED,
                Lists.newArrayList("bytes-sent", "bytes-received"))
            .withStringList(
                DefaultDriverOption.METRICS_NODE_ENABLED,
                Lists.newArrayList("bytes-sent", "bytes-received"))
            .withDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER, Duration.ofHours(1))
            .build();
    FakeTicker fakeTicker = new FakeTicker();
    try (CqlSession session =
        new MetricsTestContextBuilder()
            .addContactEndPoints(SIMULACRON_RULE.getContactPoints())
            .withConfigLoader(loader)
            .withTicker(fakeTicker)
            .build()) {
      for (int i = 0; i < 10; i++) {
        session.execute("SELECT release_version FROM system.local");
      }

      // when
      fakeTicker.advance(Duration.ofHours(2));

      // then session metrics are not evicted
      assertThat(session.getMetrics())
          .hasValueSatisfying(
              metrics -> {
                assertThat(metrics.<Meter>getSessionMetric(DefaultSessionMetric.BYTES_SENT))
                    .hasValueSatisfying(
                        bytesSent -> assertThat(bytesSent.getCount()).isGreaterThan(0));
                assertThat(metrics.<Meter>getSessionMetric(DefaultSessionMetric.BYTES_RECEIVED))
                    .hasValueSatisfying(
                        bytesReceived -> assertThat(bytesReceived.getCount()).isGreaterThan(0));
              });

      // and node metrics are evicted
      Awaitility.await()
          .until(
              () -> {
                // get only node in a cluster and evaluate its metrics.
                Node node = session.getMetadata().getNodes().values().iterator().next();
                Metrics metrics = session.getMetrics().get();
                return !metrics.<Meter>getNodeMetric(node, DefaultNodeMetric.BYTES_SENT).isPresent()
                    && !metrics
                        .<Meter>getNodeMetric(node, DefaultNodeMetric.BYTES_RECEIVED)
                        .isPresent();
              });
    }
  }

  @Test
  public void
      should_not_evict_not_updated_node_metric_if_any_other_node_level_metric_was_updated() {
    // given
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withStringList(
                DefaultDriverOption.METRICS_NODE_ENABLED,
                Lists.newArrayList("bytes-sent", "errors.request.aborted"))
            .withDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER, Duration.ofHours(1))
            .build();
    FakeTicker fakeTicker = new FakeTicker();
    try (CqlSession session =
        new MetricsTestContextBuilder()
            .addContactEndPoints(SIMULACRON_RULE.getContactPoints())
            .withConfigLoader(loader)
            .withTicker(fakeTicker)
            .build()) {
      for (int i = 0; i < 10; i++) {
        session.execute("SELECT release_version FROM system.local");
      }

      // when advance time to before eviction
      fakeTicker.advance(Duration.ofMinutes(59));
      // execute query that update only bytes-sent
      session.execute("SELECT release_version FROM system.local");
      // advance time to after eviction
      fakeTicker.advance(Duration.ofMinutes(2));

      // then all node-level metrics should not be evicted
      Awaitility.await()
          .until(
              () -> {
                // get only node in a cluster and evaluate its metrics.
                Node node = session.getMetadata().getNodes().values().iterator().next();
                Metrics metrics = session.getMetrics().get();
                return metrics.<Meter>getNodeMetric(node, DefaultNodeMetric.BYTES_SENT).isPresent()
                    && metrics
                        .<Meter>getNodeMetric(node, DefaultNodeMetric.ABORTED_REQUESTS)
                        .isPresent();
              });
    }
  }

  @Test
  public void should_log_warning_when_provided_eviction_time_setting_is_too_low() {
    // given
    Duration expireAfter = Duration.ofMinutes(59);
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER, expireAfter)
            .build();
    LoggerTest.LoggerSetup logger =
        LoggerTest.setupTestLogger(DropwizardMetricsFactory.class, Level.WARN);

    try {
      new MetricsTestContextBuilder()
          .addContactEndPoints(SIMULACRON_RULE.getContactPoints())
          .withTicker(new FakeTicker())
          .withConfigLoader(loader)
          .build();
      verify(logger.appender, timeout(500).times(1)).doAppend(logger.loggingEventCaptor.capture());
      assertThat(logger.loggingEventCaptor.getValue().getMessage()).isNotNull();
      assertThat(logger.loggingEventCaptor.getValue().getFormattedMessage())
          .contains(
              String.format(
                  "The %s setting was provided with too low value: %s. Consider increasing it to at least 1 hour. "
                      + "Having lower values may cause your node-level metrics to keep disappearing and reappearing.",
                  DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER, expireAfter));
    } finally {
      logger.close();
    }
  }

  @Test
  @UseDataProvider(value = "acceptableEvictionTimes")
  public void should_not_log_warning_when_provided_eviction_time_setting_is_acceptable(
      Duration evictionTime) {
    // given
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER, evictionTime)
            .build();
    LoggerTest.LoggerSetup logger =
        LoggerTest.setupTestLogger(DropwizardMetricsFactory.class, Level.WARN);

    try {
      new MetricsTestContextBuilder()
          .addContactEndPoints(SIMULACRON_RULE.getContactPoints())
          .withTicker(new FakeTicker())
          .withConfigLoader(loader)
          .build();
      verify(logger.appender, timeout(500).times(0)).doAppend(logger.loggingEventCaptor.capture());
    } finally {
      logger.close();
    }
  }

  @DataProvider
  public static Object[][] acceptableEvictionTimes() {
    return new Object[][] {{Duration.ofHours(1)}, {Duration.ofMinutes(61)}};
  }

  private static class MetricsTestContextBuilder
      extends SessionBuilder<MetricsTestContextBuilder, CqlSession> {

    private Ticker ticker;

    @Override
    protected CqlSession wrap(@NonNull CqlSession defaultSession) {
      return defaultSession;
    }

    public MetricsTestContextBuilder withTicker(Ticker ticker) {
      this.ticker = ticker;
      return this;
    }

    @Override
    protected DriverContext buildContext(
        DriverConfigLoader configLoader, ProgrammaticArguments programmaticArguments) {
      return new MetricsTestContext(configLoader, programmaticArguments, ticker);
    }
  }

  private static class MetricsTestContext extends DefaultDriverContext {
    private final Ticker ticker;

    public MetricsTestContext(
        @NonNull DriverConfigLoader configLoader,
        @NonNull ProgrammaticArguments programmaticArguments,
        @NonNull Ticker ticker) {
      super(configLoader, programmaticArguments);
      this.ticker = ticker;
    }

    @Override
    protected MetricsFactory buildMetricsFactory() {
      return new DropwizardMetricsFactoryCustomTicker(this, ticker);
    }

    private static class DropwizardMetricsFactoryCustomTicker extends DropwizardMetricsFactory {

      public DropwizardMetricsFactoryCustomTicker(InternalDriverContext context, Ticker ticker) {
        super(context, ticker);
      }
    }
  }
}
