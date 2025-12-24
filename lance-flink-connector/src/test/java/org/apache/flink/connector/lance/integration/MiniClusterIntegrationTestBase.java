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

package org.apache.flink.connector.lance.integration;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;
import org.apache.flink.connector.lance.common.LanceConfig;
import org.apache.flink.connector.lance.common.LanceReadOptions;
import org.apache.flink.connector.lance.common.LanceWriteOptions;

import org.junit.ClassRule;
import org.junit.Before;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Base class for Lance-Flink integration tests using MiniCluster.
 *
 * <p>This class provides:
 * <ul>
 *   <li>MiniCluster resource management (2 TaskManagers, 2 slots each)</li>
 *   <li>Checkpoint directory creation and cleanup</li>
 *   <li>Test data directory isolation</li>
 *   <li>Common utility methods for test data creation and validation</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * public class MyIntegrationTest extends MiniClusterIntegrationTestBase {
 *     @Test
 *     public void testMyFeature() throws Exception {
 *         createTestDataset(1000);
 *         LanceConfig config = new LanceConfig.Builder(getTestDatasetUri()).build();
 *         // Test implementation
 *     }
 * }
 * }</pre>
 */
public abstract class MiniClusterIntegrationTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(MiniClusterIntegrationTestBase.class);

    // ============================================================================
    // MiniCluster Configuration
    // ============================================================================

    @ClassRule
    public static MiniClusterWithClientResource miniCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(2)
                            .setConfiguration(createFlinkConfiguration())
                            .build());

    /**
     * Creates the Flink configuration for MiniCluster.
     */
    private static Configuration createFlinkConfiguration() {
        Configuration config = new Configuration();
        
        // REST API configuration
        config.set(RestOptions.BIND_PORT, "0");
        
        // TaskManager memory configuration
        config.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, 
                org.apache.flink.configuration.MemorySize.ofMebiBytes(1024));
        config.set(TaskManagerOptions.MANAGED_MEMORY_FRACTION, 0.7f);
        config.set(TaskManagerOptions.NETWORK_MEMORY_FRACTION, 0.1f);
        
        return config;
    }

    // ============================================================================
    // Test Data Management
    // ============================================================================

    protected static final String TEST_BASE_DIR =
            System.getProperty("java.io.tmpdir") + "/lance-flink-integration-tests";

    protected String testDatasetUri;
    protected Path testDataDir;

    @Before
    public void setupTestCase() throws IOException {
        // Create base test directory if it doesn't exist
        File baseDirFile = new File(TEST_BASE_DIR);
        if (!baseDirFile.exists()) {
            Files.createDirectories(Paths.get(TEST_BASE_DIR));
        }
        
        // Create test directory
        testDataDir = Files.createTempDirectory(Paths.get(TEST_BASE_DIR), "test_");
        testDatasetUri = "file://" + testDataDir.toString() + "/dataset";
        
        LOG.info("Test data directory: {}", testDataDir);
        LOG.info("Dataset URI: {}", testDatasetUri);
    }

    @After
    public void cleanupTestCase() {
        try {
            if (testDataDir != null && Files.exists(testDataDir)) {
                FileUtils.deleteDirectory(testDataDir.toFile());
                LOG.info("Cleaned up test directory: {}", testDataDir);
            }
        } catch (IOException e) {
            LOG.warn("Failed to cleanup test directory: {}", testDataDir, e);
        }
    }

    // ============================================================================
    // Abstract Methods for Subclasses
    // ============================================================================

    /**
     * Get the dataset URI for this test.
     * Default implementation returns the test dataset URI.
     */
    protected String getTestDatasetUri() {
        return testDatasetUri;
    }

    /**
     * Get the row type information for test data.
     * Subclasses can override for custom schemas.
     */
    protected RowTypeInfo getDefaultRowTypeInfo() {
        return new RowTypeInfo(
                new TypeInformation<?>[] {
                    Types.LONG,         // id
                    Types.STRING,       // name
                    Types.DOUBLE,       // value
                    Types.LONG          // timestamp
                },
                new String[] {"id", "name", "value", "timestamp"}
        );
    }

    // ============================================================================
    // Configuration Builders
    // ============================================================================

    /**
     * Create a default LanceConfig for testing.
     */
    protected LanceConfig createDefaultLanceConfig() {
        return new LanceConfig.Builder(getTestDatasetUri())
                .readBatchSize(256)
                .writeBatchSize(512)
                .fragmentSize(10000)
                .enablePredicatePushdown(true)
                .enableColumnPruning(true)
                .maxRetries(3)
                .retryWaitMillis(1000)
                .build();
    }

    /**
     * Create a LanceConfig with custom batch sizes.
     */
    protected LanceConfig createLanceConfigWithBatchSize(long readBatch, long writeBatch) {
        return new LanceConfig.Builder(getTestDatasetUri())
                .readBatchSize(readBatch)
                .writeBatchSize(writeBatch)
                .fragmentSize(10000)
                .enablePredicatePushdown(true)
                .enableColumnPruning(true)
                .build();
    }

    /**
     * Create read options with specified columns.
     */
    protected LanceReadOptions createReadOptionsWithColumns(List<String> columns) {
        return new LanceReadOptions.Builder()
                .columns(columns)
                .build();
    }

    /**
     * Create write options with specified mode.
     */
    protected LanceWriteOptions createWriteOptionsWithMode(LanceWriteOptions.WriteMode mode) {
        return new LanceWriteOptions.Builder()
                .mode(mode)
                .build();
    }

    // ============================================================================
    // StreamExecutionEnvironment Setup
    // ============================================================================

    /**
     * Create a StreamExecutionEnvironment for testing.
     */
    protected StreamExecutionEnvironment createTestStreamEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        return env;
    }

    /**
     * Create a StreamExecutionEnvironment with checkpoint enabled.
     */
    protected StreamExecutionEnvironment createCheckpointedStreamEnvironment(long checkpointInterval) throws Exception {
        StreamExecutionEnvironment env = createTestStreamEnvironment();
        
        // Create checkpoint directory
        Path checkpointDir = Files.createTempDirectory(testDataDir, "checkpoint_");
        String checkpointPath = "file://" + checkpointDir.toString();
        
        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().setCheckpointingMode(
                org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        
        LOG.info("Checkpoint directory: {}", checkpointPath);
        return env;
    }

    // ============================================================================
    // Test Data Management
    // ============================================================================

    /**
     * Create a test dataset with the specified number of rows.
     * Uses default schema: (id LONG, name STRING, value DOUBLE, timestamp LONG)
     */
    public void createTestDataset(int rowCount) throws IOException {
        createTestDataset(rowCount, getDefaultRowTypeInfo());
    }

    /**
     * Create a test dataset with the specified number of rows and schema.
     */
    public void createTestDataset(int rowCount, RowTypeInfo schema) throws IOException {
        List<Row> data = generateTestData(rowCount, schema);
        writeDataToDataset(data, schema);
        LOG.info("Created test dataset with {} rows", rowCount);
    }

    /**
     * Append additional data to an existing dataset.
     */
    public void appendTestData(int rowCount) throws IOException {
        appendTestData(rowCount, getDefaultRowTypeInfo());
    }

    /**
     * Append additional data to an existing dataset with specified schema.
     */
    public void appendTestData(int rowCount, RowTypeInfo schema) throws IOException {
        List<Row> data = generateTestData(rowCount, schema);
        appendDataToDataset(data, schema);
        LOG.info("Appended {} rows to test dataset", rowCount);
    }

    // ============================================================================
    // Helper Methods (Protected)
    // ============================================================================

    /**
     * Generate test data rows.
     */
    protected List<Row> generateTestData(int rowCount, RowTypeInfo schema) {
        return org.apache.flink.connector.lance.integration.utils
                .TestDataGenerator.generateRows(rowCount, schema.getArity());
    }

    /**
     * Write data to dataset.
     * Note: This is a placeholder. Actual implementation depends on Lance SDK.
     */
    protected void writeDataToDataset(List<Row> data, RowTypeInfo schema) throws IOException {
        // Implementation depends on Lance Java SDK
        // For now, this is stubbed - actual implementation in Phase 2
        LOG.debug("Writing {} rows to dataset", data.size());
    }

    /**
     * Append data to existing dataset.
     * Note: This is a placeholder. Actual implementation depends on Lance SDK.
     */
    protected void appendDataToDataset(List<Row> data, RowTypeInfo schema) throws IOException {
        // Implementation depends on Lance Java SDK
        // For now, this is stubbed - actual implementation in Phase 2
        LOG.debug("Appending {} rows to dataset", data.size());
    }

    // ============================================================================
    // Result Validation
    // ============================================================================

    /**
     * Validate the number of rows in results.
     */
    protected void assertRowCount(List<Row> results, int expectedCount) {
        org.apache.flink.connector.lance.integration.utils.TestResultValidator
                .assertRowCount(results, expectedCount);
    }

    /**
     * Validate that columns match the expected list.
     */
    protected void assertColumnsMatch(List<Row> results, List<String> expectedColumns) {
        org.apache.flink.connector.lance.integration.utils.TestResultValidator
                .assertColumnsMatch(results, expectedColumns);
    }

    /**
     * Validate that no rows are null.
     */
    protected void assertNoNullRows(List<Row> results) {
        org.apache.flink.connector.lance.integration.utils.TestResultValidator
                .assertNoNullRows(results);
    }

    /**
     * Validate that primary key values are unique.
     */
    protected void assertUniqueByPrimaryKey(List<Row> results, int primaryKeyFieldIndex) {
        org.apache.flink.connector.lance.integration.utils.TestResultValidator
                .assertUniqueByPrimaryKey(results, primaryKeyFieldIndex);
    }

    // ============================================================================
    // Metrics Collection
    // ============================================================================

    /**
     * Create a metrics collector for performance measurements.
     */
    protected org.apache.flink.connector.lance.integration.utils.TestMetricsCollector 
            createMetricsCollector() {
        return new org.apache.flink.connector.lance.integration.utils.TestMetricsCollector();
    }

    // ============================================================================
    // Inner Classes
    // ============================================================================
    
    // Note: We use org.apache.flink.api.java.typeutils.RowTypeInfo directly
    // No need for custom RowTypeInfo implementation
}
