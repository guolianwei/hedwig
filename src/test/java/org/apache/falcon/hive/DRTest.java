/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.falcon.hive;

import org.apache.falcon.hive.util.DRStatusStore;
import org.apache.falcon.hive.util.FileUtils;
import org.apache.falcon.hive.util.HiveDRStatusStore;
import org.apache.falcon.hive.util.HiveDRUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.HashMap;
import java.util.Map;

/**
 * Test class for DR.
 */


@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DRTest {
    public void testHiveDrExport(String executionStage) {
        //EXPORT,LASTEVENTS
        String source = "nn2";
        String target = "nn1";
        String[] testArgs = {
                "-sourceMetastoreUri", String.format("thrift://%s:9083", source),
                "-sourceDatabases", "default",
                "-sourceTables", "*",
                "-sourceStagingPath", "/apps/hive/tools/dr",
                "-sourceNN", String.format("hdfs://%s:8020", source),
                "-sourceCluster", source,
                "-sourceHiveServer2Uri", String.format("hive2://%s:10000", source),
                "-sourceDatabase", "default",

                "-targetCluster", target,
                "-targetHiveServer2Uri", String.format("hive2://%s:10000", target),
                "-targetMetastoreUri", String.format("thrift://%s:9083", target),
                "-targetStagingPath", "/apps/hive/tools/dr",
                "-targetNN", String.format("hdfs://%s:8020", target),
                "-hiveJobName", "synclab",
                "-clusterForJobRun", source,
                "-clusterForJobRunWriteEP", target,
                "-maxEvents", "5",
                "-replicationMaxMaps", "1",
                "-distcpMapBandwidth", "4",
                "-distcpMaxMaps", "4",
                "-executionStage", executionStage
        };
        HiveDRTool.main(testArgs);
    }



    @Test
    public void testHiveDrAxEvents() {
        testHiveDrExport(HiveDRUtils.ExecutionStage.LASTEVENTS.name());
    }

    @Test
    public void testHiveDrExport() {
        testHiveDrExport(HiveDRUtils.ExecutionStage.EXPORT.name());
    }

    @Test
    public void testHiveDrImport() {
        testHiveDrExport(HiveDRUtils.ExecutionStage.IMPORT.name());
    }

    public void testLastReplicatedEvents() {
        String source = "nn2";
        String target = "nn1";
        String targetMetastoreUri = String.format("thrift://%s:9083", target);
        String sourceMetastoreUri = String.format("thrift://%s:9083", source);
        String targetNN = String.format("hdfs://%s:8020", target);
        String sourceNN = String.format("hdfs://%s:8020", source);
        //JOB_CLUSTER_NN
        Map<HiveDRArgs, String> options = new HashMap<>();
        options.put(HiveDRArgs.SOURCE_TABLES, "*");
        options.put(HiveDRArgs.SOURCE_DATABASES, "default");
        options.put(HiveDRArgs.JOB_NAME, "synclab");
        options.put(HiveDRArgs.SOURCE_METASTORE_URI, sourceMetastoreUri);
        options.put(HiveDRArgs.TARGET_METASTORE_URI, targetMetastoreUri);

        options.put(HiveDRArgs.TARGET_NN, targetNN);
        options.put(HiveDRArgs.JOB_CLUSTER_NN, sourceNN);


        HiveDROptions hiveDROptions = new HiveDROptions(options);


        try {
            Configuration conf = new Configuration();
            Configuration targetConf = FileUtils.getConfiguration(conf, hiveDROptions.getTargetWriteEP(),
                    hiveDROptions.getTargetNNKerberosPrincipal());
            Configuration jobConf = FileUtils.getConfiguration(conf, hiveDROptions.getJobClusterWriteEP(),
                    hiveDROptions.getJobClusterNNPrincipal());

            FileSystem targetClusterFS = FileSystem.get(targetConf);
            String targetMetastoreKerberosPrincipal = null;
            String targetHive2KerberosPrincipal = null;
            DRStatusStore drStore = new HiveDRStatusStore(targetClusterFS);
            LastReplicatedEvents lastReplicatedEvents = new LastReplicatedEvents(
                    jobConf, targetMetastoreUri,
                    targetMetastoreKerberosPrincipal,
                    targetHive2KerberosPrincipal,
                    drStore, hiveDROptions
            );

            lastReplicatedEvents.getLastEvents(hiveDROptions);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
