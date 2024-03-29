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

import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.hive.util.FileUtils;

import java.util.*;

import static org.apache.falcon.hive.util.FileUtils.HDFS_SEP;

/**
 * Tool Options.
 */
public class HiveDROptions {
    private final Map<HiveDRArgs, String> context;

    protected HiveDROptions(Map<HiveDRArgs, String> context) {
        this.context = context;
    }

    public String getValue(HiveDRArgs arg) {
        return context.get(arg);
    }

    public Map<HiveDRArgs, String> getContext() {
        return context;
    }

    public String getSourceMetastoreUri() {
        return context.get(HiveDRArgs.SOURCE_METASTORE_URI);
    }

    public String getSourceMetastoreKerberosPrincipal() {
        return context.get(HiveDRArgs.SOURCE_HIVE_METASTORE_KERBEROS_PRINCIPAL);
    }

    public String getSourceHive2KerberosPrincipal() {
        return context.get(HiveDRArgs.SOURCE_HIVE2_KERBEROS_PRINCIPAL);
    }

    public List<String> getSourceDatabases() {
        return Arrays.asList(context.get(HiveDRArgs.SOURCE_DATABASES).trim().split(","));
    }

    public List<String> getSourceTables() {
        String s = context.get(HiveDRArgs.SOURCE_TABLES);
        if (s != null) {
            return Arrays.asList(s.trim().split(","));
        } else {
            return new ArrayList<>();
        }
    }

    public String getSourceStagingPath() {
        return context.get(HiveDRArgs.SOURCE_STAGING_PATH);
    }


    public void setSourceStagingPath() {
        String stagingPath = context.get(HiveDRArgs.SOURCE_STAGING_PATH);
        String srcStagingPath;
        if ("NA".equalsIgnoreCase(stagingPath)) {
            stagingPath = StringUtils.removeEnd(FileUtils.DEFAULT_EVENT_STORE_PATH,HDFS_SEP);
            srcStagingPath = stagingPath + HDFS_SEP + getJobName();
        } else {
            stagingPath = StringUtils.removeEnd(stagingPath, HDFS_SEP);
            srcStagingPath = stagingPath + HDFS_SEP + getJobName();
        }
        context.put(HiveDRArgs.SOURCE_STAGING_PATH, srcStagingPath);
    }

    public String getSourceWriteEP() {
        return context.get(HiveDRArgs.SOURCE_NN);
    }

    public String getSourceNNKerberosPrincipal() {
        return context.get(HiveDRArgs.SOURCE_NN_KERBEROS_PRINCIPAL);
    }

    public String getTargetWriteEP() {
        return context.get(HiveDRArgs.TARGET_NN);
    }

    public String getTargetMetastoreUri() {
        return context.get(HiveDRArgs.TARGET_METASTORE_URI);
    }

    public String getTargetNNKerberosPrincipal() {
        return context.get(HiveDRArgs.TARGET_NN_KERBEROS_PRINCIPAL);
    }

    public String getTargetMetastoreKerberosPrincipal() {
        return context.get(HiveDRArgs.TARGET_HIVE_METASTORE_KERBEROS_PRINCIPAL);
    }

    public String getTargetHive2KerberosPrincipal() {
        return context.get(HiveDRArgs.TARGET_HIVE2_KERBEROS_PRINCIPAL);
    }

    public String getTargetStagingPath() {
        return context.get(HiveDRArgs.TARGET_STAGING_PATH);
    }

    public void setTargetStagingPath() {
        String stagingPath = context.get(HiveDRArgs.TARGET_STAGING_PATH);
        String targetStagingPath;
        if ("NA".equalsIgnoreCase(stagingPath)) {
            stagingPath = StringUtils.removeEnd(FileUtils.DEFAULT_EVENT_STORE_PATH, HDFS_SEP);
            targetStagingPath = stagingPath + HDFS_SEP + getJobName();
        } else {
            stagingPath = StringUtils.removeEnd(stagingPath, HDFS_SEP);
            targetStagingPath = stagingPath + HDFS_SEP + getJobName();
        }
        context.put(HiveDRArgs.TARGET_STAGING_PATH, targetStagingPath);
    }

    public String getReplicationMaxMaps() {
        return context.get(HiveDRArgs.REPLICATION_MAX_MAPS);
    }

    public String getJobName() {
        return context.get(HiveDRArgs.JOB_NAME);
    }

    public int getMaxEvents() {
        return Integer.parseInt(context.get(HiveDRArgs.MAX_EVENTS));
    }

    public boolean shouldKeepHistory() {
        return Boolean.parseBoolean(context.get(HiveDRArgs.KEEP_HISTORY));
    }

    public String getJobClusterWriteEP() {
        return context.get(HiveDRArgs.JOB_CLUSTER_NN);
    }

    public String getJobClusterNNPrincipal() {
        return context.get(HiveDRArgs.JOB_CLUSTER_NN_KERBEROS_PRINCIPAL);
    }

    public String getExecutionStage() {
        return context.get(HiveDRArgs.EXECUTION_STAGE);
    }

    public boolean shouldBlock() {
        return true;
    }

    public static HiveDROptions create(String[] args) throws ParseException {
        Map<HiveDRArgs, String> options = new HashMap<>();

        CommandLine cmd = getCommand(args);
        for (HiveDRArgs arg : HiveDRArgs.values()) {
            String optionValue = arg.getOptionValue(cmd);
            if (StringUtils.isNotEmpty(optionValue)) {
                options.put(arg, optionValue);
            }
        }

        return new HiveDROptions(options);
    }

    private static CommandLine getCommand(String[] arguments) throws ParseException {
        Options options = new Options();

        for (HiveDRArgs arg : HiveDRArgs.values()) {
            addOption(options, arg, arg.isRequired());
        }

        return new GnuParser().parse(options, arguments, false);
    }

    private static void addOption(Options options, HiveDRArgs arg, boolean isRequired) {
        Option option = arg.getOption();
        option.setRequired(isRequired);
        options.addOption(option);
    }
}
