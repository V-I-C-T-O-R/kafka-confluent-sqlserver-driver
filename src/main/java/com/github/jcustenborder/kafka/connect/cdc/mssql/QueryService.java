/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.cdc.mssql;

import com.github.jcustenborder.kafka.connect.cdc.Change;
import com.github.jcustenborder.kafka.connect.cdc.ChangeKey;
import com.github.jcustenborder.kafka.connect.cdc.ChangeWriter;
import com.github.jcustenborder.kafka.connect.cdc.JdbcUtils;
import com.github.jcustenborder.kafka.connect.cdc.TableMetadataProvider;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.PooledConnection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class QueryService extends AbstractExecutionThreadService {
    static final Pattern CHANGE_TRACKING_TABLE_PATTERN = Pattern.compile("^([^\\.]+)\\.([^\\.]+)$");
    private static final Logger log = LoggerFactory.getLogger(QueryService.class);
    final Time time;
    final TableMetadataProvider tableMetadataProvider;
    final MsSqlSourceConnectorConfig config;
    final ChangeWriter changeWriter;
    /*添加数据源的ip做数据表的标示*/
    final String ip_Sql = "SELECT LOCAL_NET_ADDRESS AS 'ip' FROM SYS.DM_EXEC_CONNECTIONS WHERE SESSION_ID = @@SPID";
    RateLimiter rateLimiter = RateLimiter.create(1);
    private String ip;

    QueryService(Time time, TableMetadataProvider tableMetadataProvider, MsSqlSourceConnectorConfig config, ChangeWriter changeWriter) {
        this.time = time;
        this.tableMetadataProvider = tableMetadataProvider;
        this.config = config;
        this.changeWriter = changeWriter;
    }

    ChangeKey changeKey(String changeTrackingTable) {
        Matcher matcher = CHANGE_TRACKING_TABLE_PATTERN.matcher(changeTrackingTable);
        Preconditions.checkState(matcher.matches(), "'%s' is not formatted in properly. Use 'schemaName.databaseName.tableName'.", changeTrackingTable);
        String schemaName = matcher.group(1);
        String tableName = matcher.group(2);
        return new ChangeKey(this.config.initialDatabase, schemaName, tableName);
    }

    @Override
    protected void run() throws Exception {
        this.ip = commonQuery();
        while (isRunning()) {
            try {
                processTables();
            } catch (Exception ex) {
                log.error("Exception thrown", ex);
            }
        }
    }

    void processTables() throws SQLException {
        for (String changeTrackingTable : Iterables.cycle(this.config.changeTrackingTables)) {
            if (!isRunning()) {
                break;
            }

            rateLimiter.acquire();

            ChangeKey changeKey;

            try {
                changeKey = changeKey(changeTrackingTable);
            } catch (Exception ex) {
                log.error("Exception thrown while parsing table name '{}'", changeTrackingTable, ex);
                continue;
            }

            try {
                queryTable(this.changeWriter, changeKey);
            } catch (Exception ex) {
                log.error("Exception thrown while querying for {}", changeKey, ex);
            }
        }
    }

    void queryTable(ChangeWriter changeWriter, ChangeKey changeKey) throws SQLException {
//        String ip = commonQuery();
        PooledConnection pooledConnection = null;
        try {
            pooledConnection = JdbcUtils.openPooledConnection(this.config, changeKey);
            log.trace("{}: Setting transaction level to 4096 (READ_COMMITTED_SNAPSHOT)", changeKey);
            pooledConnection.getConnection().setTransactionIsolation(4096);
            pooledConnection.getConnection().setAutoCommit(false);

            TableMetadataProvider.TableMetadata tableMetadata = this.tableMetadataProvider.tableMetadata(changeKey);
            MsSqlQueryBuilder queryBuilder = new MsSqlQueryBuilder(pooledConnection.getConnection());

            Map<String, Object> sourcePartition = Change.sourcePartition(changeKey);
            Map<String, Object> startOffset = this.tableMetadataProvider.startOffset(changeKey);
            long offset = MsSqlChange.offset(startOffset);

            log.trace("{}: Starting at offset {} ", changeKey, offset);

            try (PreparedStatement statement = queryBuilder.changeTrackingStatement(tableMetadata)) {
                statement.setLong(1, offset);
                long count = 0;

                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        final long changeVersion = resultSet.getLong("__metadata_sys_change_version");

                        log.trace("{}: __metadata_sys_change_version = {}", changeKey, changeVersion);

                        MsSqlChange.Builder builder = MsSqlChange.builder();
                        MsSqlChange change = builder.build(tableMetadata, resultSet, this.time,this.ip);
                        change.sourcePartition = sourcePartition;
                        change.sourceOffset = MsSqlChange.offset(changeVersion);
                        changeWriter.addChange(change);
                        this.tableMetadataProvider.cacheOffset(changeKey, change.sourceOffset);
                        count++;
                    }
                }

                log.info("{}: Processed {} record(s).", changeKey, count);

            }
        } finally {
            if (null != pooledConnection) {
                log.trace("{}: calling connection.commit()", changeKey);
                pooledConnection.getConnection().commit();
            }

            JdbcUtils.closeConnection(pooledConnection);
        }
    }

    String commonQuery() throws SQLException {
        PooledConnection pooledConnection = null;
        try {
            pooledConnection = JdbcUtils.openPooledConnection(this.config, null);
            pooledConnection.getConnection().setTransactionIsolation(4096);
            pooledConnection.getConnection().setAutoCommit(false);

            MsSqlQueryBuilder queryBuilder = new MsSqlQueryBuilder(pooledConnection.getConnection());
            try (PreparedStatement statement = queryBuilder.connection.prepareStatement(ip_Sql)) {
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        return resultSet.getString("ip");
                    }
                }
            }
        } finally {
            if (null != pooledConnection) {
                pooledConnection.getConnection().commit();
            }
            JdbcUtils.closeConnection(pooledConnection);
        }
        return "127.0.0.1";
    }
}
