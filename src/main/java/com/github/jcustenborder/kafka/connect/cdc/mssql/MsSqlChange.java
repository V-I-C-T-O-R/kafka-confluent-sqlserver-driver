/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
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
package com.github.jcustenborder.kafka.connect.cdc.mssql;

import com.github.jcustenborder.kafka.connect.cdc.Change;
import com.github.jcustenborder.kafka.connect.cdc.ChangeKey;
import com.github.jcustenborder.kafka.connect.cdc.TableMetadataProvider;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

class MsSqlChange implements Change {
  private static final Logger log = LoggerFactory.getLogger(MsSqlChange.class);
  Map<String, String> metadata;
  Map<String, Object> sourcePartition;
  Map<String, Object> sourceOffset;
  String databaseName;
  String schemaName;
  String tableName;
  long timestamp;
  ChangeType changeType;
  List<ColumnValue> keyColumns;
  List<ColumnValue> valueColumns;

  public static Builder builder() {
    return new Builder();
  }

  public static Map<String, Object> offset(long changeVersion) {
    return ImmutableMap.of(
        "sys_change_version", (Object) changeVersion
    );
  }

  public static Long offset(Map<String, Object> sourceOffset) {
    Preconditions.checkNotNull(sourceOffset, "sourceOffset cannot be null.");
    Long changeVersion = (Long) sourceOffset.get("sys_change_version");
    Preconditions.checkNotNull(changeVersion, "sourceOffset[\"sys_change_version\"] cannot be null.");
    return changeVersion;
  }

  @Override
  public Map<String, String> metadata() {
    return this.metadata;
  }

  @Override
  public Map<String, Object> sourcePartition() {
    return this.sourcePartition;
  }

  @Override
  public Map<String, Object> sourceOffset() {
    return this.sourceOffset;
  }

  @Override
  public String databaseName() {
    return this.databaseName;
  }

  @Override
  public String schemaName() {
    return this.schemaName;
  }

  @Override
  public String tableName() {
    return this.tableName;
  }

  @Override
  public List<ColumnValue> keyColumns() {
    return this.keyColumns;
  }

  @Override
  public List<ColumnValue> valueColumns() {
    return this.valueColumns;
  }

  @Override
  public ChangeType changeType() {
    return this.changeType;
  }

  @Override
  public long timestamp() {
    return this.timestamp;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(MsSqlChange.class)
        .omitNullValues()
        .add("databaseName", this.databaseName)
        .add("schemaName", this.schemaName)
        .add("tableName", this.tableName)
        .add("changeType", this.changeType)
        .add("timestamp", this.timestamp)
        .add("metadata", this.metadata)
        .add("sourcePartition", this.sourcePartition)
        .add("sourceOffset", this.sourceOffset)
        .add("keyColumns", this.keyColumns)
        .add("valueColumns", this.valueColumns)
        .toString();
  }

  static class MsSqlColumnValue implements ColumnValue {

    final String columnName;
    final Schema schema;
    final Object value;

    MsSqlColumnValue(String columnName, Schema schema, Object value) {
      this.columnName = columnName;
      this.schema = schema;
      this.value = value;
    }


    @Override
    public String columnName() {
      return this.columnName;
    }

    @Override
    public Schema schema() {
      return this.schema;
    }

    @Override
    public Object value() {
      return this.value;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(MsSqlColumnValue.class)
          .omitNullValues()
          .add("columnName", this.columnName)
          .add("schema", this.schema)
          .add("value", this.value)
          .toString();
    }
  }

  static class Builder {
    static Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    public MsSqlChange build(TableMetadataProvider.TableMetadata tableMetadata, ResultSet resultSet, Time time,String ip) throws SQLException {
      MsSqlChange change = new MsSqlChange();
      change.timestamp = time.milliseconds();
      change.databaseName = tableMetadata.databaseName();
      change.schemaName = tableMetadata.schemaName();
      change.tableName = tableMetadata.tableName();

      final long sysChangeVersion = resultSet.getLong("__metadata_sys_change_version");
      final long sysChangeCreationVersion = resultSet.getLong("__metadata_sys_change_creation_version");
      final String changeOperation = resultSet.getString("__metadata_sys_change_operation");

//      change.metadata = ImmutableMap.of(
//          "sys_change_operation", changeOperation,
////          "sys_change_creation_version", String.valueOf(sysChangeCreationVersion),
////          "sys_change_version", String.valueOf(sysChangeVersion),
//          "database_name",change.databaseName,
//          "schema_name",change.schemaName,
//          "table_name",change.tableName,
//          "timestamp",String.valueOf(change.timestamp)
//      );
        change.metadata = ImmutableMap.<String, String>builder().put("sys_change_operation", changeOperation).put("database_name",change.databaseName)
                .put("schema_name",change.schemaName)
                .put("table_name",change.tableName).put("timestamp",String.valueOf(change.timestamp)).put("ip",ip).build();
      boolean flag = false;
      switch (changeOperation) {
        case "I":
          change.changeType = ChangeType.INSERT;
          break;
        case "U":
          change.changeType = ChangeType.UPDATE;
          break;
        case "D":
          flag = true;
          change.changeType = ChangeType.INSERT;
//          change.changeType = ChangeType.DELETE;
          break;
        default:
          throw new UnsupportedOperationException(
              String.format("Unsupported sys_change_operation of '%s'", changeOperation)
          );
      }
      log.trace("build() - changeType = {}", change.changeType);

      change.keyColumns = new ArrayList<>(tableMetadata.keyColumns().size());
      change.valueColumns = new ArrayList<>(tableMetadata.columnSchemas().size());

      if(!flag){
        for (Map.Entry<String, Schema> kvp : tableMetadata.columnSchemas().entrySet()) {
          String columnName = kvp.getKey();
          Schema schema = kvp.getValue();
          Object value;

          if (Schema.Type.INT64 == schema.type() &&
                  Timestamp.LOGICAL_NAME.equals(schema.name())) {
            value = new java.util.Date(
                    resultSet.getTimestamp(columnName, calendar).getTime()
            );
          } else if (Schema.Type.INT32 == schema.type() &&
                  Date.LOGICAL_NAME.equals(schema.name())) {
            value = new java.util.Date(
                    resultSet.getDate(columnName, calendar).getTime()
            );
          } else if (Schema.Type.INT32 == schema.type() &&
                  org.apache.kafka.connect.data.Time.LOGICAL_NAME.equals(schema.name())) {
            value = new java.util.Date(
                    resultSet.getTime(columnName, calendar).getTime()
            );
          } else if (Schema.Type.INT8 == schema.type()) {
            // Really lame Microsoft. A tiny int is stored as a single byte with a value of 0-255.
            // Explain how this should be returned as a short?
            value = resultSet.getByte(columnName);
          } else {
            value = resultSet.getObject(columnName);
          }

          log.trace("build() - columnName = '{}' value = '{}'", columnName, value);
          MsSqlColumnValue columnValue = new MsSqlColumnValue(columnName, schema, value);
          change.valueColumns.add(columnValue);
          if (tableMetadata.keyColumns().contains(columnName)) {
            change.keyColumns.add(columnValue);
          }
        }
      } else{
        for (Map.Entry<String, Schema> kvp : tableMetadata.columnSchemas().entrySet()) {
          String columnName = kvp.getKey();
          Schema schema = kvp.getValue();
          Object value;

          if (Schema.Type.INT64 == schema.type() &&
                  Timestamp.LOGICAL_NAME.equals(schema.name())) {
            value = new java.util.Date(
                    resultSet.getTimestamp(columnName, calendar).getTime()
            );
          } else if (Schema.Type.INT32 == schema.type() &&
                  Date.LOGICAL_NAME.equals(schema.name())) {
            value = new java.util.Date(
                    resultSet.getDate(columnName, calendar).getTime()
            );
          } else if (Schema.Type.INT32 == schema.type() &&
                  org.apache.kafka.connect.data.Time.LOGICAL_NAME.equals(schema.name())) {
            value = new java.util.Date(
                    resultSet.getTime(columnName, calendar).getTime()
            );
          } else if (Schema.Type.INT8 == schema.type()) {
            // Really lame Microsoft. A tiny int is stored as a single byte with a value of 0-255.
            // Explain how this should be returned as a short?
            value = resultSet.getByte(columnName);
          } else {
            value = resultSet.getObject(columnName);
          }

          log.trace("build() - columnName = '{}' value = '{}'", columnName, generateDelValue(schema.type()));
          MsSqlColumnValue columnValue = new MsSqlColumnValue(columnName, schema, generateDelValue(schema.type()));
          change.valueColumns.add(columnValue);
          if (tableMetadata.keyColumns().contains(columnName)) {
            columnValue = new MsSqlColumnValue(columnName, schema,value);
            change.keyColumns.add(columnValue);
          }
        }
      }
      return change;
    }
  }

  static Object generateDelValue(Schema.Type type){
      if(type == Schema.Type.INT16||type == Schema.Type.INT8||type == Schema.Type.INT32){
        return 0;
      }else if (type == Schema.Type.INT64){
        return 0L;
      }else if(type == Schema.Type.BOOLEAN){
        return false;
      }else if(type == Schema.Type.FLOAT32||type == Schema.Type.FLOAT64){
        return 0.0;
      }else{
        return "";
      }
  }

  static Schema generateSchema(final String columnName) throws SQLException {
    SchemaBuilder builder = SchemaBuilder.string();
    builder.parameters(
            ImmutableMap.of(Change.ColumnValue.COLUMN_NAME, columnName)
    );
    return builder.build();
  }
}
