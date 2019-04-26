/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.sink;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringJoiner;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig.PrimaryKeyMode;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.TableId;

public class OffsetsDbStorage {

  private static final Logger log = LoggerFactory.getLogger(OffsetsDbStorage.class);

  public static final String OFFSETS_TABLE_NAME = "connect_offsets";
  public static final String TOPIC_NAME_COLUMN_NAME = "topic_name";
  public static final String PARTITION_COLUMN_NAME = "partition";
  public static final String OFFSET_COLUMN_NAME = "offset_value";

  private final JdbcSinkConfig config;
  private final DatabaseDialect dbDialect;
  private final DbStructure dbStructure;
  private final CachedConnectionProvider cachedConnectionProvider;

  public OffsetsDbStorage(
      JdbcSinkConfig config,
      DatabaseDialect dbDialect,
      DbStructure dbStructure
  ) {
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;
    this.cachedConnectionProvider = new CachedConnectionProvider(this.dbDialect) {
      @Override
      protected void onConnect(Connection connection) throws SQLException {
        log.info("OffsetsDbStorage Connected");
      }
    };
  }

  private void createOrAmendOffsetTable(
      Connection connection,
      TableId tableId,
      DbStructure dbStructure,
      JdbcSinkConfig config) throws SQLException {
    Schema keySchema = SchemaBuilder.struct()
        .field(TOPIC_NAME_COLUMN_NAME, Schema.STRING_SCHEMA)
        .field(PARTITION_COLUMN_NAME, Schema.INT32_SCHEMA);
    Schema valueSchema = SchemaBuilder.struct()
        .field(OFFSET_COLUMN_NAME, SchemaBuilder.INT64_SCHEMA);
    FieldsMetadata fieldsMetadata = FieldsMetadata.extract(
        OFFSETS_TABLE_NAME,
        PrimaryKeyMode.RECORD_KEY,
        Arrays.asList(TOPIC_NAME_COLUMN_NAME, PARTITION_COLUMN_NAME),
        new HashSet<>(Arrays.asList(TOPIC_NAME_COLUMN_NAME,
            PARTITION_COLUMN_NAME,
            OFFSET_COLUMN_NAME)),
        keySchema,
        valueSchema
    );
    dbStructure.createOrAmendIfNecessary(
        config,
        connection,
        tableId,
        fieldsMetadata
    );
  }

  public Map<TopicPartition, Long> read(Set<TopicPartition> assignment) throws SQLException {
    if (assignment == null || assignment.size() == 0) {
      return Collections.emptyMap();
    }
    log.info("Reading offsets, assignment: {}", assignment);
    final Connection connection = cachedConnectionProvider.getConnection();
    Map<TopicPartition, Long> result = new HashMap<>();
    final TableId tableId = dbDialect.parseTableIdentifier(OffsetsDbStorage.OFFSETS_TABLE_NAME);
    createOrAmendOffsetTable(connection, tableId, dbStructure, config);
    String selectQueryStatement = buildSelectQuery(assignment);
    PreparedStatement preparedStatement = connection.prepareStatement(selectQueryStatement);
    int i = 0;
    for (TopicPartition a : assignment) {
      preparedStatement.setObject(i + 1, a.topic());
      preparedStatement.setObject(assignment.size() + i + 1, a.partition());
      i++;
    }
    log.info("Executing query: {}", preparedStatement);
    boolean offsetsFound = false;
    try (ResultSet rs = preparedStatement.executeQuery()) {
      while (rs.next()) {
        offsetsFound = true;
        result.put(new TopicPartition(rs.getString(3), rs.getInt(1)), rs.getLong(2));
      }
    }
    if (!offsetsFound) {
      for (TopicPartition a : assignment) {
        result.put(new TopicPartition(a.topic(), a.partition()), 0L);
      }
    }
    return result;
  }

  public void writeAndCommit(
      final Map<TopicPartition, OffsetAndMetadata> offsets
  ) throws SQLException {
    if (offsets.size() == 0) {
      log.info("No offsets are available for commit, skipping");
      return;
    }
    final Connection connection = cachedConnectionProvider.getConnection();
    for (Entry<TopicPartition, OffsetAndMetadata> offset : offsets.entrySet()) {
      final TableId tableId = dbDialect.parseTableIdentifier(OffsetsDbStorage.OFFSETS_TABLE_NAME);
      String upsertQueryStatement;
      try {
        upsertQueryStatement = dbDialect.buildUpsertQueryStatement(
            tableId,
            Arrays.asList(new ColumnId(tableId, OffsetsDbStorage.TOPIC_NAME_COLUMN_NAME),
                new ColumnId(tableId, OffsetsDbStorage.PARTITION_COLUMN_NAME)),
            Arrays.asList(new ColumnId(tableId, OffsetsDbStorage.OFFSET_COLUMN_NAME))
        );
      } catch (UnsupportedOperationException e) {
        throw new ConnectException(String.format(
            "Write to table '%s' in UPSERT mode is not supported with the %s dbDialect.",
            tableId,
            dbDialect.name()
        ));
      }
      createOrAmendOffsetTable(connection, tableId, dbStructure, config);
      PreparedStatement preparedStatement = connection.prepareStatement(upsertQueryStatement);
      preparedStatement.setString(1, offset.getKey().topic());
      preparedStatement.setInt(2, offset.getKey().partition());
      preparedStatement.setLong(3, offset.getValue().offset());
      preparedStatement.execute();
    }
    log.info("Committing offsets");
    connection.commit();
  }

  private String buildSelectQuery(Set<TopicPartition> assignment) {
    String baseSelectQueryStatement = "SELECT "
        + OffsetsDbStorage.PARTITION_COLUMN_NAME + ", "
        + OffsetsDbStorage.OFFSET_COLUMN_NAME + ", "
        + OffsetsDbStorage.TOPIC_NAME_COLUMN_NAME
        + " FROM " + OffsetsDbStorage.OFFSETS_TABLE_NAME
        + " WHERE " + OffsetsDbStorage.TOPIC_NAME_COLUMN_NAME + " IN (";

    StringJoiner joiner = new StringJoiner(
        ",",
        baseSelectQueryStatement,
        ") AND " + OffsetsDbStorage.PARTITION_COLUMN_NAME + " IN (");
    for (Object ignored : assignment) {
      joiner.add("?");
    }
    joiner = new StringJoiner(
        ",",
        joiner.toString(),
        ")");
    for (Object ignored : assignment) {
      joiner.add("?");
    }
    String selectQueryStatement = joiner.toString();
    return selectQueryStatement;
  }
}
