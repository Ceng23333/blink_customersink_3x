package com.alibaba.blink.customersink;

import com.dmetasoul.lakesoul.meta.DBConfig;
import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.DBUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.*;

import static com.dmetasoul.lakesoul.lakesoul.local.LakeSoulLocalJavaWriter.TABLE_NAME;

public class LakeSoulSinkEntry {
    private static class ArrowTypeMockDataGenerator
            implements ArrowType.ArrowTypeVisitor<Object> {

        long count = 0;

        static long mod = 511;

        public static final ArrowTypeMockDataGenerator INSTANCE = new ArrowTypeMockDataGenerator();

        @Override
        public Object visit(ArrowType.Null aNull) {
            return null;
        }

        @Override
        public Object visit(ArrowType.Struct struct) {
            return null;
        }

        @Override
        public Object visit(ArrowType.List list) {
            return null;
        }

        @Override
        public Object visit(ArrowType.LargeList largeList) {
            return null;
        }

        @Override
        public Object visit(ArrowType.FixedSizeList fixedSizeList) {
            return null;
        }

        @Override
        public Object visit(ArrowType.Union union) {
            return null;
        }

        @Override
        public Object visit(ArrowType.Map map) {
            return null;
        }

        @Override
        public Object visit(ArrowType.Int type) {
            int bitWidth = type.getBitWidth();
            long value = (count++) % mod;
            if (bitWidth <= 8) return (byte) value;
            if (bitWidth <= 2 * 8) return (short) value;
            if (bitWidth <= 4 * 8) return (int) value;
            return value;
        }

        @Override
        public Object visit(ArrowType.FloatingPoint type) {
            double value = ((double) (count++)) / mod;
            switch (type.getPrecision()) {
                case HALF:
                case SINGLE:
                    return (float) value;
            }
            return value;
        }

        @Override
        public Object visit(ArrowType.Utf8 utf8) {
            return String.valueOf((count++) % mod);
        }

        @Override
        public Object visit(ArrowType.LargeUtf8 largeUtf8) {
            return String.valueOf((count++) % mod);
        }

        @Override
        public Object visit(ArrowType.Binary binary) {
            return String.valueOf((count++) % mod).getBytes();
        }

        @Override
        public Object visit(ArrowType.LargeBinary largeBinary) {
            return String.valueOf((count++) % mod).getBytes();
        }

        @Override
        public Object visit(ArrowType.FixedSizeBinary fixedSizeBinary) {
            return String.valueOf((count++) % mod).getBytes();
        }

        @Override
        public Object visit(ArrowType.Bool bool) {
            return (count++) % 2 == 0;
        }

        @Override
        public Object visit(ArrowType.Decimal decimal) {
            return new BigDecimal(((double) (count++)) / mod).setScale(decimal.getScale(), BigDecimal.ROUND_UP);
        }

        @Override
        public Object visit(ArrowType.Date date) {
            return (int) ((count++) % mod);
        }

        @Override
        public Object visit(ArrowType.Time time) {
            long value = (count++) % mod;
            return new Timestamp(value * 1_000_000_000);
        }

        @Override
        public Object visit(ArrowType.Timestamp timestamp) {
            long value = (count++) % mod;
            return new java.sql.Timestamp(value * 1000); // 将秒转换为毫秒
        }

        @Override
        public Object visit(ArrowType.Interval interval) {
            return null;
        }

        @Override
        public Object visit(ArrowType.Duration duration) {
            return null;
        }
    }

    public static void main(String[] args) throws IOException {
        DBManager meta = new DBManager();

        boolean cdc = true;
        meta.cleanMeta();
        String tableId = "table_" + UUID.randomUUID();
        List<String> primaryKeys = Arrays.asList("id");
        List<String> partitionKeys = Arrays.asList("range");
//        List<String> partitionKeys = Collections.emptyList();
        String partition = DBUtil.formatTableInfoPartitionsField(
                primaryKeys,
                partitionKeys
        );

        List<Field> fields;
        if (cdc) {
            fields = Arrays.asList(
                    new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)
                    , new Field("range", FieldType.nullable(new ArrowType.Int(32, true)), null)
                    , new Field("int", FieldType.nullable(new ArrowType.Int(32, true)), null)
                    , new Field("utf8", FieldType.nullable(new ArrowType.Utf8()), null)
                    , new Field("decimal", FieldType.nullable(ArrowType.Decimal.createDecimal(10, 3, null)), null)
                    , new Field("boolean", FieldType.nullable(new ArrowType.Bool()), null)
                    , new Field("date", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null)
                    , new Field("datetimeSec", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.SECOND, ZoneId.of("UTC").toString())), null)
                    , new Field(DBConfig.TableInfoProperty.CDC_CHANGE_COLUMN_DEFAULT, FieldType.notNullable(new ArrowType.Utf8()), null)
                    , new Field("datetimeMilli", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, ZoneId.of("UTC").toString())), null)
            );
        } else {
            fields = Arrays.asList(
                    new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)
                    , new Field("range", FieldType.nullable(new ArrowType.Int(32, true)), null)
                    , new Field("int", FieldType.nullable(new ArrowType.Int(32, true)), null)
                    , new Field("utf8", FieldType.nullable(new ArrowType.Utf8()), null)
                    , new Field("decimal", FieldType.nullable(ArrowType.Decimal.createDecimal(10, 3, null)), null)
                    , new Field("boolean", FieldType.nullable(new ArrowType.Bool()), null)
                    , new Field("date", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null)
                    , new Field("datetimeSec", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.SECOND, ZoneId.of("UTC").toString())), null)
                    , new Field("datetimeMilli", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, ZoneId.of("UTC").toString())), null)
            );
        }
        Schema schema = new Schema(fields);

        ObjectMapper objectMapper = new ObjectMapper();

        Map<String, String> properties = new HashMap<>();
        if (!primaryKeys.isEmpty()) {
            properties.put(DBConfig.TableInfoProperty.HASH_BUCKET_NUM, "4");
            properties.put("hashPartitions",
                    String.join(DBConfig.LAKESOUL_HASH_PARTITION_SPLITTER, primaryKeys));
            if (cdc) {
                properties.put(DBConfig.TableInfoProperty.USE_CDC, "true");
                properties.put(DBConfig.TableInfoProperty.CDC_CHANGE_COLUMN,
                        DBConfig.TableInfoProperty.CDC_CHANGE_COLUMN_DEFAULT);
            }
        }

        meta.createTable(
                tableId,
                "default",
                "test_local_java_table",
                "file:/tmp/test_local_java_table",
                schema.toJson(),
                objectMapper.writeValueAsString(properties),
                partition);

        LakeSoulSink sink = new LakeSoulSink();

        Map<String, String> params = new HashMap<>();
        params.put("lakesoul.pg.url", "jdbc:postgresql://127.0.0.1:5433/test_lakesoul_meta?stringtype=unspecified");
        params.put("lakesoul.pg.username", "yugabyte");
        params.put("lakesoul.pg.password", "yugabyte");
        params.put(TABLE_NAME, "test_local_java_table");

        sink.setUserParamsMap(params);

        sink.open(1,1);

        int times = 8;
        int ranges = 13;
        for (int t = 0; t < times; t++) {
            int numRows = 1024;
            int numCols = fields.size();
            for (int i = 0; i < numRows; i++) {
                Object[] objects = new Object[cdc ? numCols - 1 : numCols];
                for (int j = 0, k = 0; j < numCols; j++) {
                    if (fields.get(j).getName().contains(DBConfig.TableInfoProperty.CDC_CHANGE_COLUMN_DEFAULT)) {
                        continue;
                    } else if (fields.get(j).getName().contains("id")) {
                        objects[k++] = i;
                    } else if (fields.get(j).getName().contains("range")) {
                        objects[k++] = i % ranges;
                    } else {
                        objects[k++] = fields.get(j).getType().accept(ArrowTypeMockDataGenerator.INSTANCE);
                    }
                }
                Row row = Row.of(objects);
                sink.writeAddRecord(row);
                if (cdc && i % 7 == 0) {
                    sink.writeDeleteRecord(row);
                }
            }
            sink.sync();
        }


        assert meta.getAllPartitionInfo(tableId).size() == times;

        System.out.println("data commit DONE");
        sink.close();
    }
}
