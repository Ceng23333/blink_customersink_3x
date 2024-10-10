package com.alibaba.blink.customersink;


import com.alibaba.blink.streaming.connector.custom.api.CustomSinkBase;
import com.dmetasoul.lakesoul.lakesoul.local.LakeSoulLocalJavaWriter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LakeSoulSink extends CustomSinkBase {
    private static Logger LOG = LoggerFactory.getLogger(LakeSoulSink.class);
    private LakeSoulLocalJavaWriter lakeSoulWriter;

    public void open(int taskNumber, int numTasks) throws IOException{
        LOG.info(String.format("Open Method Called: taskNumber %d numTasks %d", taskNumber, numTasks));

        lakeSoulWriter = new LakeSoulLocalJavaWriter();
        lakeSoulWriter.init(userParamsMap);

    }

    public void close() throws IOException {
        if (lakeSoulWriter != null) {
            try {
                lakeSoulWriter.close();
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void writeAddRecord(Row row) throws IOException {
        int arity = row.getArity();
        Object[] objRow = new Object[arity];
        for (int i = 0; i < arity; i++) {
            objRow[i] = row.getField(i);
        }
        lakeSoulWriter.writeAddRow(objRow);
    }

    public void writeDeleteRecord(Row row) throws IOException{
        int arity = row.getArity();
        Object[] objRow = new Object[arity];
        for (int i = 0; i < arity; i++) {
            objRow[i] = row.getField(i);
        }
        lakeSoulWriter.writeDeleteRow(objRow);
    }

    public void sync() throws IOException{
        try {
            lakeSoulWriter.commit();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String getName() {
        return "LakeSoulSink";
    }

}