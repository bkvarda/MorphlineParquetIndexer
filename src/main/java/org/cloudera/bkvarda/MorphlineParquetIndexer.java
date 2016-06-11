/**
 * Created by bkvarda on 6/5/16.
 */
package org.cloudera.bkvarda;

import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.Record;
import java.io.File;
import java.io.IOException;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.Notifications;
import org.apache.log4j.BasicConfigurator;
import org.kitesdk.morphline.hadoop.parquet.avro.ReadAvroParquetFileBuilder;

public class MorphlineParquetIndexer {

    /** Usage: java ... <morphline.conf> <dataFile1> ... <dataFileN> */
    public static void main(String[] args) throws IOException {
        //Debug logging
        BasicConfigurator.configure();
        // compile morphline.conf file on the fly
        File morphlineFile = new File(args[0]);
        String morphlineId = "solrTest";
        MorphlineContext morphlineContext = new MorphlineContext.Builder().build();
        Command morphline = new Compiler().compile(morphlineFile, morphlineId, morphlineContext, null);

        // process each input data file
        Notifications.notifyBeginTransaction(morphline);
        try {
            for (int i = 1; i < args.length; i++) {
                String file = new String(args[i]);
                Record record = new Record();
                record.put(ReadAvroParquetFileBuilder.FILE_UPLOAD_URL,file);
                Notifications.notifyStartSession(morphline);
                boolean success = morphline.process(record);
                if (!success) {
                    System.out.println("Morphline failed to process record: " + record);

                }
            }
            Notifications.notifyCommitTransaction(morphline);
        } catch (RuntimeException e) {
            Notifications.notifyRollbackTransaction(morphline);
            morphlineContext.getExceptionHandler().handleException(e, null);
        }
        Notifications.notifyShutdown(morphline);
    }

}
