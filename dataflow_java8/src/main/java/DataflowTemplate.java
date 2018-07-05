import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DataflowTemplate {

    private static final String SCHEMA = "GlobalRank:INTEGER,TldRank:INTEGER,Domain:STRING,TLD:STRING,RefSubNets:STRING,RefIPs:STRING," +
            "IDN_Domain:STRING,IDN_TLD:STRING,PrevGlobalRank:INTEGER,PrevTldRank:INTEGER,PrevRefSubNets:STRING,PrevRefIPs:STRING";
    private static final List<String> cols = Arrays.asList(SCHEMA.split(","));

    public interface MyPipelineOptions extends PipelineOptions {
        @Description("Path of the file to read from")
        String getInputFile();
        void setInputFile(String value);

        @Description("Table name")
        String getTableName();
        void setTableName(String value);
    }


    public static void main(String[] args) {

        MyPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(MyPipelineOptions.class);
        run(options);
    }

    static void run(MyPipelineOptions options) {
        Pipeline p = Pipeline.create(options);

        p.apply("Read file", TextIO.read().from(options.getInputFile()))
                .apply("String to Table Row", new StringToTableRow())
                .apply("Write to BQ", BigQueryIO.writeTableRows()
                        .to(options.getTableName())
                        .withSchema(createTableSchema())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

        // Run the pipeline.
        p.run().waitUntilFinish();
    }

    public static class StringToTableRow extends PTransform<PCollection<String>, PCollection<TableRow>> {
        @Override
        public PCollection<TableRow> expand(PCollection<String> lines) {
            return lines
                    .apply("Parse CSV", ParDo.of(new Split()))
                    .apply("As table rows", ParDo.of(new ToTableRow()));
        }
    }


    /**
     * ParDo -- Converts lines to maps
     */
    public static class Split extends DoFn<String, Map<String, String>> {

        private static List<String> keys = cols.stream().map(item -> item.split(":")[0]).collect(Collectors.toList());

        @ProcessElement
        public void processElement(ProcessContext c) {
            List<String> values = Arrays.asList(c.element().split(","));
            //Create Map from two lists
            Map<String, String> map = IntStream.range(0, Math.min(keys.size(), values.size()))
                    .boxed()
                    .collect(Collectors.toMap(keys::get, values::get));

            c.output(map);
        }
    }

    /**
     * ParDo -- Create TableRows from maps
     */
    public static class ToTableRow extends DoFn<Map<String, String>, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Map<String, String> map = c.element();
            TableRow tableRow = new TableRow();
            if(map.get("GlobalRank").equals("GlobalRank")) {
                System.out.println("Ignoring header row");
            } else {
                map.forEach(tableRow::set);
                c.output(tableRow);
            }
        }
    }

    private static TableSchema createTableSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        Map<String, String> bqCols = cols.stream().map(item -> item.split(":")).collect(Collectors.toMap(a -> a[0], a -> a[1]));
        bqCols.forEach((k,v) -> fields.add(new TableFieldSchema().setName(k).setType(v)));
        return new TableSchema().setFields(fields);
    }
}