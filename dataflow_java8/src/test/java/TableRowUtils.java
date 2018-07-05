import com.google.api.services.bigquery.model.TableRow;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TableRowUtils {

    public static class rowAsStringTransform extends DoFn<TableRow, String> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            c.output(rowAsString(c.element()));
        }
    }

    public static String rowAsString(TableRow row) {
        List<String> entries = Lists.newArrayListWithCapacity(row.size());
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            entries.add(entry.getKey() + ":" + entry.getValue());
        }
        Collections.sort(entries);
        return Joiner.on(",").join(entries);
    }
}
