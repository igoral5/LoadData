package com.example;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by igor on 03.03.17.
 */
public class FilterMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    private final DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd");

    private final Set<String> permittedTypes = new HashSet<String>(Arrays.asList("pay", "rec"));

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (validate(value.toString())) {
            context.write(key, value);
        }
    }

    protected boolean validate(String line) {
        try {
            Reader in = new StringReader(line);
            CSVParser parser = new CSVParser(in, CSVFormat.DEFAULT);
            List<CSVRecord> list = parser.getRecords();
            if (list.size() != 1) {
                return false;
            }
            CSVRecord record = list.get(0);
            if (record.size() != 4) {
                return false;
            }
            int id = Integer.parseInt(record.get(0));
            if (id < 0) {
                return false;
            }
            if (!permittedTypes.contains(record.get(1).toLowerCase())) {
                return false;
            }
            int value = Integer.parseInt(record.get(2));
            DateTime business_date = fmt.parseDateTime(record.get(3));
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
