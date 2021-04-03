package xiaoyf.demo.avrokafka.avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

public class GenericDemo {
    public static void main(String[] args) throws IOException {
        Schema schema = new Parser().parse(new File("./src/main/avro/user.avsc"));

        GenericRecord user1 = new GenericData.Record(schema);
        user1.put("experience", "Strong");
        // Leave favorite color null

        GenericRecord user2 = new GenericData.Record(schema);
        user2.put("experience", 10L);

        // Serialize user1 and user2 to disk
        File file = new File("users.avro");
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.create(schema, file);
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.close();

        // Deserialize users from disk
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        GenericRecord user = null;
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader)) {
            while (dataFileReader.hasNext()) {
                // Reuse user object by passing it to next(). This saves us from
                // allocating and garbage collecting many objects for files with
                // many items.
                user = dataFileReader.next(user);
                Object experience = user.get("experience");
                System.out.printf("experience, type=%s, value=%s\n", experience.getClass().getName(), experience);
            }
        }


    }
}
