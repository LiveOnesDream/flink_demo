package common.schemas;

import com.flink.entity.SensorReading;
import com.google.gson.Gson;
import com.sun.org.apache.bcel.internal.generic.NEW;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;

/**
 * @author zhangpeng.sun
 * @ClassName: SensorReadingSchema
 * @Description TODO
 * @date 2021/8/9 14:31
 */
public class SensorReadingSchema implements DeserializationSchema<SensorReading>, SerializationSchema<SensorReading> {

    private static final Gson gson = new Gson();

    @Override
    public SensorReading deserialize(byte[] bytes) throws IOException {
        return gson.fromJson(new String(bytes), SensorReading.class);
    }

    @Override
    public boolean isEndOfStream(SensorReading sensorReading) {
        return false;
    }

    @Override
    public byte[] serialize(SensorReading sensorReading) {
        return gson.toJson(sensorReading).getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public TypeInformation<SensorReading> getProducedType() {
        return TypeInformation.of(SensorReading.class);
    }
}
