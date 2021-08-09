package utils;

import com.flink.entity.PropertiesConstants;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * @author zhangpeng.sun
 * @ClassName: ExecutionEnvUtil
 * @Description TODO
 * @date 2021/8/6 11:07
 */
public class ExecutionEnvUtil {

    public static ParameterTool createParameterTool(final String[] args) throws Exception {
        return ParameterTool
                .fromPropertiesFile(ExecutionEnvUtil.class.getResourceAsStream(PropertiesConstants.PROPERTIES_FILE_NAME))
                .mergeWith(ParameterTool.fromArgs(args))
                .mergeWith(ParameterTool.fromSystemProperties());
    }

    public static final ParameterTool PARAMETER_TOOL = createParameterTool();

    private static ParameterTool createParameterTool() {
        try {
            return ParameterTool
                    .fromPropertiesFile(ExecutionEnvUtil.class.getResourceAsStream(PropertiesConstants.PROPERTIES_FILE_NAME))
                    .mergeWith(ParameterTool.fromSystemProperties());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ParameterTool.fromSystemProperties();
    }

    public static StreamExecutionEnvironment prepare(ParameterTool parameterTool) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(parameterTool.getInt(PropertiesConstants.STREAM_PARALLELISM, 5));
        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 60000));
        //启动checkpoint
        if (parameterTool.getBoolean(PropertiesConstants.STREAM_CHECKPOINT_ENABLE, true)) {
            //checkpoint 间隔周期
            env.enableCheckpointing(parameterTool.getLong(PropertiesConstants.STREAM_CHECKPOINT_INTERVAL, 10000));
        }
        //全局参数配置
        env.getConfig().setGlobalJobParameters(parameterTool);
        //事件time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        return env;
    }
}
