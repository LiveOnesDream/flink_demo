package demo.checkpoint;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhangpeng.sun
 * @ClassName: CheckpointAPI
 * @Description TODO
 * @date 2021/7/28 18:09
 */
public class CheckpointAPI {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //第一种启用checkpoint的方式
        env.enableCheckpointing(1000, CheckpointingMode.AT_LEAST_ONCE);

        //第二种启用checkpoint的方式
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(500);

        /**设置检查点尝试之间的最小间隔。此设置定义检查点协调器在可能触发另一个检查点（相对于最大并发检查点数）后多久可以触发另一个检查点
         * 比如下面的设置，下一个检查点将在上一个检查点完成后，不超过5s内启动
         * 这个参数跟检查点生成的间隔时间是有点冲突的，这个参数不考虑上面提到的检查点持续时间和检查点间隔。或者说设置下面的参数以后，检查点自动就变成5s了，
         * 如果觉得5s不够可以再将间隔设置的大一点，但是不能小于5s了
         * 设置检查点的interval和设置检查点之间的间隔时间有啥不同呢？
         * interval是物理时间的间隔，即时间只要过去1s了，就会生成一个检查点。但是设置检查点之间的间隔是说检查点完成1s了就会设置间隔，这个是跟检查点完成时间相关的
         * 比如存储检查点的系统比较慢，完成一个检查点平均10s，然后下面检查点之间的间隔设置为5s，那么两个检查点生成的时间间隔就是15s
         * 再简单点说，interval设置的是两个检查点生成时刻的间隔，而下面参数设置的是第一个检查点结束和第二个检查点创建（还没有结束）之间的间隔
         **/
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        //设置检查点最大并发数，表示只能有一个检查点，当这个检查点完成以后才可能产生下一个检查点，也是默认的参数。
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        /**
         * ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:表示取消作业是保留检查点，这里说的取消是正常取消不是任务失败。如果重启任务，检查点不会自动清除，如果需要清除则需要手动清除。
         * ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION:表示取消作业时删除检查点，如果任务失败，检查点不会删除。也就是说任务失败可以从检查点恢复任务。
         **/
        //设置外部检查点，可以将检查点的元数据信息定期写入外部系统，job失败时检查点不会被消除，并且可以从该检查点恢复job。
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //检查点和保存点都可以用于容灾，该配置是指时间更近的保存点。
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        // 设置使用非对齐检查点。它可以极大减少背压情况下检查点的时间，但是只有在精确一次的检查点并且允许的最大检查点并发数量为1的情况下才能使用
        //env.getCheckpointConfig().enableUnalignedCheckpoints();


    }
}
