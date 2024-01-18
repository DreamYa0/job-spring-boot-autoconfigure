package com.g7.framework.job.modle;


import com.g7.framework.job.properties.JobProperties;
import com.g7.framework.job.util.Constants;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@RequiredArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@Builder
public class JobConfigInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private int id;
    /**
     * 作业名称       db_column: JOB_NAME
     */
    private String jobName;
    /**
     * 作业类       db_column: JOB_CLASS
     */
    private String jobClass;
    /**
     * 作业类型       db_column: JOB_TYPE
     */
    private String jobType;
    /**
     * 分片数        db_column: SHARDING_TOTAL_COUNT
     */
    private int shardingTotalCount;
    /**
     * 分片参数       db_column: SHARDING_ITEM_PARAMETERS
     */
    private String shardingItemParameters;
    /**
     * 作业参数       db_column: JOB_PARAMETER
     */
    private String jobParameter;
    /**
     * CRON表达式       db_column: CRON
     */
    private String cron;
    /**
     * 自定义作业初始化类       db_column: JOB_INIT_CLASS
     */
    private String jobInitClass;
    /**
     * 是否支持故障转移       db_column: FAIL_OVER
     */
    @Builder.Default
    private boolean failover = true;
    /**
     * 是否开启错过任务重新执行       db_column: MIS_FIRE
     */
    @Builder.Default
    private boolean misfire = true;
    /**
     * 是否流式处理       db_column: IS_STREAM
     */
    private boolean streamingProcess;
    /**
     * 脚步作业执行命令行       db_column: COMMAND_LINE
     */
    private String scriptCommandLine;
    /**
     * 是否监控作业执行状态       db_column: IS_MONITOR
     */

    private boolean monitorExecution;
    /**
     * 监控端口       db_column: MONITOR_PORT
     */
    @Builder.Default
    private int monitorPort = -1;
    /**
     * 最大容忍本机与注册中心的时间误差秒数       db_column: MAX_TIME_DIFF_SECONDS
     */
    @Builder.Default
    private int maxTimeDiffSeconds = -1;
    /**
     * 作业服务器状态修复周期       db_column: RECONCILE_INTERVAL_MINUTES
     */
    @Builder.Default
    private int reconcileIntervalMinutes = 10;
    /**
     * 作业分片策略实现类全路径       db_column: JOB_SHARDING_STRATEGY_CLASS
     */
    private String jobShardingStrategyClass;
    /**
     * 事件追踪数据源ID       db_column: EVENT_TRACE_RDB_DATASOURCE
     */
    private String eventTraceRdbDatasource;
    /**
     * 是否覆盖zk信息       db_column: OVER_WRITER
     */
    @Builder.Default
    private boolean overwrite = true;
    /**
     * 是否启动时禁止       db_column: IS_DISABLED
     */
    private boolean disabled;
    /**
     * 监听类       db_column: LISTENER_CLASS
     */
    private String listenerClass;
    /**
     * 描述       db_column: DESCRIPTION
     */
    private String description;
    /**
     * 域       db_column: DOMAIN
     */
    private String domain;
    /**
     * 创建时间       db_column: GMT_CREATE
     */
    private java.util.Date gmtCreate;
    /**
     * 修改时间       db_column: GMT_MODIFIED
     */
    private java.util.Date gmtModified;
    /**
     * 自定义执行线程池
     */
    @Builder.Default
    private String executorServiceHandler = Constants.EXECUTOR_SERVICE_HANDLER;
    /**
     * 自定义异常类
     */
    @Builder.Default
    private String jobExceptionHandler = Constants.JOB_EXCEPTION_HANDLER;
    /**
     * 是否终止 0 否 1是
     */
    private boolean stop;
    /**
     * 为了zk监听到信息,反序列化使用
     */
    private JobProperties jobProperties = new JobProperties();
}

