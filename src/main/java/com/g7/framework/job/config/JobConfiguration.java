package com.g7.framework.job.config;

import com.dangdang.ddframe.job.config.JobCoreConfiguration;
import com.dangdang.ddframe.job.config.JobTypeConfiguration;
import com.dangdang.ddframe.job.config.dataflow.DataflowJobConfiguration;
import com.dangdang.ddframe.job.config.script.ScriptJobConfiguration;
import com.dangdang.ddframe.job.config.simple.SimpleJobConfiguration;
import com.dangdang.ddframe.job.executor.handler.JobProperties;
import com.dangdang.ddframe.job.lite.config.LiteJobConfiguration;
import com.g7.framework.job.modle.JobConfigInfo;
import com.g7.framework.job.util.Constants;
import org.springframework.util.StringUtils;

/**
 * @author dreamyao
 * @date 2018/11/18 下午8:19
 */
public class JobConfiguration {

    public static LiteJobConfiguration configuration(JobConfigInfo job) {

        String executorServiceHandler = job.getExecutorServiceHandler();
        if (StringUtils.isEmpty(executorServiceHandler)) {
            executorServiceHandler = Constants.EXECUTOR_SERVICE_HANDLER;
        }

        String jobExceptionHandler = job.getJobExceptionHandler();
        if (StringUtils.isEmpty(jobExceptionHandler)) {
            jobExceptionHandler = Constants.JOB_EXCEPTION_HANDLER;
        }

        JobCoreConfiguration coreConfig = JobCoreConfiguration.newBuilder(job.getJobName(), job.getCron(),
                job.getShardingTotalCount())
                .shardingItemParameters(job.getShardingItemParameters())
                .description(job.getDescription())
                .failover(job.isFailover())
                .jobParameter(job.getJobParameter())
                .misfire(job.isMisfire())
                .jobProperties(JobProperties.JobPropertiesEnum.EXECUTOR_SERVICE_HANDLER.getKey(),
                        executorServiceHandler)
                .jobProperties(JobProperties.JobPropertiesEnum.JOB_EXCEPTION_HANDLER.getKey(),
                        jobExceptionHandler)
                .build();

        JobTypeConfiguration typeConfig;

        switch (job.getJobType()) {

            case Constants.JOB_TYPE_SIMPLE:
                typeConfig = new SimpleJobConfiguration(coreConfig, job.getJobClass());
                break;
            case Constants.JOB_TYPE_DATA_FLOW:
                typeConfig = new DataflowJobConfiguration(coreConfig, job.getJobClass(), job.isStreamingProcess());
                break;
            case Constants.JOB_TYPE_SCRIPT:
                typeConfig = new ScriptJobConfiguration(coreConfig, job.getScriptCommandLine());
                break;
            default:
                typeConfig = new SimpleJobConfiguration(coreConfig, job.getJobClass());
                break;
        }

        LiteJobConfiguration.Builder builder = LiteJobConfiguration.newBuilder(typeConfig);
        builder.overwrite(job.isOverwrite())
                .disabled(job.isDisabled())
                .monitorPort(job.getMonitorPort())
                .monitorExecution(job.isMonitorExecution())
                .maxTimeDiffSeconds(job.getMaxTimeDiffSeconds())
                .reconcileIntervalMinutes(job.getReconcileIntervalMinutes())
                .jobShardingStrategyClass(job.getJobShardingStrategyClass());

        return builder.build();
    }
}
