package com.g7.framework.job.service;

import com.g7.framework.job.modle.JobConfigInfo;

/**
 * Created by dreamyao on 2018/11/10.
 * 动态分片接口,需要依赖db数据来决定分片,需要实现此接口
 */
public interface DynamicStrategySharding {

    /**
     * 分片与分片参数配置
     * @param job 待分片的job信息
     * @return 分片后job信息
     */
    JobConfigInfo configuration(JobConfigInfo job);
}
