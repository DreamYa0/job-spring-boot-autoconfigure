package com.g7.framework.job;

import com.dangdang.ddframe.job.api.ShardingContext;
import com.dangdang.ddframe.job.api.simple.SimpleJob;
import com.g7.framework.job.rdb.JobEventRdbStorage;
import com.g7.framework.job.util.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Calendar;
import java.util.Date;

/**
 * @author dreamyao
 * @date 2018/11/16 上午11:08
 * 历史作业数据清理
 */
public class CleanHistoryDataJob implements SimpleJob {

    private static final Logger logger = LoggerFactory.getLogger(CleanHistoryDataJob.class);
    @Autowired
    private JobEventRdbStorage jobEventRdbStorage;

    @Override
    public void execute(ShardingContext shardingContext) {
        logger.debug("------------------------- clean history data job execute begin -------------------------");
        jobEventRdbStorage.deleteHistoryData(DateUtils.add(new Date(), Calendar.DAY_OF_MONTH,
                Integer.parseInt(shardingContext.getJobParameter())));
        logger.debug("------------------------- clean history data job execute end -------------------------");
    }
}
