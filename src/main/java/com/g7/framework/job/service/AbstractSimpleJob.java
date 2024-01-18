package com.g7.framework.job.service;

import com.dangdang.ddframe.job.api.ShardingContext;
import com.dangdang.ddframe.job.api.simple.SimpleJob;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;
import com.g7.framework.feishu.webhook.FeiShuNotificationRobot;
import com.g7.framework.mybatis.transaction.TransactionWrapper;
import com.g7.framework.trace.SpanContext;
import com.g7.framework.trace.TraceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author dreamyao
 * @title 任务调度抽象类
 * @date 2019/10/23 2:22 AM
 * @since 1.0.0
 */
public abstract class AbstractSimpleJob extends TransactionWrapper implements SimpleJob {

    private static final Logger logger = LoggerFactory.getLogger(AbstractSimpleJob.class);
    @Autowired
    protected FeiShuNotificationRobot notificationRobot;
    
    @Override
    public void execute(ShardingContext shardingContext) {

        String traceId = TraceContext.getContext().genTraceIdAndSet();
        MDC.put("__X-TraceID__", traceId);
        String spanId = SpanContext.getContext().genSpanIdAndSet();
        MDC.put("SpanId", spanId);

        String simpleJobName = this.getClass().getSimpleName();
        Transaction transaction = Cat.newTransaction("SimpleJob", simpleJobName);
        transaction.addData("traceId", traceId);
        transaction.addData("spanId", spanId);
        try {

            doExecute(shardingContext);
            transaction.setStatus(Transaction.SUCCESS);

        } catch (Exception e) {

            Cat.logError(e);
            notificationRobot.sendText(String.format("%s 定时任务执行失败 ", simpleJobName) + e.getMessage());
            logger.error("[{}] 定时任务执行失败", simpleJobName, e);

        } finally {
            transaction.complete();
        }
    }

    protected abstract void doExecute(ShardingContext shardingContext);
}
