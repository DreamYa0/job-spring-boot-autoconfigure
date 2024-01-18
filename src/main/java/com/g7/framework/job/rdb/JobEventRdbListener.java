package com.g7.framework.job.rdb;

import com.dangdang.ddframe.job.event.JobEventListener;
import com.dangdang.ddframe.job.event.rdb.JobEventRdbIdentity;
import com.dangdang.ddframe.job.event.type.JobExecutionEvent;
import com.dangdang.ddframe.job.event.type.JobStatusTraceEvent;

/**
 * Created by dreamyao on 2018/11/13.
 */
public final class JobEventRdbListener extends JobEventRdbIdentity implements JobEventListener {

    private final JobEventRdbStorage repository;

    public JobEventRdbListener(JobEventRdbStorage repository) {
        this.repository = repository;
    }

    @Override
    public void listen(JobExecutionEvent jobExecutionEvent) {
        repository.addJobExecutionEvent(jobExecutionEvent);
    }

    @Override
    public void listen(JobStatusTraceEvent jobStatusTraceEvent) {
        repository.addJobStatusTraceEvent(jobStatusTraceEvent);
    }
}
