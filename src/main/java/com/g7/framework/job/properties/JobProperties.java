package com.g7.framework.job.properties;

import com.g7.framework.job.util.Constants;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class JobProperties {

    /**
     * 自定义异常处理类
     */
    private String job_exception_handler = Constants.JOB_EXCEPTION_HANDLER;

    /**
     * 自定义业务处理线程池
     */
    private String executor_service_handler = Constants.EXECUTOR_SERVICE_HANDLER;

}
