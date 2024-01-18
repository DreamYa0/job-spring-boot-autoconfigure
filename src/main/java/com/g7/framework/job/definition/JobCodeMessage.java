package com.g7.framework.job.definition;

/**
 * @author dreamyao
 * @Date: 2018/11/19 上午11:18
 */
public enum JobCodeMessage implements ICodeMessage {

    STRATEGY_ERROR("1001", "DYNAMIC_SHARDING_ERROR"),
    SPRING_JOB_SCHEDULER_CONFIG_ERROR("2001", "SPRING_SCHEDULER_CONFIG_ERROR"),
    JOB_SCHEDULER_CONFIG_ERROR("3001", "JOB_SCHEDULER_CONFIG_ERROR");

    private String code;
    private String message;

    JobCodeMessage(String code, String message) {
        this.code = code;
        this.message = message;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getMessage() {
        return message;
    }
}
