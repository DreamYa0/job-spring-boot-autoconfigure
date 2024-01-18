package com.g7.framework.job.exception;

import com.g7.framework.job.definition.ICodeMessage;

/**
 * @author dreamyao
 * @date 2018/11/19 上午11:16
 */
public class JobException extends SysException {

    public JobException(ICodeMessage codeMessage) {
        super(codeMessage);
    }

    public JobException(ICodeMessage codeMessage, Throwable cause) {
        super(codeMessage, cause);
    }
}
