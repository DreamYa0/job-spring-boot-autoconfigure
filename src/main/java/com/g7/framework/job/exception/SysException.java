package com.g7.framework.job.exception;


import com.g7.framework.job.definition.ICodeMessage;

/**
 * @author dreamyao
 */
public class SysException extends Exception implements ICodeMessage {

    private static final long serialVersionUID = 1L;

    private String code;
    private String message;


    public SysException(ICodeMessage codeMessage) {
        this.code = codeMessage.getCode();
        this.message = codeMessage.getMessage();
    }

    public SysException(ICodeMessage codeMessage, Throwable cause) {

        super(codeMessage.getMessage(), cause);
        this.code = code;
        this.message = codeMessage.getMessage();
    }

    public SysException(String message, Throwable cause) {
        super(message, cause);
    }

    public SysException(String message) {
        super(message);
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    @Override
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
