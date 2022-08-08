package com.project.front;

public class BusinessException extends RuntimeException {
    public BusinessException() {
    }

    public BusinessException(String message) {
        super(message);
    }

    public BusinessException(Throwable cause) {
        super("", cause);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
