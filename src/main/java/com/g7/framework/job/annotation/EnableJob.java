package com.g7.framework.job.annotation;

import com.g7.framework.job.JobParserAutoConfiguration;
import com.g7.framework.job.util.SpringUtils;
import org.springframework.context.annotation.Import;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@Import({SpringUtils.class, JobParserAutoConfiguration.class})
public @interface EnableJob {

}