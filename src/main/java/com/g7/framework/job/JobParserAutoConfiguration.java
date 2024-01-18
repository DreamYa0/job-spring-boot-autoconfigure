package com.g7.framework.job;

import com.dangdang.ddframe.job.reg.zookeeper.ZookeeperConfiguration;
import com.dangdang.ddframe.job.reg.zookeeper.ZookeeperRegistryCenter;
import com.g7.framework.job.properties.ZookeeperProperties;
import com.g7.framework.job.rdb.JobEventRdbConfiguration;
import com.g7.framework.job.rdb.JobEventRdbSearch;
import com.g7.framework.job.rdb.JobEventRdbStorage;
import com.g7.framework.job.service.ElasticJobService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

import javax.sql.DataSource;

/**
 * 任务自动配置
 */
@EnableConfigurationProperties(ZookeeperProperties.class)
public class JobParserAutoConfiguration implements EnvironmentAware {

    @Autowired
    private ZookeeperProperties zookeeperProperties;
    private Environment environment;

    /**
     * 初始化Zookeeper注册中心
     * @return
     */
    @Bean(initMethod = "init")
    public ZookeeperRegistryCenter zookeeperRegistryCenter() {
        ZookeeperConfiguration zkConfig = new ZookeeperConfiguration(zookeeperProperties.getServers(),
                zookeeperProperties.getNamespace());
        zkConfig.setBaseSleepTimeMilliseconds(zookeeperProperties.getBaseSleepTimeMilliseconds());
        zkConfig.setConnectionTimeoutMilliseconds(zookeeperProperties.getConnectionTimeoutMilliseconds());
        zkConfig.setDigest(zookeeperProperties.getDigest());
        zkConfig.setMaxRetries(zookeeperProperties.getMaxRetries());
        zkConfig.setMaxSleepTimeMilliseconds(zookeeperProperties.getMaxSleepTimeMilliseconds());
        zkConfig.setSessionTimeoutMilliseconds(zookeeperProperties.getSessionTimeoutMilliseconds());
        return new ZookeeperRegistryCenter(zkConfig);
    }

    /**
     * 初始化Zookeeper注册中心
     * @return
     */
    @Bean(initMethod = "init",name = "cleanJob")
    public ZookeeperRegistryCenter cleanJobZookeeperRegistryCenter() {
        ZookeeperConfiguration zkConfig = new ZookeeperConfiguration(zookeeperProperties.getServers(),
                "elastic-clean-job");
        zkConfig.setBaseSleepTimeMilliseconds(zookeeperProperties.getBaseSleepTimeMilliseconds());
        zkConfig.setConnectionTimeoutMilliseconds(zookeeperProperties.getConnectionTimeoutMilliseconds());
        zkConfig.setDigest(zookeeperProperties.getDigest());
        zkConfig.setMaxRetries(zookeeperProperties.getMaxRetries());
        zkConfig.setMaxSleepTimeMilliseconds(zookeeperProperties.getMaxSleepTimeMilliseconds());
        zkConfig.setSessionTimeoutMilliseconds(zookeeperProperties.getSessionTimeoutMilliseconds());
        return new ZookeeperRegistryCenter(zkConfig);
    }

    @Bean
    public JobEventRdbConfiguration jobEventRdbConfiguration(@Qualifier(value = "elastic-job") DataSource dataSource) {
        return new JobEventRdbConfiguration(dataSource);
    }

    @Bean
    public JobEventRdbSearch jobEventRdbSearch(@Qualifier(value = "elastic-job") DataSource dataSource) {
        return new JobEventRdbSearch(dataSource);
    }

    @Bean
    public JobEventRdbStorage jobEventRdbStorage(@Qualifier(value = "elastic-job") DataSource dataSource) {
        return new JobEventRdbStorage(dataSource);
    }

    @Bean
    public ElasticJobService jobService() {
        return new ElasticJobService();
    }

    @Bean
    public CleanHistoryDataJob cleanHistoryDataJob() {
        return new CleanHistoryDataJob();
    }

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }
}
