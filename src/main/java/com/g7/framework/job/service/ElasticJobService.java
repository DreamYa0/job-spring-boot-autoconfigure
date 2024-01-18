package com.g7.framework.job.service;

import com.dangdang.ddframe.job.api.ElasticJob;
import com.dangdang.ddframe.job.lite.api.JobScheduler;
import com.dangdang.ddframe.job.lite.api.listener.ElasticJobListener;
import com.dangdang.ddframe.job.lite.spring.api.SpringJobScheduler;
import com.dangdang.ddframe.job.reg.zookeeper.ZookeeperRegistryCenter;
import com.g7.framework.job.config.JobConfiguration;
import com.g7.framework.job.definition.JobCodeMessage;
import com.g7.framework.job.exception.JobException;
import com.g7.framework.job.modle.JobConfigInfo;
import com.g7.framework.job.rdb.JobEventRdbConfiguration;
import com.g7.framework.job.util.CommUtils;
import com.g7.framework.job.util.Constants;
import com.g7.framework.job.util.SpringUtils;
import com.g7.framwork.common.util.json.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.util.CollectionUtils;

import java.util.List;

@Slf4j
public class ElasticJobService implements ApplicationListener<ApplicationReadyEvent> {

    @Autowired
    private JobEventRdbConfiguration jobEventRdbConfiguration;
    @Autowired
    private ZookeeperRegistryCenter zookeeperRegistryCenter;
    @Autowired
    @Qualifier(value = "cleanJob")
    private ZookeeperRegistryCenter cleanJobZookeeperRegistryCenter;

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        // 监听当前业务namespace定时任务动态创建事件
        monitorJobRegister();
        // 注册当前业务namespace下调度平台上创建的所有定时任务
        startReleaseJobFromZookeeper();
        // 专有的日志清理namespace下创建日志清理任务
        createCleanJob();
    }

    /**
     * 开启任务监听,当有任务添加时，监听zk中的数据增加，自动在其他节点也初始化该任务
     */
    private void monitorJobRegister() {
        try {
            CuratorFramework client = zookeeperRegistryCenter.getClient();
            PathChildrenCache childrenCache = new PathChildrenCache(client, "/", true);
            childrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
            PathChildrenCacheListener childrenCacheListener = new PathChildrenCacheListener() {
                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                    ChildData data = event.getData();
                    switch (event.getType()) {
                        case CHILD_ADDED:
                            String config = new String(client.getData().forPath(data.getPath() + "/config"));
                            log.debug("JobRegister configData [{}] ", config);
                            JobConfigInfo job = JsonUtils.fromJson(config, JobConfigInfo.class);
                            createJob(job,zookeeperRegistryCenter);
                            break;
                        default:
                            break;
                    }
                }
            };
            childrenCache.getListenable().addListener(childrenCacheListener);
        } catch (Exception e) {
            log.error("Dynamic job registration failed", e);
        }
    }

    /**
     * 启动发布，每次启动时从新创建zookeeper上已存在记录的job
     * 进行job创建
     */
    private void startReleaseJobFromZookeeper() {
        CuratorFramework client = zookeeperRegistryCenter.getClient();
        String namespace = client.getNamespace();
        try {

            // 拉取当前namespace下的所有一级节点的节点名称
            List<String> nodePaths = client.getChildren().forPath("/");

            if (Boolean.FALSE.equals(CollectionUtils.isEmpty(nodePaths))) {

                for (String nodePath : nodePaths) {

                    if (nodePath.contains(Constants.CLEAN_JOB_NAME)) {
                        // 业务项目namespace下不创建日志清理任务
                        continue;
                    }

                    try {
                        String jobInfo = new String(client.getData().forPath("/" + nodePath + "/config"));
                        JobConfigInfo job = JsonUtils.fromJson(jobInfo, JobConfigInfo.class);
                        createJob(job, zookeeperRegistryCenter);
                    } catch (Exception e) {
                        log.error("Pull node path job info form zookeeper namespace failed, namespace is {} , " +
                                "node path is {}", namespace, nodePath, e);
                    }
                }
            }

        } catch (Exception e) {
            log.error("Pull all job info form zookeeper namespace failed, namespace is {}", namespace, e);
        }
    }

    private void createCleanJob() {

        CuratorFramework client = cleanJobZookeeperRegistryCenter.getClient();
        String namespace = client.getNamespace();

        try {

            // 拉取当前namespace下的所有一级节点的节点名称
            List<String> nodePaths = client.getChildren().forPath("/");

            if (Boolean.FALSE.equals(CollectionUtils.isEmpty(nodePaths))) {

                for (String nodePath : nodePaths) {

                    if (nodePath.contains(Constants.CLEAN_JOB_NAME)) {
                        // 日志清理namespace下创建日志清理任务
                        String jobInfo = new String(client.getData().forPath("/" + nodePath + "/config"));
                        JobConfigInfo job = JsonUtils.fromJson(jobInfo, JobConfigInfo.class);
                        createJob(job, cleanJobZookeeperRegistryCenter);
                        break;
                    }
                }
            }

            boolean existed = cleanJobZookeeperRegistryCenter.isExisted("/" + Constants.CLEAN_JOB_NAME);
            if (!existed) {
                createJob(addCleanJob(cleanJobZookeeperRegistryCenter),
                        cleanJobZookeeperRegistryCenter);
            }

        } catch (Exception e) {
            log.error("Create clean job info form zookeeper namespace failed, namespace is {}", namespace, e);
        }
    }

    private JobConfigInfo addCleanJob(ZookeeperRegistryCenter registryCenter) {
        CuratorFramework client = registryCenter.getClient();
        return JobConfigInfo.builder()
                .cron(Constants.CLEAN_JOB_CRON)
                .jobClass(Constants.CLEAN_JOB_CLASS)
                .jobParameter(Constants.CLEAN_JOB_DAY_INTERVAL)
                .shardingTotalCount(1)
                .jobName(Constants.CLEAN_JOB_NAME)
                .domain(client.getNamespace())
                .jobType(Constants.JOB_TYPE_SIMPLE)
                .description(Constants.CLEAN_JOB_REMARK)
                .build();
    }

    /**
     * 任务创建
     */
    private void createJob(JobConfigInfo job,ZookeeperRegistryCenter registryCenter) {
        try {
            if (!springJobSchedulerConfig(strategyShardingConfig(job),registryCenter)) {
                jobSchedulerConfig(job,registryCenter);
            }
            log.info("JobName [{}] class [{}] init success", job.getJobName(), job.getJobClass());
        } catch (Exception e) {
            log.error("JobName[{}] class[{}]  init failed ", job.getJobName(), job.getJobClass(), e);
        }
    }

    /**
     * 作业初始化类配置
     * @param job
     * @return
     */
    private JobConfigInfo strategyShardingConfig(JobConfigInfo job) throws JobException {
        try {
            if (CommUtils.isNotEmpty(job.getJobInitClass())) {
                DynamicStrategySharding customStrategySharding;
                try {
                    customStrategySharding =
                            (DynamicStrategySharding) SpringUtils.getBean(Class.forName(job.getJobInitClass()));
                } catch (NoSuchBeanDefinitionException e) {
                    customStrategySharding = (DynamicStrategySharding) Class.forName(job.getJobInitClass()).newInstance();
                }
                return customStrategySharding.configuration(job);
            }
            return job;
        } catch (Exception e) {
            throw new JobException(JobCodeMessage.STRATEGY_ERROR, e);
        }
    }


    /**
     * spring bean获取
     * @param job
     * @return
     * @throws JobException
     */
    private boolean springJobSchedulerConfig(JobConfigInfo job,ZookeeperRegistryCenter registryCenter) throws JobException {
        try {
            ElasticJob o = (ElasticJob) SpringUtils.getBean(Class.forName(job.getJobClass()));
            if (CommUtils.isNotEmpty(job.getListenerClass())) {
                new SpringJobScheduler(o, registryCenter, JobConfiguration.configuration(job),
                        jobEventRdbConfiguration,
                        (ElasticJobListener) Class.forName(job.getListenerClass()).newInstance()).init();
            } else {
                new SpringJobScheduler(o, registryCenter, JobConfiguration.configuration(job),
                        jobEventRdbConfiguration).init();
            }
        } catch (NoSuchBeanDefinitionException e) {
            return false;
        } catch (Exception e) {
            throw new JobException(JobCodeMessage.SPRING_JOB_SCHEDULER_CONFIG_ERROR, e);
        }
        return true;
    }

    /**
     * 基于类实例化
     * @param job
     * @return
     * @throws JobException
     */
    private boolean jobSchedulerConfig(JobConfigInfo job,ZookeeperRegistryCenter registryCenter) throws JobException {
        try {
            if (CommUtils.isNotEmpty(job.getListenerClass())) {
                new JobScheduler(registryCenter, JobConfiguration.configuration(job),
                        jobEventRdbConfiguration,
                        (ElasticJobListener) Class.forName(job.getListenerClass()).newInstance()).init();
            } else {
                new JobScheduler(registryCenter, JobConfiguration.configuration(job),
                        jobEventRdbConfiguration).init();
            }
        } catch (NoSuchBeanDefinitionException e) {
            return false;
        } catch (Exception e) {
            throw new JobException(JobCodeMessage.JOB_SCHEDULER_CONFIG_ERROR, e);
        }
        return true;
    }
}
