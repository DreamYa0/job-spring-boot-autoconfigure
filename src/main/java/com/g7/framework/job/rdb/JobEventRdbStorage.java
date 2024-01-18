package com.g7.framework.job.rdb;

import com.dangdang.ddframe.job.context.ExecutionType;
import com.dangdang.ddframe.job.event.rdb.DatabaseType;
import com.dangdang.ddframe.job.event.type.JobExecutionEvent;
import com.dangdang.ddframe.job.event.type.JobStatusTraceEvent;
import com.g7.framework.job.modle.JobConfigInfo;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by dreamyao on 2018/11/13.
 */
@Slf4j
public final class JobEventRdbStorage {

    private static final String TABLE_JOB_CONFIG = "T_JOB_CONFIG_INFO";
    private static final String TABLE_JOB_CONFIG_INDEX = "IDX_T_JOB_CONFIG_INFO_1";
    private static final String TABLE_JOB_EXECUTION_LOG = "JOB_EXECUTION_LOG";
    private static final String TABLE_JOB_EXECUTION_LOG_INDEX = "IDX_JOB_EXECUTION_LOG_1";
    private static final String TABLE_JOB_STATUS_TRACE_LOG = "JOB_STATUS_TRACE_LOG";
    private static final String TABLE_JOB_STATUS_TRACE_LOG_INDEX = "IDX_JOB_STATUS_TRACE_LOG_1";
    private static final String TASK_ID_STATE_INDEX = "TASK_ID_STATE_INDEX";
    private final DataSource dataSource;
    private DatabaseType databaseType;

    public JobEventRdbStorage(final DataSource dataSource) {
        this.dataSource = dataSource;
        initTablesAndIndexes();
    }

    /**
     * 初始创建job相关表
     * @throws SQLException
     */
    private void initTablesAndIndexes() {
        try (Connection conn = dataSource.getConnection()) {
            // createJobConfigTableAndIndexIfNeeded(conn);
            createJobExecutionTableAndIndexIfNeeded(conn);
            createJobStatusTraceTableAndIndexIfNeeded(conn);
            databaseType = DatabaseType.valueFrom(conn.getMetaData().getDatabaseProductName());
        } catch (Exception e) {
            log.error("initTablesAndIndexes failed", e);
        }
    }

    /**
     * ZX
     * 作业配置表创建
     * @param conn
     * @throws SQLException
     */
    private void createJobConfigTableAndIndexIfNeeded(final Connection conn) throws SQLException {
        DatabaseMetaData dbMetaData = conn.getMetaData();
        try (ResultSet resultSet = dbMetaData.getTables(null, null,
                TABLE_JOB_CONFIG, new String[]{"TABLE"})) {
            if (!resultSet.next()) {
                createJobConfigTable(conn);
                createJobConfigIndex(conn, TABLE_JOB_CONFIG);
            }
        }
        // createTaskIdIndexIfNeeded(conn, TABLE_JOB_CONFIG, TABLE_JOB_CONFIG_INDEX);
    }

    /**
     * 创建作业执行记录表
     * @param conn
     * @throws SQLException
     */
    private void createJobExecutionTableAndIndexIfNeeded(final Connection conn) throws SQLException {
        DatabaseMetaData dbMetaData = conn.getMetaData();
        try (ResultSet resultSet = dbMetaData.getTables(null, null,
                TABLE_JOB_EXECUTION_LOG, new String[]{"TABLE"})) {
            if (!resultSet.next()) {
                createJobExecutionTable(conn);
                createJobExecuteLogIndex(conn, TABLE_JOB_EXECUTION_LOG);
            }
        }
    }

    /**
     * 创建作业状态表以及相关索引
     * @param conn
     * @throws SQLException
     */
    private void createJobStatusTraceTableAndIndexIfNeeded(final Connection conn) throws SQLException {
        DatabaseMetaData dbMetaData = conn.getMetaData();
        try (ResultSet resultSet = dbMetaData.getTables(null, null,
                TABLE_JOB_STATUS_TRACE_LOG, new String[]{"TABLE"})) {
            if (!resultSet.next()) {
                createJobStatusTraceTable(conn);
                createTaskIdAndStateIndex(conn, TABLE_JOB_STATUS_TRACE_LOG);
                createJobStatusTimeIndex(conn, TABLE_JOB_STATUS_TRACE_LOG);
            }
        }
    }

    private void createJobConfigTable(final Connection conn) throws SQLException {
        String dbSchema = "CREATE TABLE `" + TABLE_JOB_CONFIG + "` (" +
                "`ID` int(11) NOT NULL AUTO_INCREMENT COMMENT '作业ID'," +
                "`JOB_NAME` varchar(100)   NOT NULL COMMENT '作业名称'," +
                "`JOB_CLASS` varchar(500)   NOT NULL COMMENT '作业类'," +
                "`JOB_TYPE` varchar(20)   NOT NULL COMMENT '作业类型'," +
                "`SHARDING_TOTAL_COUNT` int(11) DEFAULT '1' COMMENT '分片数 '," +
                "`SHARDING_ITEM_PARAMETERS` varchar(2000)   DEFAULT NULL COMMENT '分片参数'," +
                "`JOB_PARAMETER` varchar(500)   DEFAULT NULL COMMENT '作业参数'," +
                "`CRON` varchar(50)   NOT NULL COMMENT 'CRON表达式'," +
                "`JOB_INIT_CLASS` varchar(500)   DEFAULT NULL COMMENT '自定义作业初始化类'," +
                "`FAIL_OVER` char(1)   DEFAULT '1' COMMENT '是否支持故障转移'," +
                "`MIS_FIRE` char(1)   DEFAULT '1' COMMENT '是否开启错过任务重新执行'," +
                "`IS_STREAM` char(1)   DEFAULT '0' COMMENT '是否流式处理'," +
                "`COMMAND_LINE` varchar(500)   DEFAULT NULL COMMENT '脚步作业执行命令行'," +
                "`IS_MONITOR` char(1)   DEFAULT '0' COMMENT '是否监控作业执行状态'," +
                "`MONITOR_PORT` int(11) DEFAULT '-1' COMMENT '监控端口'," +
                "`MAX_TIME_DIFF_SECONDS` int(11) DEFAULT '-1' COMMENT '最大容忍本机与注册中心的时间误差秒数'," +
                "`RECONCILE_INTERVAL_MINUTES` int(11) DEFAULT '10' COMMENT '作业服务器状态修复周期'," +
                "`JOB_SHARDING_STRATEGY_CLASS` varchar(100)   DEFAULT NULL COMMENT '作业分片策略实现类全路径'," +
                "`EVENT_TRACE_RDB_DATASOURCE` varchar(100)   DEFAULT NULL COMMENT '事件追踪数据源ID'," +
                "`OVER_WRITER` char(1)   DEFAULT '1' COMMENT '是否覆盖zk信息'," +
                "`IS_DISABLED` char(1)   DEFAULT '0' COMMENT '是否启动时禁止'," +
                "`LISTENER_CLASS` varchar(500)   DEFAULT NULL COMMENT '监听类'," +
                "`DESCRIPTION` varchar(500)   DEFAULT NULL COMMENT '描述'," +
                "`DOMAIN` varchar(50)   DEFAULT NULL COMMENT '域'," +
                "`EXECUTOR_SERVICE_HANDLER` varchar(500)   DEFAULT NULL COMMENT '自定义异常处理类'," +
                "`GMT_CREATE` datetime(6) DEFAULT NULL COMMENT '创建时间'," +
                "`GMT_MODIFIED` datetime(6) DEFAULT NULL COMMENT '修改时间'," +
                "`IS_STOP` char(1) COLLATE utf8_bin DEFAULT '0' COMMENT '是否终止'," +
                "`JOB_EXCEPTION_HANDLER` varchar(500) COLLATE utf8_bin DEFAULT NULL COMMENT '自定义异常处理类'," +
                "`NAMESPACE` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '命名空间'," +
                "PRIMARY KEY (`ID`)) CHARSET=utf8 COLLATE=utf8_bin;";
        try (PreparedStatement preparedStatement = conn.prepareStatement(dbSchema)) {
            preparedStatement.execute();
        }
    }

    private void createJobConfigIndex(final Connection conn, final String tableName) throws SQLException {
        String sql = "CREATE UNIQUE  INDEX " + TABLE_JOB_CONFIG_INDEX + " ON " + tableName +
                " (`JOB_NAME`,`NAMESPACE`);";
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.execute();
        }
    }


    private void createJobExecutionTable(final Connection conn) throws SQLException {
        String dbSchema = "CREATE TABLE `" + TABLE_JOB_EXECUTION_LOG + "` ("
                + "`id` VARCHAR(40) NOT NULL, "
                + "`job_name` VARCHAR(100) NOT NULL, "
                + "`task_id` VARCHAR(255) NOT NULL, "
                + "`hostname` VARCHAR(255) NOT NULL, "
                + "`ip` VARCHAR(50) NOT NULL, "
                + "`sharding_item` INT NOT NULL, "
                + "`execution_source` VARCHAR(20) NOT NULL, "
                + "`failure_cause` VARCHAR(4000) NULL, "
                + "`is_success` INT NOT NULL, "
                + "`start_time` TIMESTAMP NULL, "
                + "`complete_time` TIMESTAMP NULL, "
                + "PRIMARY KEY (`id`)) CHARSET=utf8 COLLATE=utf8_bin;";
        try (PreparedStatement preparedStatement = conn.prepareStatement(dbSchema)) {
            preparedStatement.execute();
        }
    }

    private void createJobStatusTraceTable(final Connection conn) throws SQLException {
        String dbSchema = "CREATE TABLE `" + TABLE_JOB_STATUS_TRACE_LOG + "` ("
                + "`id` VARCHAR(40) NOT NULL, "
                + "`job_name` VARCHAR(100) NOT NULL, "
                + "`original_task_id` VARCHAR(255) NOT NULL, "
                + "`task_id` VARCHAR(255) NOT NULL, "
                + "`slave_id` VARCHAR(50) NOT NULL, "
                + "`source` VARCHAR(50) NOT NULL, "
                + "`execution_type` VARCHAR(20) NOT NULL, "
                + "`sharding_item` VARCHAR(2000) NOT NULL, "
                + "`state` VARCHAR(20) NOT NULL, "
                + "`message` VARCHAR(4000) NULL, "
                + "`creation_time` TIMESTAMP NULL, "
                + "PRIMARY KEY (`id`)) CHARSET=utf8 COLLATE=utf8_bin;";
        try (PreparedStatement preparedStatement = conn.prepareStatement(dbSchema)) {
            preparedStatement.execute();
        }
    }

    private void createTaskIdAndStateIndex(final Connection conn, final String tableName) throws SQLException {
        String sql = "CREATE INDEX " + TASK_ID_STATE_INDEX + " ON " + tableName + " (`task_id`, `state`);";
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.execute();
        }
    }

    /**
     * 添加执行记录或更新执行记录
     * @param jobExecutionEvent
     * @return
     */
    public boolean addJobExecutionEvent(final JobExecutionEvent jobExecutionEvent) {
        if (null == jobExecutionEvent.getCompleteTime()) {
            return insertJobExecutionEvent(jobExecutionEvent);
        } else {
            if (jobExecutionEvent.isSuccess()) {
                return updateJobExecutionEventWhenSuccess(jobExecutionEvent);
            } else {
                return updateJobExecutionEventFailure(jobExecutionEvent);
            }
        }
    }

    /**
     * 删除历史轨迹
     * @param date
     * @return
     */
    private void deleteExecutionData(Date date) throws SQLException {
        String sql = "delete from " + TABLE_JOB_EXECUTION_LOG + " where start_time < ?";
        executeSql(date, sql);
    }

    private void executeSql(Date date, String sql) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setDate(1, date);
            preparedStatement.executeUpdate();
        } catch (final SQLException ex) {
            throw new SQLException(ex);
        }
    }

    /**
     * 删除历史轨迹
     * @param date
     * @return
     */
    private void deleteHistoryData(String sql, Date date) throws SQLException {
        executeSql(date, sql);
    }

    /**
     * 删除历史状态数据
     * @param date
     * @return
     */
    public boolean deleteHistoryData(java.util.Date date) {
        boolean result;
        try {
            String sql = "delete from " + TABLE_JOB_EXECUTION_LOG + " where start_time < ?";
            deleteHistoryData(sql, new Date(date.getTime()));
            sql = "delete from " + TABLE_JOB_STATUS_TRACE_LOG + " where creation_time < ?";
            deleteHistoryData(sql, new Date(date.getTime()));
            result = true;
        } catch (final SQLException ex) {
            result = false;
            log.error("deleteHistoryData fail", ex);
        }
        return result;
    }

    public void setSqlValue(PreparedStatement preparedStatement, Object val, int index) throws SQLException {
        setSqlValue(preparedStatement, val, index, String.class);
    }

    public void setSqlValue(PreparedStatement preparedStatement, Object val, int index, Class type)
            throws SQLException {
        if (type == Integer.class) {
            if (val == null) {
                val = 0;
            }
            preparedStatement.setInt(index, (int) val);
        }

        if (type == String.class) {
            if (val == null) {
                val = "";
            }
            preparedStatement.setString(index, (String) val);
        }
    }


    private boolean insertJobExecutionEvent(final JobExecutionEvent jobExecutionEvent) {
        boolean result = false;
        String sql = "INSERT INTO `" + TABLE_JOB_EXECUTION_LOG + "` (`id`, `job_name`, `task_id`, `hostname`, " +
                "`ip`, `sharding_item`, `execution_source`, `is_success`, `start_time`) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement(jobExecutionEvent, preparedStatement);
            preparedStatement.execute();
            result = true;
        } catch (final SQLException ex) {
            if (!isDuplicateRecord(ex)) {
                // TODO 记录失败直接输出日志,未来可考虑配置化
                log.error("insertJobExecutionEvent failed", ex);
            }
        }
        return result;
    }

    private void preparedStatement(JobExecutionEvent jobExecutionEvent, PreparedStatement preparedStatement)
            throws SQLException {
        preparedStatement.setString(1, jobExecutionEvent.getId());
        preparedStatement.setString(2, jobExecutionEvent.getJobName());
        preparedStatement.setString(3, jobExecutionEvent.getTaskId());
        preparedStatement.setString(4, jobExecutionEvent.getHostname());
        preparedStatement.setString(5, jobExecutionEvent.getIp());
        preparedStatement.setInt(6, jobExecutionEvent.getShardingItem());
        preparedStatement.setString(7, jobExecutionEvent.getSource().toString());
        preparedStatement.setBoolean(8, jobExecutionEvent.isSuccess());
        preparedStatement.setTimestamp(9, new Timestamp(jobExecutionEvent.getStartTime().getTime()));
    }

    private boolean isDuplicateRecord(final SQLException ex) {
        return DatabaseType.MySQL.equals(databaseType) && 1062 == ex.getErrorCode()
                || DatabaseType.H2.equals(databaseType) && 23505 == ex.getErrorCode()
                || DatabaseType.SQLServer.equals(databaseType) && 1 == ex.getErrorCode()
                || DatabaseType.DB2.equals(databaseType) && -803 == ex.getErrorCode()
                || DatabaseType.PostgreSQL.equals(databaseType) && 0 == ex.getErrorCode()
                || DatabaseType.Oracle.equals(databaseType) && 1 == ex.getErrorCode();
    }

    private boolean updateJobExecutionEventWhenSuccess(final JobExecutionEvent jobExecutionEvent) {
        boolean result = false;
        String sql = "UPDATE `" + TABLE_JOB_EXECUTION_LOG + "` SET `is_success` = ?, `complete_time` = ? WHERE id = ?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setBoolean(1, jobExecutionEvent.isSuccess());
            preparedStatement.setTimestamp(2, new Timestamp(jobExecutionEvent.getCompleteTime().getTime()));
            preparedStatement.setString(3, jobExecutionEvent.getId());
            if (0 == preparedStatement.executeUpdate()) {
                return insertJobExecutionEventWhenSuccess(jobExecutionEvent);
            }
            result = true;
        } catch (final SQLException ex) {
            // TODO 记录失败直接输出日志,未来可考虑配置化
            log.error("updateJobExecutionEventWhenSuccess failed", ex);
        }
        return result;
    }

    private boolean insertJobExecutionEventWhenSuccess(final JobExecutionEvent jobExecutionEvent) {
        boolean result = false;
        String sql = "INSERT INTO `" + TABLE_JOB_EXECUTION_LOG + "` (`id`, `job_name`, `task_id`, `hostname`, " +
                "`ip`, `sharding_item`, `execution_source`, `is_success`, `start_time`, `complete_time`) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement(jobExecutionEvent, preparedStatement);
            preparedStatement.setTimestamp(10, new Timestamp(jobExecutionEvent.getCompleteTime().getTime()));
            preparedStatement.execute();
            result = true;
        } catch (final SQLException ex) {
            if (isDuplicateRecord(ex)) {
                return updateJobExecutionEventWhenSuccess(jobExecutionEvent);
            }
            // TODO 记录失败直接输出日志,未来可考虑配置化
            log.error("insertJobExecutionEventWhenSuccess failed", ex);
        }
        return result;
    }

    private boolean updateJobExecutionEventFailure(final JobExecutionEvent jobExecutionEvent) {
        boolean result = false;
        String sql = "UPDATE `" + TABLE_JOB_EXECUTION_LOG + "` SET `is_success` = ?, `complete_time` = ?, " +
                "`failure_cause` = ? WHERE id = ?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setBoolean(1, jobExecutionEvent.isSuccess());
            preparedStatement.setTimestamp(2, new Timestamp(jobExecutionEvent.getCompleteTime().getTime()));
            preparedStatement.setString(3, truncateString(jobExecutionEvent.getFailureCause()));
            preparedStatement.setString(4, jobExecutionEvent.getId());
            if (0 == preparedStatement.executeUpdate()) {
                return insertJobExecutionEventWhenFailure(jobExecutionEvent);
            }
            result = true;
        } catch (final SQLException ex) {
            // TODO 记录失败直接输出日志,未来可考虑配置化
            log.error("updateJobExecutionEventFailure failed", ex);
        }
        return result;
    }

    private boolean insertJobExecutionEventWhenFailure(final JobExecutionEvent jobExecutionEvent) {
        boolean result = false;
        String sql = "INSERT INTO `" + TABLE_JOB_EXECUTION_LOG + "` (`id`, `job_name`, `task_id`, `hostname`, " +
                "`ip`, `sharding_item`, `execution_source`, `failure_cause`, `is_success`, `start_time`) " + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setString(1, jobExecutionEvent.getId());
            preparedStatement.setString(2, jobExecutionEvent.getJobName());
            preparedStatement.setString(3, jobExecutionEvent.getTaskId());
            preparedStatement.setString(4, jobExecutionEvent.getHostname());
            preparedStatement.setString(5, jobExecutionEvent.getIp());
            preparedStatement.setInt(6, jobExecutionEvent.getShardingItem());
            preparedStatement.setString(7, jobExecutionEvent.getSource().toString());
            preparedStatement.setString(8, truncateString(jobExecutionEvent.getFailureCause()));
            preparedStatement.setBoolean(9, jobExecutionEvent.isSuccess());
            preparedStatement.setTimestamp(10, new Timestamp(jobExecutionEvent.getStartTime().getTime()));
            preparedStatement.execute();
            result = true;
        } catch (final SQLException ex) {
            if (isDuplicateRecord(ex)) {
                return updateJobExecutionEventFailure(jobExecutionEvent);
            }
            // TODO 记录失败直接输出日志,未来可考虑配置化
            log.error("insertJobExecutionEventWhenFailure failed", ex);
        }
        return result;
    }

    boolean addJobStatusTraceEvent(final JobStatusTraceEvent jobStatusTraceEvent) {
        String originalTaskId = jobStatusTraceEvent.getOriginalTaskId();
        if (JobStatusTraceEvent.State.TASK_STAGING != jobStatusTraceEvent.getState()) {
            originalTaskId = getOriginalTaskId(jobStatusTraceEvent.getTaskId());
        }
        boolean result = false;
        String sql = "INSERT INTO `" + TABLE_JOB_STATUS_TRACE_LOG + "` (`id`, `job_name`, `original_task_id`, " +
                "`task_id`, `slave_id`, `source`, `execution_type`, `sharding_item`,  " +
                "`state`, `message`, `creation_time`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setString(1, UUID.randomUUID().toString());
            preparedStatement.setString(2, jobStatusTraceEvent.getJobName());
            preparedStatement.setString(3, originalTaskId);
            preparedStatement.setString(4, jobStatusTraceEvent.getTaskId());
            preparedStatement.setString(5, jobStatusTraceEvent.getSlaveId());
            preparedStatement.setString(6, jobStatusTraceEvent.getSource().toString());
            preparedStatement.setString(7, jobStatusTraceEvent.getExecutionType().name());
            preparedStatement.setString(8, jobStatusTraceEvent.getShardingItems());
            preparedStatement.setString(9, jobStatusTraceEvent.getState().toString());
            preparedStatement.setString(10, truncateString(jobStatusTraceEvent.getMessage()));
            preparedStatement.setTimestamp(11, new Timestamp(jobStatusTraceEvent.getCreationTime().getTime()));
            preparedStatement.execute();
            result = true;
        } catch (final SQLException ex) {
            // TODO 记录失败直接输出日志,未来可考虑配置化
            log.error("addJobStatusTraceEvent failed", ex);
        }
        return result;
    }

    private String getOriginalTaskId(final String taskId) {
        String sql = String.format("SELECT original_task_id FROM %s WHERE task_id = '%s' and state='%s' limit 1",
                TABLE_JOB_STATUS_TRACE_LOG, taskId, JobStatusTraceEvent.State.TASK_STAGING);
        String result = "";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = conn.prepareStatement(sql);
                ResultSet resultSet = preparedStatement.executeQuery()
        ) {
            if (resultSet.next()) {
                return resultSet.getString("original_task_id");
            }
        } catch (final SQLException ex) {
            // TODO 记录失败直接输出日志,未来可考虑配置化
            log.error("getOriginalTaskId failed", ex);
        }
        return result;
    }

    /**
     * 添加Job配置
     * @param jobConfigInfo
     * @return
     */
    public void insertJobConfig(final JobConfigInfo jobConfigInfo) throws SQLException {
        String sql = "INSERT INTO " + TABLE_JOB_CONFIG + " (JOB_NAME,JOB_CLASS,JOB_TYPE,SHARDING_TOTAL_COUNT," +
                "SHARDING_ITEM_PARAMETERS," +
                "JOB_PARAMETER,CRON,JOB_INIT_CLASS,FAIL_OVER,MIS_FIRE,IS_STREAM,COMMAND_LINE,IS_MONITOR,MONITOR_PORT," +
                "MAX_TIME_DIFF_SECONDS," +
                "RECONCILE_INTERVAL_MINUTES ,JOB_SHARDING_STRATEGY_CLASS,OVER_WRITER ,IS_DISABLED,LISTENER_CLASS," +
                "DESCRIPTION,NAMESPACE,EXECUTOR_SERVICE_HANDLER,GMT_CREATE,GMT_MODIFIED,JOB_EXCEPTION_HANDLER) "
                + " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            setSqlValue(preparedStatement, jobConfigInfo.getJobName(), 1);
            setSqlValue(preparedStatement, jobConfigInfo.getJobClass(), 2);
            setSqlValue(preparedStatement, jobConfigInfo.getJobType(), 3);
            setSqlValue(preparedStatement, jobConfigInfo.getShardingTotalCount(), 4, Integer.class);
            setSqlValue(preparedStatement, jobConfigInfo.getShardingItemParameters(), 5);
            setSqlValue(preparedStatement, jobConfigInfo.getJobParameter(), 6);
            setSqlValue(preparedStatement, jobConfigInfo.getCron(), 7);
            setSqlValue(preparedStatement, jobConfigInfo.getJobInitClass(), 8);
            setSqlValue(preparedStatement, jobConfigInfo.isFailover() ? "1" : "0", 9);
            setSqlValue(preparedStatement, jobConfigInfo.isMisfire() ? "1" : "0", 10);
            setSqlValue(preparedStatement, jobConfigInfo.isStreamingProcess() ? "1" : "0", 11);
            setSqlValue(preparedStatement, jobConfigInfo.getScriptCommandLine(), 12);
            setSqlValue(preparedStatement, jobConfigInfo.isMonitorExecution() ? "1" : "0", 13);
            setSqlValue(preparedStatement, jobConfigInfo.getMonitorPort(), 14, Integer.class);
            setSqlValue(preparedStatement, jobConfigInfo.getMaxTimeDiffSeconds(), 15, Integer.class);
            setSqlValue(preparedStatement, jobConfigInfo.getReconcileIntervalMinutes(), 16, Integer.class);
            setSqlValue(preparedStatement, jobConfigInfo.getJobShardingStrategyClass(), 17);
            setSqlValue(preparedStatement, "1", 18);
            setSqlValue(preparedStatement, jobConfigInfo.isDisabled() ? "1" : "0", 19);
            setSqlValue(preparedStatement, jobConfigInfo.getListenerClass(), 20);
            setSqlValue(preparedStatement, jobConfigInfo.getDescription(), 21);
            setSqlValue(preparedStatement, jobConfigInfo.getDomain(), 22);
            setSqlValue(preparedStatement, jobConfigInfo.getExecutorServiceHandler(), 23);
            Date date = new Date(System.currentTimeMillis());
            preparedStatement.setDate(24, date);
            preparedStatement.setDate(25, date);
            setSqlValue(preparedStatement, jobConfigInfo.getJobExceptionHandler(), 26);
            preparedStatement.execute();
        } catch (SQLException e) {
            log.error("insert job config failed", e);
            throw new SQLException(e);
        }
    }

    public void updateJobConfig(final JobConfigInfo jobConfigInfo) throws SQLException {

        String updateSql = "UPDATE " + TABLE_JOB_CONFIG + " SET JOB_CLASS=?,JOB_TYPE=?,SHARDING_TOTAL_COUNT=?," +
                "SHARDING_ITEM_PARAMETERS=?," +
                "JOB_PARAMETER=?,CRON=?,JOB_INIT_CLASS=?,FAIL_OVER=?,MIS_FIRE=?,IS_STREAM=?,COMMAND_LINE=?," +
                "IS_MONITOR=?,MONITOR_PORT=?,MAX_TIME_DIFF_SECONDS=?,RECONCILE_INTERVAL_MINUTES=? ," +
                "JOB_SHARDING_STRATEGY_CLASS=?,OVER_WRITER=? ,IS_DISABLED=?,LISTENER_CLASS=?," +
                "DESCRIPTION=?,EXECUTOR_SERVICE_HANDLER=?,GMT_CREATE=?,GMT_MODIFIED=?," +
                "JOB_EXCEPTION_HANDLER=? WHERE JOB_NAME=? AND NAMESPACE=?;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement preparedStatement = conn.prepareStatement(updateSql)) {

            setSqlValue(preparedStatement, jobConfigInfo.getJobClass(), 1);
            setSqlValue(preparedStatement, jobConfigInfo.getJobType(), 2);
            setSqlValue(preparedStatement, jobConfigInfo.getShardingTotalCount(), 3, Integer.class);
            setSqlValue(preparedStatement, jobConfigInfo.getShardingItemParameters(), 4);
            setSqlValue(preparedStatement, jobConfigInfo.getJobParameter(), 5);
            setSqlValue(preparedStatement, jobConfigInfo.getCron(), 6);
            setSqlValue(preparedStatement, jobConfigInfo.getJobInitClass(), 7);
            setSqlValue(preparedStatement, jobConfigInfo.isFailover() ? "1" : "0", 8);
            setSqlValue(preparedStatement, jobConfigInfo.isMisfire() ? "1" : "0", 9);
            setSqlValue(preparedStatement, jobConfigInfo.isStreamingProcess() ? "1" : "0", 10);
            setSqlValue(preparedStatement, jobConfigInfo.getScriptCommandLine(), 11);
            setSqlValue(preparedStatement, jobConfigInfo.isMonitorExecution() ? "1" : "0", 12);
            setSqlValue(preparedStatement, jobConfigInfo.getMonitorPort(), 13, Integer.class);
            setSqlValue(preparedStatement, jobConfigInfo.getMaxTimeDiffSeconds(), 14, Integer.class);
            setSqlValue(preparedStatement, jobConfigInfo.getReconcileIntervalMinutes(), 15, Integer.class);
            setSqlValue(preparedStatement, jobConfigInfo.getJobShardingStrategyClass(), 16);
            setSqlValue(preparedStatement, "1", 17);
            setSqlValue(preparedStatement, jobConfigInfo.isDisabled() ? "1" : "0", 18);
            setSqlValue(preparedStatement, jobConfigInfo.getListenerClass(), 19);
            setSqlValue(preparedStatement, jobConfigInfo.getDescription(), 20);

            setSqlValue(preparedStatement, jobConfigInfo.getExecutorServiceHandler(), 21);
            Date date = new Date(System.currentTimeMillis());
            preparedStatement.setDate(22, date);
            preparedStatement.setDate(23, date);
            setSqlValue(preparedStatement, jobConfigInfo.getJobExceptionHandler(), 24);

            setSqlValue(preparedStatement, jobConfigInfo.getJobName(), 25);
            setSqlValue(preparedStatement, jobConfigInfo.getDomain(), 26);

            preparedStatement.execute();
        } catch (SQLException e) {
            log.error("update job config failed", e);
            throw new SQLException(e);
        }
    }

    public void deleteJobConfig(final JobConfigInfo jobConfigInfo) throws SQLException {

        String deleteSql = "DELETE FROM" + TABLE_JOB_CONFIG + " t_job_config_info WHERE JOB_NAME=? AND NAMESPACE=?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement preparedStatement = conn.prepareStatement(deleteSql)) {
            setSqlValue(preparedStatement, jobConfigInfo.getJobName(), 1);
            setSqlValue(preparedStatement, jobConfigInfo.getDomain(), 2);
            preparedStatement.execute();
        } catch (SQLException e) {
            log.error("delete job config failed", e);
            throw new SQLException(e);
        }
    }

    private String truncateString(final String str) {
        return !Strings.isNullOrEmpty(str) && str.length() > 4000 ? str.substring(0, 4000) : str;
    }

    List<JobStatusTraceEvent> getJobStatusTraceEvents(final String taskId) {
        String sql = String.format("SELECT * FROM %s WHERE task_id = '%s'", TABLE_JOB_STATUS_TRACE_LOG, taskId);
        List<JobStatusTraceEvent> result = new ArrayList<>();
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = conn.prepareStatement(sql);
                ResultSet resultSet = preparedStatement.executeQuery()
        ) {
            while (resultSet.next()) {
                JobStatusTraceEvent jobStatusTraceEvent = new JobStatusTraceEvent(resultSet.getString(1),
                        resultSet.getString(2), resultSet.getString(3),
                        resultSet.getString(4), resultSet.getString(5),
                        JobStatusTraceEvent.Source.valueOf(resultSet.getString(6)),
                        ExecutionType.valueOf(resultSet.getString(7)), resultSet.getString(8),
                        JobStatusTraceEvent.State.valueOf(resultSet.getString(9)),
                        resultSet.getString(10),
                        new SimpleDateFormat("yyyy-mm-dd HH:MM:SS").parse(resultSet.getString(11)));
                result.add(jobStatusTraceEvent);
            }
        } catch (final SQLException | ParseException ex) {
            // TODO 记录失败直接输出日志,未来可考虑配置化
            log.error("getJobStatusTraceEvents failed", ex);
        }
        return result;
    }

    private void createJobExecuteLogIndex(final Connection conn, final String tableName) throws SQLException {
        String sql = "CREATE  INDEX " + TABLE_JOB_EXECUTION_LOG_INDEX + " ON " + tableName + " (`start_time`);";
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.execute();
        }
    }

    private void createJobStatusTimeIndex(final Connection conn, final String tableName) throws SQLException {
        String sql = "CREATE  INDEX " + TABLE_JOB_STATUS_TRACE_LOG_INDEX + " ON " + tableName + " (`creation_time`);";
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.execute();
        }
    }
}
