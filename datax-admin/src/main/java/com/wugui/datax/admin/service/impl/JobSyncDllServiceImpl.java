package com.wugui.datax.admin.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.wugui.datatx.core.biz.model.ReturnT;
import com.wugui.datatx.core.enums.ExecutorBlockStrategyEnum;
import com.wugui.datatx.core.glue.GlueTypeEnum;
import com.wugui.datax.admin.core.cron.CronExpression;
import com.wugui.datax.admin.core.route.ExecutorRouteStrategyEnum;
import com.wugui.datax.admin.core.util.I18nUtil;
import com.wugui.datax.admin.entity.JobGroup;
import com.wugui.datax.admin.entity.JobInfo;
import com.wugui.datax.admin.entity.JobSyncDll;
import com.wugui.datax.admin.mapper.JobGroupMapper;
import com.wugui.datax.admin.mapper.JobInfoMapper;
import com.wugui.datax.admin.service.DatasourceQueryService;
import com.wugui.datax.admin.service.JobSyncDllService;
import com.wugui.datax.admin.util.AESUtil;
import com.wugui.datax.admin.util.DateFormatUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * 已废弃，之前计划一次job提交处理所有表，后来发现dataX底层只支持job的单次提交，也就是需要遍历创建job
 *
 * @author : lijie
 * @since 版本号
 * Date: 5/31/23
 */
@Deprecated
@Service
public class JobSyncDllServiceImpl implements JobSyncDllService {
    @Resource
    private JobGroupMapper jobGroupMapper;
    @Resource
    private JobInfoMapper jobInfoMapper;
    @Autowired
    private DatasourceQueryService datasourceQueryService;


    @Override
    public ReturnT<String> addAllTable(JobSyncDll jobSyncDll) throws IOException {
        JobInfo jobInfo = jobSyncDll.getJobInfo();
        // valid
        JobGroup group = jobGroupMapper.load(jobInfo.getJobGroup());
        ReturnT<String> FAIL_CODE = checkParamReturnT(jobInfo, group);
        if (FAIL_CODE != null) {
            return FAIL_CODE;
        }


        if (StringUtils.isBlank(jobInfo.getReplaceParamType()) || !DateFormatUtils.formatList().contains(jobInfo.getReplaceParamType())) {
            jobInfo.setReplaceParamType(DateFormatUtils.TIMESTAMP);
        }

        // fix "\r" in shell
        if (GlueTypeEnum.GLUE_SHELL == GlueTypeEnum.match(jobInfo.getGlueType()) && jobInfo.getGlueSource() != null) {
            jobInfo.setGlueSource(jobInfo.getGlueSource().replaceAll("\r", ""));
        }

        // ChildJobId valid
        if (jobInfo.getChildJobId() != null && jobInfo.getChildJobId().trim().length() > 0) {
            String[] childJobIds = jobInfo.getChildJobId().split(",");
            for (String childJobIdItem : childJobIds) {
                if (StringUtils.isNotBlank(childJobIdItem) && isNumeric(childJobIdItem) && Integer.parseInt(childJobIdItem) > 0) {
                    JobInfo childJobInfo = jobInfoMapper.loadById(Integer.parseInt(childJobIdItem));
                    if (childJobInfo == null) {
                        return new ReturnT<String>(ReturnT.FAIL_CODE,
                                MessageFormat.format((I18nUtil.getString("jobinfo_field_childJobId") + "({0})" + I18nUtil.getString("system_not_found")), childJobIdItem));
                    }
                } else {
                    return new ReturnT<String>(ReturnT.FAIL_CODE,
                            MessageFormat.format((I18nUtil.getString("jobinfo_field_childJobId") + "({0})" + I18nUtil.getString("system_invalid")), childJobIdItem));
                }
            }

            // join , avoid "xxx,,"
            String temp = "";
            for (String item : childJobIds) {
                temp += item + ",";
            }
            temp = temp.substring(0, temp.length() - 1);

            jobInfo.setChildJobId(temp);
        }

        //表同步
        //fromDb查询所有table，设置到jobJson的 job->content->reader->parameter->connection->table数组
        //writer->parameter->connection->table数组
        List<String> fromTables = datasourceQueryService.getTables(jobSyncDll.getFromDbId(), jobSyncDll.getFromDbSchemaName());

        JSONObject jobJsonObject = JSONObject.parseObject(jobInfo.getJobJson());
        //赋值到reader的table属性
        JSONArray readerJsonArray = jobJsonObject.getJSONObject("job").getJSONObject("content").getJSONObject("reader").getJSONObject("parameter").getJSONArray("connection");
        readerJsonArray.getJSONObject(0).put("table", new JSONArray(Collections.singletonList(fromTables)));

        //toDb查询所有table
        List<String> toTables = datasourceQueryService.getTables(jobSyncDll.getToDbId(), jobSyncDll.getToDbSchemaName());
        //赋值到writer的table属性
        JSONArray writerJsonArray = jobJsonObject.getJSONObject("job").getJSONObject("content").getJSONObject("writer").getJSONObject("parameter").getJSONArray("connection");
        writerJsonArray.getJSONObject(0).put("table", new JSONArray(Collections.singletonList(toTables)));

        String convertJson = jobJsonObject.toJSONString();

        // add in db
        jobInfo.setAddTime(new Date());
        jobInfo.setJobJson(convertJson);
        jobInfo.setUpdateTime(new Date());
        jobInfo.setGlueUpdatetime(new Date());
        jobInfoMapper.save(jobInfo);
        if (jobInfo.getId() < 1) {
            return new ReturnT<>(ReturnT.FAIL_CODE, (I18nUtil.getString("jobinfo_field_add") + I18nUtil.getString("system_fail")));
        }

        return new ReturnT<>(String.valueOf(jobInfo.getId()));
    }

    private static ReturnT<String> checkParamReturnT(JobInfo jobInfo, JobGroup group) {
        if (group == null) {
            return new ReturnT<>(ReturnT.FAIL_CODE, (I18nUtil.getString("system_please_choose") + I18nUtil.getString("jobinfo_field_jobgroup")));
        }
        if (!CronExpression.isValidExpression(jobInfo.getJobCron())) {
            return new ReturnT<>(ReturnT.FAIL_CODE, I18nUtil.getString("jobinfo_field_cron_invalid"));
        }
        if (jobInfo.getGlueType().equals(GlueTypeEnum.BEAN.getDesc()) && jobInfo.getJobJson().trim().length() <= 2) {
            return new ReturnT<>(ReturnT.FAIL_CODE, (I18nUtil.getString("system_please_input") + I18nUtil.getString("jobinfo_field_jobjson")));
        }
        if (jobInfo.getJobDesc() == null || jobInfo.getJobDesc().trim().length() == 0) {
            return new ReturnT<>(ReturnT.FAIL_CODE, (I18nUtil.getString("system_please_input") + I18nUtil.getString("jobinfo_field_jobdesc")));
        }
        if (jobInfo.getUserId() == 0 ) {
            return new ReturnT<>(ReturnT.FAIL_CODE, (I18nUtil.getString("system_please_input") + I18nUtil.getString("jobinfo_field_author")));
        }
        if (ExecutorRouteStrategyEnum.match(jobInfo.getExecutorRouteStrategy(), null) == null) {
            return new ReturnT<>(ReturnT.FAIL_CODE, (I18nUtil.getString("jobinfo_field_executorRouteStrategy") + I18nUtil.getString("system_invalid")));
        }
        if (ExecutorBlockStrategyEnum.match(jobInfo.getExecutorBlockStrategy(), null) == null) {
            return new ReturnT<>(ReturnT.FAIL_CODE, (I18nUtil.getString("jobinfo_field_executorBlockStrategy") + I18nUtil.getString("system_invalid")));
        }
        if (GlueTypeEnum.match(jobInfo.getGlueType()) == null) {
            return new ReturnT<>(ReturnT.FAIL_CODE, (I18nUtil.getString("jobinfo_field_gluetype") + I18nUtil.getString("system_invalid")));
        }
        if (GlueTypeEnum.BEAN == GlueTypeEnum.match(jobInfo.getGlueType()) && (jobInfo.getExecutorHandler() == null || jobInfo.getExecutorHandler().trim().length() == 0)) {
            return new ReturnT<>(ReturnT.FAIL_CODE, (I18nUtil.getString("system_please_input") + "JobHandler"));
        }
        return null;
    }

    @Override
    public ReturnT<String> addAllDll(JobInfo jobInfo) {
        // valid
        JobGroup group = jobGroupMapper.load(jobInfo.getJobGroup());
        ReturnT<String> FAIL_CODE = checkParamReturnT(jobInfo, group);
        if (FAIL_CODE != null) return FAIL_CODE;


        if (StringUtils.isBlank(jobInfo.getReplaceParamType()) || !DateFormatUtils.formatList().contains(jobInfo.getReplaceParamType())) {
            jobInfo.setReplaceParamType(DateFormatUtils.TIMESTAMP);
        }

        // fix "\r" in shell
        if (GlueTypeEnum.GLUE_SHELL == GlueTypeEnum.match(jobInfo.getGlueType()) && jobInfo.getGlueSource() != null) {
            jobInfo.setGlueSource(jobInfo.getGlueSource().replaceAll("\r", ""));
        }

        // ChildJobId valid
        if (jobInfo.getChildJobId() != null && jobInfo.getChildJobId().trim().length() > 0) {
            String[] childJobIds = jobInfo.getChildJobId().split(",");
            for (String childJobIdItem : childJobIds) {
                if (StringUtils.isNotBlank(childJobIdItem) && isNumeric(childJobIdItem) && Integer.parseInt(childJobIdItem) > 0) {
                    JobInfo childJobInfo = jobInfoMapper.loadById(Integer.parseInt(childJobIdItem));
                    if (childJobInfo == null) {
                        return new ReturnT<String>(ReturnT.FAIL_CODE,
                                MessageFormat.format((I18nUtil.getString("jobinfo_field_childJobId") + "({0})" + I18nUtil.getString("system_not_found")), childJobIdItem));
                    }
                } else {
                    return new ReturnT<String>(ReturnT.FAIL_CODE,
                            MessageFormat.format((I18nUtil.getString("jobinfo_field_childJobId") + "({0})" + I18nUtil.getString("system_invalid")), childJobIdItem));
                }
            }

            // join , avoid "xxx,,"
            String temp = "";
            for (String item : childJobIds) {
                temp += item + ",";
            }
            temp = temp.substring(0, temp.length() - 1);

            jobInfo.setChildJobId(temp);
        }

        //表同步
        //todo 查询所有table，设置到jobJson的 job->content->reader->parameter->connection->table数组
        //writer->parameter->connection->table数组
        JSONObject jobJsonObject = JSONObject.parseObject(jobInfo.getJobJson());
        JSONObject jobParams = JSONObject.parseObject(jobJsonObject.getString("job"));
        JSONObject contentJsonObject = JSONObject.parseObject(jobParams.getString("content"));

        JSONObject readerJsonObject = JSONObject.parseObject(contentJsonObject.getString("reader"));

        boolean isJdbcQuery = readerJsonObject.containsKey("querySql");
        if(!isJdbcQuery){
            throw new UnsupportedOperationException("该方法仅支持reader为querySql方式！");
        }

        JSONObject writerJsonObject = JSONObject.parseObject(contentJsonObject.getString("writer"));
        JSONObject writerParameterJsonObject = JSONObject.parseObject(writerJsonObject.getString("parameter"));

        String dUsername = AESUtil.decrypt(writerParameterJsonObject.getString("username"));
        String dPassword = AESUtil.decrypt(writerParameterJsonObject.getString("password"));

        JSONObject writerConnectionJsonObject = JSONObject.parseObject(writerParameterJsonObject.getString("connection"));
        String jdbcUrl = writerConnectionJsonObject.getString("jdbcUrl");
        //todo 查询所有table


        //存储过程同步




        // add in db
        jobInfo.setAddTime(new Date());
        jobInfo.setJobJson(jobInfo.getJobJson());
        jobInfo.setUpdateTime(new Date());
        jobInfo.setGlueUpdatetime(new Date());
        jobInfoMapper.save(jobInfo);
        if (jobInfo.getId() < 1) {
            return new ReturnT<>(ReturnT.FAIL_CODE, (I18nUtil.getString("jobinfo_field_add") + I18nUtil.getString("system_fail")));
        }

        return new ReturnT<>(String.valueOf(jobInfo.getId()));
    }

    private boolean isNumeric(String str) {
        try {
            Integer.valueOf(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

}
