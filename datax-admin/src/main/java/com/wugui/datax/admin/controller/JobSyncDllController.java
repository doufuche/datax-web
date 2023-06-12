package com.wugui.datax.admin.controller;

import com.wugui.datatx.core.biz.model.ReturnT;
import com.wugui.datax.admin.entity.JobInfo;
import com.wugui.datax.admin.entity.JobSyncDll;
import com.wugui.datax.admin.service.JobSyncDllService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

/**
 * (一句话功能简述)
 * <p> (功能详细描述)
 *
 * @author : lijie
 * @since 版本号
 * Date: 5/31/23
 */
@Api(tags = "任务配置接口")
@RestController
@RequestMapping("/api/job/sync")
public class JobSyncDllController extends BaseController {

    @Autowired
    private JobSyncDllService jobSyncDllService;

    @PostMapping("/table/add")
    @ApiOperation("添加任务,由调用方将所有表生成job")
    public ReturnT<String> addAllTable(HttpServletRequest request, @RequestBody JobSyncDll jobSyncDll) throws IOException {
        jobSyncDll.getJobInfo().setUserId(getCurrentUserId(request));
        return jobSyncDllService.addAllTable(jobSyncDll);
    }


    @PostMapping("/dll/add")
    @ApiOperation("添加任务,包含所有DLL生成(存储过程、视图、序列)")
    public ReturnT<String> addAllDll(HttpServletRequest request, @RequestBody JobInfo jobInfo) {
        jobInfo.setUserId(getCurrentUserId(request));
        return jobSyncDllService.addAllDll(jobInfo);
    }

}
