package com.wugui.datax.admin.service;

import com.wugui.datatx.core.biz.model.ReturnT;
import com.wugui.datax.admin.entity.JobInfo;
import com.wugui.datax.admin.entity.JobSyncDll;

import java.io.IOException;

/**
 * (一句话功能简述)
 * <p> (功能详细描述)
 *
 * @author : lijie
 * @since 版本号
 * Date: 5/31/23
 */
public interface JobSyncDllService {
    /**
     * 添加job，包含生成该库下所有table到job内容json中
     * @param jobSyncDll
     * @return
     */
    ReturnT<String> addAllTable(JobSyncDll jobSyncDll) throws IOException;

    /**
     * 添加job，包含生成该库下所有DLL到job内容json中
     * @param jobInfo
     * @return
     */
    ReturnT<String> addAllDll(JobInfo jobInfo);
}
