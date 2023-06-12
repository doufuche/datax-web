package com.wugui.datax.admin.entity;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

/**
 * (一句话功能简述)
 * <p> (功能详细描述)
 *
 * @author : lijie
 * @since 版本号
 * Date: 5/31/23
 */
@Data
public class JobSyncDll {
    @ApiModelProperty("同步来源的数据源id")
    Long fromDbId;
    @ApiModelProperty("来源db的schema")
    String fromDbSchemaName;

    @ApiModelProperty("被同步的数据源id")
    Long toDbId;
    @ApiModelProperty("被同步db的schema")
    String toDbSchemaName;

    @ApiModelProperty("标准job信息")
    JobInfo jobInfo;

}
