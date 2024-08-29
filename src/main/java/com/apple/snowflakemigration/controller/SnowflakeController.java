package com.apple.snowflakemigration.controller;


import com.apple.snowflakemigration.model.SnowflakeProperties;
import com.apple.snowflakemigration.service.SnowflakeService;
import com.apple.snowflakemigration.util.SnowflakeUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/snowflake")
public class SnowflakeController {

    @Autowired
    SnowflakeService snowflakeService;


    @PostMapping("/export")
    public void exportS3ToSnowflake(@RequestBody SnowflakeProperties snowflakeProperties){
        snowflakeService.exportS3ObjectsToSnowflake(snowflakeProperties);
    }

    @GetMapping("/isTableExists")
    public boolean isTableExists(@RequestBody SnowflakeProperties snowflakeProperties,@RequestParam String tableName){
        return snowflakeService.isTableExists(snowflakeProperties,tableName);
    }

}
