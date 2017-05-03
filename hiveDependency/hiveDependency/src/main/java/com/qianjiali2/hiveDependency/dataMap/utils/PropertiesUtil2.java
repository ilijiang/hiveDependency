package com.qianjiali2.hiveDependency.dataMap.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;

import com.qianjiali.hiveDependency.entity.HiveConfig;

public class PropertiesUtil2 {

    private static Log logger = LogFactory.getLog(PropertiesUtil2.class);
    private static Properties pros = new Properties();

    public static HiveConfig initProperties() {
        System.out.println("开始加载配置资源....................");
        String rootPath = System.getProperty("user.dir");
        System.out.println("资源根路径是======================>>>>>" + rootPath);
        String resourcePath = rootPath + "/config/config.properties";
        InputStream is = null;
        try {
            File file = new File(resourcePath);
            is = new FileInputStream(file);
            pros.load(is);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("资源文件找不到");
        }
        HiveConfig conf = HiveConfig.getInstance();
        conf.setHdfsConfig(pros.getProperty("hdfs_config_url"));
        conf.setHdfsDest(pros.getProperty("hdfs_script_parse_relation_map_url"));
        conf.setHdfsDir(pros.getProperty("hdfs_script_save_dir_url"));
      //  conf.setFilterPrefix(pros.getProperty("hive_parse_filter_suffix"));
      //  conf.setFilterSuffix(pros.getProperty("hive_parse_filter_prefix"));
        System.out.println("配置资源加载完成....................");
        return conf;
    }

    public static String getPropertiesValue(String propertiesKey) {
        if (pros.isEmpty()) {
            initProperties();
        }
        return pros.getProperty(propertiesKey);
    }
}
