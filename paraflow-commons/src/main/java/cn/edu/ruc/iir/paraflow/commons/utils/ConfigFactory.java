package cn.edu.ruc.iir.paraflow.commons.utils;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * ParaFlow
 *
 * @author guodong
 */
public class ConfigFactory
{
    private String configPath;
    private Properties properties;
    private Properties defaultProps;

    private ConfigFactory(String configPath)
    {
        this.configPath = configPath;
        defaultProps = new Properties();
    }

    private static ConfigFactory configInstance = null;

    public static synchronized ConfigFactory getConfigInstance(String configPath)
    {
        if (configInstance == null) {
            configInstance = new ConfigFactory(configPath);
        }
        return configInstance;
    }

    public ConfigFactory build() throws ConfigFileNotFoundException
    {
        properties = new Properties(defaultProps);
        try {
            properties.load(new FileInputStream(new File(this.configPath)));
        }
        catch (IOException e) {
            throw new ConfigFileNotFoundException(this.configPath);
        }

        return this;
    }

    public ConfigFactory setDefault(String key, String value)
    {
        this.defaultProps.setProperty(key, value);

        return this;
    }

    public void addProperty(String key, String value)
    {
        this.properties.setProperty(key, value);
    }

    public String getProperty(String key)
    {
        return this.properties.getProperty(key);
    }
}
