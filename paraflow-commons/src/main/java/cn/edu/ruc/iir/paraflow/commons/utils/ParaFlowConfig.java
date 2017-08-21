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
public class ParaFlowConfig
{
    private String configPath;
    private Properties properties;
    private Properties defaultProps;

    public ParaFlowConfig(String configPath)
    {
        this.configPath = configPath;
        defaultProps = new Properties();
    }

    public ParaFlowConfig build() throws ConfigFileNotFoundException
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

    public ParaFlowConfig setDefault(String key, String value)
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
