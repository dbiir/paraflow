package cn.edu.ruc.iir.paraflow.commons.utils;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ParaFlowConfig
{
    private String configFile;
    private Properties properties;

    public ParaFlowConfig(String configFile)
    {
        this.configFile = configFile;
    }

    public ParaFlowConfig build() throws ConfigFileNotFoundException
    {
        properties = new Properties();
        String paraflowHome = System.getenv("PARAFLOW_HOME");
        InputStream in = null;
        if (paraflowHome != null) {
            if (!paraflowHome.endsWith("/") || paraflowHome.endsWith("\\")) {
                paraflowHome += "/";
            }
            paraflowHome += "config/";
            properties.setProperty("paraflow.home", paraflowHome);
            try {
                in = new FileInputStream(paraflowHome + configFile);
            }
            catch (FileNotFoundException e) {
                throw new ConfigFileNotFoundException(configFile);
            }
        }
        if (in != null) {
            try {
                properties.load(in);
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        return this;
    }

    public void loadProperties(String propFilePath)
    {
        try (FileInputStream in = new FileInputStream(propFilePath)) {
            this.properties.load(in);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setProperty(String key, String value)
    {
        this.properties.setProperty(key, value);
    }

    public String getProperty(String key)
    {
        return this.properties.getProperty(key).trim();
    }

    public Properties getProperties()
    {
        return properties;
    }
}
