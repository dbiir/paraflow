package cn.edu.ruc.iir.paraflow.commons.utils;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigFactory
{
	private static ConfigFactory instance = null;

	/**
	 * Injected classes in paraflow-presto should not use this method to get ConfigFactory Instance.
	 * @return
	 */
	public static ConfigFactory Instance ()
	{
		if (instance == null)
		{
			instance = new ConfigFactory();
		}
		return instance;
	}

	// Properties is thread safe, so we do not put synchronization on it.
	private Properties prop = null;

	private ConfigFactory()
	{
		prop = new Properties();
		String paraflowHome = System.getenv("PARAFLOW_HOME");
		InputStream in = null;
		if (paraflowHome == null)
		{
			in = this.getClass().getResourceAsStream("/paraflow.properties");
		}
		else
		{
			if (!(paraflowHome.endsWith("/") || paraflowHome.endsWith("\\")))
			{
				paraflowHome += "/";
			}
			prop.setProperty("paraflow.home", paraflowHome);
			try
			{
				in = new FileInputStream(paraflowHome + "paraflow.properties");
			} catch (FileNotFoundException e)
			{
				e.printStackTrace();
			}
		}
		try {
			if (in != null)
			{
				prop.load(in);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void loadProperties(String propFilePath) throws IOException
	{
		FileInputStream in = null;
		try
		{
			in = new FileInputStream(propFilePath);
			this.prop.load(in);
		} catch (IOException e)
		{
			throw e;
		} finally
		{
			if (in != null)
			{
				try
				{
					in.close();
				} catch (IOException e)
				{
					throw e;
				}
			}
		}
	}

	public void addProperty (String key, String value)
	{
		this.prop.setProperty(key, value);
	}

	public String getProperty (String key)
	{
		return this.prop.getProperty(key);
	}

	private ParaFlowConfig paraflowConfig;

	private static class ConfigFactoryHolder
	{
		public static final ConfigFactory instance = new ConfigFactory();
	}

	public static final ConfigFactory INSTANCE()
	{
		return ConfigFactoryHolder.instance;
	}

	public void init(String configPath) throws ConfigFileNotFoundException
	{
		paraflowConfig = new ParaFlowConfig(configPath);
		paraflowConfig.build();
	}

	public int getServerPort()
	{
		return Integer.parseInt(paraflowConfig.getProperty("server.port"));
	}

	public String getDBDriver()
	{
		return paraflowConfig.getProperty("db.driver");
	}

	public String getDBHost()
	{
		return paraflowConfig.getProperty("db.host");
	}

	public String getDBUser()
	{
		return paraflowConfig.getProperty("db.user");
	}

	public String getDBPassword()
	{
		return paraflowConfig.getProperty("db.password");
	}

	public String getHDFSWarehouse()
	{
		return paraflowConfig.getProperty("hdfs.warehouse");
	}
}
