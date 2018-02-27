package cn.edu.ruc.iir.paraflow.connector.utils;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.utils.ParaFlowConfig;

public class ConfigFactory
{
	private ParaFlowConfig paraflowConfig;

	public ConfigFactory()
	{}

	private static class ConfigFactoryHolder
	{
		public static final ConfigFactory instance = new ConfigFactory();
	}

	public static final ConfigFactory INSTANCE()
	{
		return ConfigFactoryHolder.instance;
	}

	public void init() throws ConfigFileNotFoundException
	{
		paraflowConfig = new ParaFlowConfig("/home/yixuanwang/Documents/paraflow/dist/conf/metaserver.conf");
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
