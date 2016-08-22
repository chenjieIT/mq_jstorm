package com.databig.jstorm;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.yaml.snakeyaml.Yaml;

import com.alibaba.jstorm.utils.JStormUtils;
import com.databig.jstorm.bolt.WeatherDataAnalyBolt;
import com.databig.jstorm.common.log.LogConfigUtils;
import com.databig.jstorm.spout.RabbitMqWeatherDataSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class MonitorTopology {
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		LogConfigUtils.logInfo("Submit Topology");
		if (args.length == 0) {
			System.err.println("Please input configuration file");
			System.exit(-1);
		}
		// TODO 自动生成的方法存根
		LoadConf(args[0]);
		TopologyBuilder builder;
		try {
			builder = setupBuilder();
			submitTopology(builder);
		} catch (Exception e1) {
			LogConfigUtils.logError(e1.getMessage(), e1.getCause());
		}

	}
	
	private static TopologyBuilder setupBuilder() throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		int writerParallel = JStormUtils.parseInt(
				conf.get("topology.writer.parallel"), 1);

		int spoutParallel = JStormUtils.parseInt(
				conf.get("topology.spout.parallel"), 1);

		builder.setSpout("RabbitMqWeatherDataSpout", new RabbitMqWeatherDataSpout(), spoutParallel);

		builder.setBolt("WeatherDataAnalyBolt", new WeatherDataAnalyBolt(), writerParallel)
				.shuffleGrouping("RabbitMqWeatherDataSpout");

		return builder;
	}

	private static void submitTopology(TopologyBuilder builder) {
		try {
			if (local_mode(conf)) {

				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology(
						String.valueOf(conf.get("topology.name")), conf,
						builder.createTopology());

				Thread.sleep(200000);
				//结束拓扑
				cluster.killTopology(String.valueOf(conf.get("topology.name")));
				cluster.shutdown();
			} else {
				StormSubmitter.submitTopology(
						String.valueOf(conf.get("topology.name")), conf,
						builder.createTopology());
			}

		} catch (Exception e) {
			LogConfigUtils.logError(e.getMessage(), e.getCause());
		}
	}

	private static Map<Object, Object> conf = new HashMap<Object, Object>();

	private static void LoadProperty(String prop) {
		Properties properties = new Properties();

		try {
			InputStream stream = new FileInputStream(prop);
			properties.load(stream);
		} catch (FileNotFoundException e) {
			LogConfigUtils.logError(e.getMessage(), e.getCause());
		} catch (Exception e1) {
			LogConfigUtils.logError(e1.getMessage(), e1.getCause());

			return;
		}

		conf.putAll(properties);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static void LoadYaml(String confPath) {

		Yaml yaml = new Yaml();

		try {
			InputStream stream = new FileInputStream(confPath);

			conf = (Map) yaml.load(stream);
			if (conf == null || conf.isEmpty() == true) {
				throw new RuntimeException("Failed to read config file");
			}

		
		} catch (Exception e1) {
			LogConfigUtils.logError(e1.getMessage(), e1.getCause());
			throw new RuntimeException("Failed to read config file");
		}

		return;
	}

	private static void LoadConf(String arg) {
		if (arg.endsWith("yaml")) {
			LoadYaml(arg);
		} else {
			LoadProperty(arg);
		}
	}

	public static boolean local_mode(Map<Object, Object> conf) {
		String mode = (String) conf.get(Config.STORM_CLUSTER_MODE);
		if (mode != null) {
			if (mode.equals("local")) {
				return true;
			}
		}

		return false;

	}

}
