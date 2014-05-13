package com.sihuatech.webcache.memcached;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import com.onewaveinc.mip.core.ModuleContextHolder;
import com.onewaveinc.mip.log.Logger;
import com.onewaveinc.mrc.ModuleFile;
import com.whalin.MemCached.MemCachedClient;
import com.whalin.MemCached.SockIOPool;
/**
 * memcached缓存服务器连接工具类,使用memcached java client 实现
 * @author SLM
 *
 */
public class Memcached {
	private static Logger logger = Logger.getInstance(Memcached.class);
	private static final String POOL_NAME = "webcache.ram.share";
	private static Memcached memcached;
	private String servers;
	private int initialConnections;
	private int minSpareConnections;
	private int maxSpareConnections;

	protected Memcached() {
	}

	public static Memcached getInstance() {
		if (null == memcached) {
			memcached = new Memcached();
		}
		return memcached;
	}

	private static MemCachedClient mcc = new MemCachedClient(POOL_NAME);
	private SockIOPool pool = SockIOPool.getInstance(POOL_NAME);

	protected void initSockIOPool() {
		logger.info("Initializes the pool-->" + POOL_NAME);
		if(StringUtils.isBlank(servers)){
			ModuleFile configFile = ModuleContextHolder.getModuleContext().getModule("webcache.basic").getModuleFile("/META-INF/services/config/spring/config.properties");
			Properties pro = new Properties();
			InputStream input = null;
			try {
				input = new FileInputStream(configFile);
				pro.load(input);
			} catch (FileNotFoundException e) {
				logger.error("配置文件/webcache.basic/META-INF/services/config/spring/config.properties不存在", e);
			} catch (IOException e) {
				logger.error("获取配置文件/webcache.basic//META-INF/services/config/spring/config.properties出现异常", e);
			} finally {
				if(null != input){
					IOUtils.closeQuietly(input);
				}
			}
			servers = pro.getProperty("memcached.servers");
			initialConnections = Integer.valueOf(pro.getProperty("memcached.initialConnections"));
			minSpareConnections = Integer.valueOf(pro.getProperty("memcached.minSpareConnections"));
			maxSpareConnections = Integer.valueOf(pro.getProperty("memcached.maxSpareConnections"));
		}
		if(StringUtils.isNotBlank(servers)){
			String[] tmp = servers.split(",");
			if(tmp.length > 0){
				List<String> serverlist = new ArrayList<String>(tmp.length);
				List<Integer> weights = new ArrayList<Integer>(tmp.length);
				for(String t : tmp){
					String[] siw = t.split(":");
					if(siw.length == 3){
						serverlist.add(siw[0] + ":" + siw[1]);
						weights.add(Integer.valueOf(siw[2]));
					}else{
						logger.error("memcached服务地址[" + t + "]配置不符合规范");
					}
				}
				pool.setServers(serverlist.toArray(new String[serverlist.size()]));
				pool.setWeights(weights.toArray(new Integer[weights.size()]));
		        pool.setInitConn(initialConnections);
		        pool.setMinConn(minSpareConnections);
		        pool.setMaxConn(maxSpareConnections);
		        pool.setMaxIdle(1000 * 60 * 30);
		        pool.setMaxBusyTime(1000 * 60 * 5);
		        pool.setMaintSleep(1000 * 5);
		        pool.setSocketTO(1000 * 3);
		        pool.setSocketConnectTO(1000 * 3);
		        pool.setFailback(true);
		        pool.setFailover(true);
		        pool.setNagle(false);
		        pool.setHashingAlg(SockIOPool.NEW_COMPAT_HASH);
		        pool.setAliveCheck(true);
		        pool.initialize();
		        logger.info("Initializes the pool success-->" + POOL_NAME);
			}
		}else{
			logger.error("配置项：memcached服务地址及权重值[memcached.servers]没有正确配置");
		}
	}
	
	protected void destorySockIOPool(){
		if(null != pool){
			logger.info("Shuts down the pool-->" + POOL_NAME);
			pool.shutDown();
		}
	}
	
	/**
	 * 检查memcached节点状态
	 * @return 
	 */
	public MCStatus status(){
		//所有配置的服务节点
		String[] servers = pool.getServers();
		MCStatus mcStatus = new MCStatus(servers.length);
		StringBuilder stat = new StringBuilder();
		//存活的节点
		Map<String, Map<String, String>> alive = mcc.statsSlabs();
		Set<String> aliveServerName = alive.keySet();
		for(String server : servers){
			if(aliveServerName.contains(server)){
				stat.append(server).append(MCStatus.ON).append(System.getProperty("line.separator"));
			}else{
				//down的节点
				stat.append(server).append(MCStatus.OFF).append(System.getProperty("line.separator"));
				mcStatus.setDown(mcStatus.getDown() + 1);
			}
		}
		mcStatus.setNodeStatus(stat.toString());
		
		return mcStatus;
	}
	
	/**
	 * 保存或者递增指定的key
	 * @param key
	 * @param inc 递增的值
	 * @return
	 */
	public long addOrIncr(String key, long inc){
		return mcc.addOrIncr(key, inc);
	}
	
	/**
	 * 保存或者递减指定的key
	 * @param key
	 * @param inc 递减的值
	 * @return
	 */
	public long addOrDecr(String key, long inc){
		return mcc.addOrDecr(key, inc);
	}
	
	/**
	 * 将数据添加到cache服务器
	 * <br>如果保存成功则返回true,如果cache服务器存在同样key，则返回false
	 * @param key
	 * @param value 必须实现Serializable接口
	 * @return
	 */
	public boolean add(String key, Object value){
		return mcc.add(key, value);
	}
	
	/**
	 * 将数据添加到cache服务器,并指定有效期
	 * <br>如果保存成功则返回true,如果cache服务器存在同样key，则返回false
	 * @param key
	 * @param value 必须实现Serializable接口
	 * @param expiry 有效期
	 * @return
	 */
	public boolean add(String key, Object value, Date expiry){
		return mcc.add(key, value, expiry);
	}
	
	/**
	 * 将数据保存到cache服务器
	 * <br>如果保存成功则返回true,如果cache服务器存在同样的key，则替换之
	 * @param key
	 * @param value 必须实现Serializable接口
	 * @return
	 */
	public boolean set(String key, Object value){
		return mcc.set(key, value);
	}
	
	/**
	 * 将数据保存到cache服务器,并指定有效期
	 * <br>如果保存成功则返回true,如果cache服务器存在同样的key，则替换之
	 * @param key
	 * @param value 必须实现Serializable接口
	 * @param expiry 有效期
	 * @return
	 */
	public boolean set(String key, Object value, Date expiry){
		return mcc.set(key, value, expiry);
	}
	
	/**
	 * 将数据替换cache服务器中相同的key
	 * <br>如果保存成功则返回true,如果cache服务器不存在同样key，则返回false
	 * @param key
	 * @param value 必须实现Serializable接口
	 * @return
	 */
	public boolean replace(String key, Object value){
		return mcc.replace(key, value);
	}
	
	/**
	 * 将数据替换cache服务器中相同的key并指定有效期
	 * <br>如果保存成功则返回true,如果cache服务器不存在同样key，则返回false
	 * @param key
	 * @param value 必须实现Serializable接口
	 * @param expiry 有效期
	 * @return
	 */
	public boolean replace(String key, Object value, Date expiry){
		return mcc.replace(key, value, expiry);
	}
	
	/**
	 * 从cache服务器获取一个数据
	 * @param key
	 * @return
	 */
	public Object get(String key){
		return mcc.get(key);
	}
	
	/**
	 * 从cache服务器获取一组数据
	 * <br>根据keys映射为一个map:key是输入的keys,value是查到的值
	 * @param keys
	 * @return
	 */
	public Map<String, Object> getMulti(String[] keys){
		return mcc.getMulti(keys);
	}
	
	/**
	 * 从cache服务器获取一组数据
	 * <br>按照输入的keys的顺序返回查到的值
	 * @param keys
	 * @return
	 */
	public Object[] getMultiArray(String[] keys){
		return mcc.getMultiArray(keys);
	}
	
	/**
	 * 从cache服务器删除对应的key：value键值对
	 * @param key
	 * @return
	 */
	public boolean delete(String key){
		return mcc.delete(key);
	}

	public void setServers(String servers) {
		this.servers = servers;
	}

	public void setInitialConnections(int initialConnections) {
		this.initialConnections = initialConnections;
	}

	public void setMinSpareConnections(int minSpareConnections) {
		this.minSpareConnections = minSpareConnections;
	}

	public void setMaxSpareConnections(int maxSpareConnections) {
		this.maxSpareConnections = maxSpareConnections;
	}

	
}
