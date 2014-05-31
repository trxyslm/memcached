package com.sihuatech.webcache.memcached;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import net.rubyeye.xmemcached.HashAlgorithm;
import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.MemcachedClientBuilder;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.command.BinaryCommandFactory;
import net.rubyeye.xmemcached.exception.MemcachedException;
import net.rubyeye.xmemcached.impl.KetamaMemcachedSessionLocator;
import net.rubyeye.xmemcached.utils.AddrUtil;

import org.apache.commons.lang.StringUtils;

import com.onewaveinc.mip.log.Logger;
/**
 * memcached缓存服务器连接工具类,使用 Xmemcached 实现
 * @author SLM
 *
 */
public class XMemcached {
	private static Logger logger = Logger.getInstance(XMemcached.class);
	private static final String POOL_NAME = "webcache.ram.share";
	private static XMemcached memcached;
	private String servers;
	private int initialConnections;
	private int minSpareConnections;
	private int maxSpareConnections;
	private static List<InetSocketAddress> isa = new ArrayList<InetSocketAddress>();

	protected XMemcached() {
	}

	public synchronized static XMemcached getInstance() {
		if (null == memcached) {
			memcached = new XMemcached();
		}
		return memcached;
	}

	private static MemcachedClient mcc = null;
	protected void init(){
		logger.info("开始初始化Xmemcached客户端");
		if(StringUtils.isNotBlank(servers)){
			String[] tmp = servers.split(",");
			if(tmp.length > 0){
				StringBuilder serversStr = new StringBuilder();
				List<Integer> weightsList = new ArrayList<Integer>(tmp.length);
				for(String t : tmp){
					String[] siw = t.split(":");
					if(siw.length == 3){
						serversStr.append(siw[0] + ":" + siw[1]).append(" ");
						weightsList.add(Integer.valueOf(siw[2]));
					}else{
						logger.error("memcached服务地址[" + t + "]配置不符合规范");
					}
				}
				if(serversStr.length() > 0){
					serversStr = serversStr.deleteCharAt(serversStr.length() - 1);
					logger.info("init servers:" + serversStr.toString());
					int[] weights = new int[weightsList.size()];
					for(int i = 0, len = weightsList.size(); i < len; i++){
						weights[i] = weightsList.get(i);
					}
					logger.info("weights:" + Arrays.toString(weights));
					isa = AddrUtil.getAddresses(serversStr.toString());
					MemcachedClientBuilder builder = new XMemcachedClientBuilder(isa, weights);
					builder.setName(POOL_NAME);
					builder.setConnectionPoolSize(Runtime.getRuntime().availableProcessors());
					builder.setSessionLocator(new KetamaMemcachedSessionLocator(HashAlgorithm.KETAMA_HASH));
					builder.setCommandFactory(new BinaryCommandFactory());
					try {
						mcc = builder.build();
						logger.info("初始化memcached客户端成功");
					} catch (IOException e) {
						logger.error("初始化memcached客户端出现异常", e);
					}
				}else{
					logger.error("没有可用的服务地址");
				}
			}
		}else{
			logger.error("配置项：memcached服务地址及权重值[memcached.servers]没有正确配置");
		}
	}
	
	protected void destroy(){
		if(null != mcc){
			try {
				mcc.shutdown();
				logger.info("关闭memcached客户端成功");
			} catch (IOException e) {
				logger.error("关闭memcache客户端异常", e);
			}
		}
	}
	
	/**
	 * 检查memcached节点状态
	 * @return 
	 */
	public MCStatus status(){
		//所有配置的服务节点
		MCStatus mcStatus = new MCStatus(isa.size());
		StringBuilder stat = new StringBuilder();
		//存活的节点
		Collection<InetSocketAddress> aliveServerName = mcc.getAvailableServers();
		for(InetSocketAddress server : isa){
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
	/*public long addOrIncr(String key, long inc){
		return mcc.addOrIncr(key, inc);
	}*/
	
	/**
	 * 保存或者递减指定的key
	 * @param key
	 * @param inc 递减的值
	 * @return
	 */
	/*public long addOrDecr(String key, long inc){
		return mcc.addOrDecr(key, inc);
	}*/
	
	/**
	 * 将数据添加到cache服务器
	 * <br>如果保存成功则返回true,如果cache服务器存在同样key，则返回false
	 * @param key
	 * @param value 必须实现Serializable接口
	 * @return
	 */
	public boolean add(String key, Object value){
		try {
			return mcc.add(key, 0, value);
		} catch (TimeoutException e) {
			logger.error("key[" + key + "]add异常", e);
		} catch (InterruptedException e) {
			logger.error("key[" + key + "]add异常", e);
		} catch (MemcachedException e) {
			logger.error("key[" + key + "]add异常", e);
		}
		return false;
	}
	
	/**
	 * 将数据添加到cache服务器,并指定有效期
	 * <br>如果保存成功则返回true,如果cache服务器存在同样key，则返回false
	 * @param key
	 * @param value 必须实现Serializable接口
	 * @param expiry 有效期(时间为秒)
	 * @return
	 */
	public boolean add(String key, Object value, int expiry){
		try {
			return mcc.add(key, expiry, value);
		} catch (TimeoutException e) {
			logger.error("key[" + key + "]add异常", e);
		} catch (InterruptedException e) {
			logger.error("key[" + key + "]add异常", e);
		} catch (MemcachedException e) {
			logger.error("key[" + key + "]add异常", e);
		}
		return false;
	}
	
	/**
	 * 将数据保存到cache服务器
	 * <br>如果保存成功则返回true,如果cache服务器存在同样的key，则替换之
	 * @param key
	 * @param value 必须实现Serializable接口
	 * @return
	 */
	public boolean set(String key, Object value){
		try {
			return mcc.set(key, 0, value);
		} catch (TimeoutException e) {
			logger.error("key[" + key + "]set异常", e);
		} catch (InterruptedException e) {
			logger.error("key[" + key + "]set异常", e);
		} catch (MemcachedException e) {
			logger.error("key[" + key + "]set异常", e);
		}
		return false;
	}
	
	/**
	 * 将数据保存到cache服务器,并指定有效期
	 * <br>如果保存成功则返回true,如果cache服务器存在同样的key，则替换之
	 * @param key
	 * @param value 必须实现Serializable接口
	 * @param expiry 有效期(单位为秒)
	 * @return
	 */
	public boolean set(String key, Object value, int expiry){
		try {
			return mcc.set(key, expiry, value);
		} catch (TimeoutException e) {
			logger.error("key[" + key + "]set异常", e);
		} catch (InterruptedException e) {
			logger.error("key[" + key + "]set异常", e);
		} catch (MemcachedException e) {
			logger.error("key[" + key + "]set异常", e);
		}
		return false;
	}
	
	/**
	 * 将数据替换cache服务器中相同的key
	 * <br>如果保存成功则返回true,如果cache服务器不存在同样key，则返回false
	 * @param key
	 * @param value 必须实现Serializable接口
	 * @return
	 */
	public boolean replace(String key, Object value){
		try {
			return mcc.replace(key, 0, value);
		} catch (TimeoutException e) {
			logger.error("key[" + key + "]replace异常", e);
		} catch (InterruptedException e) {
			logger.error("key[" + key + "]replace异常", e);
		} catch (MemcachedException e) {
			logger.error("key[" + key + "]replace异常", e);
		}
		return false;
	}
	
	/**
	 * 将数据替换cache服务器中相同的key并指定有效期
	 * <br>如果保存成功则返回true,如果cache服务器不存在同样key，则返回false
	 * @param key
	 * @param value 必须实现Serializable接口
	 * @param expiry 有效期(单位为秒)
	 * @return
	 */
	public boolean replace(String key, Object value, int expiry){
		try {
			return mcc.replace(key, expiry, value);
		} catch (TimeoutException e) {
			logger.error("key[" + key + "]replace异常", e);
		} catch (InterruptedException e) {
			logger.error("key[" + key + "]replace异常", e);
		} catch (MemcachedException e) {
			logger.error("key[" + key + "]replace异常", e);
		}
		return false;
	}
	
	/**
	 * 从cache服务器获取一个数据
	 * @param key
	 * @return
	 */
	public Object get(String key){
		try {
			return mcc.get(key);
		} catch (TimeoutException e) {
			logger.error("key[" + key + "]get异常", e);
		} catch (InterruptedException e) {
			logger.error("key[" + key + "]get异常", e);
		} catch (MemcachedException e) {
			logger.error("key[" + key + "]get异常", e);
		}
		return null;
	}
	
	/**
	 * 从cache服务器获取一组数据
	 * <br>根据keys映射为一个map:key是输入的keys,value是查到的值
	 * @param keys
	 * @return
	 */
	public Map<String, Object> getMulti(List<String> keys){
		try {
			return mcc.get(keys);
		} catch (TimeoutException e) {
			logger.error("keys[" + keys + "]getMulti异常", e);
		} catch (InterruptedException e) {
			logger.error("keys[" + keys + "]getMulti异常", e);
		} catch (MemcachedException e) {
			logger.error("keys[" + keys + "]getMulti异常", e);
		}
		return null;
	}
	
	/**
	 * 从cache服务器获取一组数据
	 * <br>按照输入的keys的顺序返回查到的值
	 * @param keys
	 * @return
	 */
	/*public Object[] getMultiArray(String[] keys){
		return mcc.getMultiArray(keys);
	}*/
	
	/**
	 * 从cache服务器删除对应的key：value键值对
	 * @param key
	 * @return
	 */
	public boolean delete(String key){
		try {
			return mcc.delete(key);
		} catch (TimeoutException e) {
			logger.error("key[" + key + "]delete异常", e);
		} catch (InterruptedException e) {
			logger.error("key[" + key + "]delete异常", e);
		} catch (MemcachedException e) {
			logger.error("key[" + key + "]delete异常", e);
		}
		return false;
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
