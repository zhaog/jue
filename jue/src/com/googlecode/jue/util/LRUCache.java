package com.googlecode.jue.util;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * LRU 缓存
 * @author noah
 */
public class LRUCache<K, V> extends LinkedHashMap<K, V> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -290657338531701966L;

	/**
	 * 最大缓存数量
	 */
	private int maxCapacity;
	
	/**
	 * 缓存锁
	 */
	private ReentrantLock lock = new ReentrantLock();

	/**
	 * 构建一个缓存集合
	 * @param maxCapacity 最大数量
	 */
	public LRUCache(int maxCapacity) {
		super(maxCapacity, 0.75f, true);
		this.maxCapacity = maxCapacity;
	}

	@Override
	protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
		return (size() > maxCapacity);
	}

	@Override
	public V get(Object key) {
		try {
			lock.lock();
			return super.get(key);
		} finally {
			lock.unlock();
		}
	}

	public V put(K key, V value) {
		try {
			lock.lock();
			return super.put(key, value);
		} finally {
			lock.unlock();
		}
	};
}
