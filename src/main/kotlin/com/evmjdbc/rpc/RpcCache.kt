package com.evmjdbc.rpc

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

/**
 * LRU cache for RPC responses with TTL support.
 */
class RpcCache(
    private val defaultTtlSeconds: Int,
    private val maxSize: Int = 1000
) {
    private data class CacheEntry(
        val response: RpcResponse,
        val expiresAt: Instant
    )

    private val cache = ConcurrentHashMap<String, CacheEntry>()
    private val accessOrder = java.util.LinkedHashMap<String, Long>(maxSize, 0.75f, true)

    /**
     * Generate cache key from request.
     */
    private fun cacheKey(request: RpcRequest): String {
        return "${request.method}:${request.params.hashCode()}"
    }

    /**
     * Get cached response if not expired.
     */
    fun get(request: RpcRequest): RpcResponse? {
        val key = cacheKey(request)
        val entry = cache[key] ?: return null

        if (Instant.now().isAfter(entry.expiresAt)) {
            cache.remove(key)
            synchronized(accessOrder) {
                accessOrder.remove(key)
            }
            return null
        }

        synchronized(accessOrder) {
            accessOrder[key] = System.currentTimeMillis()
        }

        return entry.response
    }

    /**
     * Cache a response with TTL based on method type.
     */
    fun put(request: RpcRequest, response: RpcResponse) {
        val key = cacheKey(request)
        val ttl = getTtlForMethod(request.method)
        val entry = CacheEntry(
            response = response,
            expiresAt = Instant.now().plusSeconds(ttl.toLong())
        )

        // Evict oldest entries if at capacity
        while (cache.size >= maxSize) {
            evictOldest()
        }

        cache[key] = entry
        synchronized(accessOrder) {
            accessOrder[key] = System.currentTimeMillis()
        }
    }

    /**
     * Get TTL for a specific method.
     */
    private fun getTtlForMethod(method: String): Int {
        return when (method) {
            // Immutable blockchain data - long TTL
            "eth_getBlockByNumber", "eth_getBlockByHash" -> defaultTtlSeconds * 10
            "eth_getTransactionByHash", "eth_getTransactionReceipt" -> defaultTtlSeconds * 10
            "eth_getLogs" -> defaultTtlSeconds * 5

            // Chain info - short TTL
            "eth_blockNumber" -> 5
            "eth_gasPrice" -> 15

            // Don't cache state queries by default
            else -> defaultTtlSeconds
        }
    }

    /**
     * Evict the least recently used entry.
     */
    private fun evictOldest() {
        val oldestKey = synchronized(accessOrder) {
            accessOrder.entries.firstOrNull()?.key
        } ?: return

        cache.remove(oldestKey)
        synchronized(accessOrder) {
            accessOrder.remove(oldestKey)
        }
    }

    /**
     * Clear all cached entries.
     */
    fun clear() {
        cache.clear()
        synchronized(accessOrder) {
            accessOrder.clear()
        }
    }

    /**
     * Get current cache size.
     */
    fun size(): Int = cache.size
}
