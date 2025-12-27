package com.evmjdbc.rpc

import com.evmjdbc.connection.ConnectionProperties
import org.slf4j.LoggerFactory
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.sql.SQLException
import java.sql.SQLNonTransientConnectionException
import java.sql.SQLTransientConnectionException
import java.time.Duration
import kotlin.math.min
import kotlin.random.Random

/**
 * HTTP client for JSON-RPC communication with Ethereum nodes.
 * Implements retry with exponential backoff and rate limit handling.
 */
class RpcClient(
    private val rpcUrl: String,
    private val properties: ConnectionProperties
) : AutoCloseable {
    private val logger = LoggerFactory.getLogger(RpcClient::class.java)

    private val httpClient: HttpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofMillis(properties.timeout.toLong()))
        .build()

    private val cache: RpcCache? = if (properties.cacheEnabled) {
        RpcCache(properties.cacheTtlSeconds)
    } else null

    /**
     * Execute an RPC request with retry and caching.
     */
    fun execute(request: RpcRequest): RpcResponse {
        // Check cache first for cacheable methods
        if (isCacheable(request.method)) {
            cache?.get(request)?.let { cached ->
                logger.debug("Cache hit for {}", request.method)
                return cached
            }
        }

        var lastException: Exception? = null
        var attempt = 0
        var backoffMs = INITIAL_BACKOFF_MS

        while (attempt < properties.maxRetries) {
            try {
                val response = executeRequest(request)

                // Check for RPC-level errors that are retryable
                if (response.isError() && response.error?.isRetryable() == true) {
                    logger.warn("Retryable RPC error on attempt {}: {}", attempt + 1, response.error?.message)
                    lastException = response.error?.toSqlException()
                    attempt++
                    backoffMs = backoffWithJitter(backoffMs)
                    Thread.sleep(backoffMs)
                    continue
                }

                // Cache successful responses for cacheable methods
                if (response.isSuccess() && isCacheable(request.method)) {
                    cache?.put(request, response)
                }

                return response

            } catch (e: java.net.http.HttpTimeoutException) {
                logger.warn("Request timeout on attempt {}: {}", attempt + 1, e.message)
                lastException = SQLTransientConnectionException("Request timeout: ${e.message}")
                attempt++
                backoffMs = backoffWithJitter(backoffMs)
                Thread.sleep(backoffMs)

            } catch (e: java.io.IOException) {
                logger.warn("IO error on attempt {}: {}", attempt + 1, e.message)
                lastException = SQLNonTransientConnectionException("Connection error: ${e.message}")
                attempt++
                backoffMs = backoffWithJitter(backoffMs)
                Thread.sleep(backoffMs)

            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                throw SQLException("Request interrupted", e)
            }
        }

        throw lastException ?: SQLException("Max retries exceeded for ${request.method}")
    }

    private fun executeRequest(request: RpcRequest): RpcResponse {
        val uri = URI.create(rpcUrl)
        val requestBuilder = HttpRequest.newBuilder()
            .uri(uri)
            .timeout(Duration.ofMillis(properties.readTimeout.toLong()))
            .header("Content-Type", "application/json")

        // Add custom headers if configured
        properties.rpcHeaders.forEach { (key, value) ->
            requestBuilder.header(key, value)
        }

        // Add API key header if configured
        properties.apiKey?.let { key ->
            requestBuilder.header("Authorization", "Bearer $key")
        }

        val httpRequest = requestBuilder
            .POST(HttpRequest.BodyPublishers.ofString(request.toJson()))
            .build()

        logger.debug("Sending RPC request: {} to {}", request.method, maskUrl(rpcUrl))

        val httpResponse = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString())

        // Handle HTTP-level rate limiting
        if (httpResponse.statusCode() == 429) {
            val retryAfter = httpResponse.headers()
                .firstValue("Retry-After")
                .map { it.toLongOrNull() ?: 30 }
                .orElse(30L)

            logger.warn("Rate limited (HTTP 429). Retry after {} seconds", retryAfter)
            throw RateLimitException("Rate limited", retryAfter)
        }

        // Handle other HTTP errors
        if (httpResponse.statusCode() !in 200..299) {
            throw SQLException(
                "HTTP error ${httpResponse.statusCode()}: ${httpResponse.body().take(200)}"
            )
        }

        return RpcResponse.fromJson(httpResponse.body())
    }

    /**
     * Calculate backoff with jitter (±20%).
     */
    private fun backoffWithJitter(currentBackoffMs: Long): Long {
        val nextBackoff = min(currentBackoffMs * 2, MAX_BACKOFF_MS)
        val jitter = (nextBackoff * 0.2 * (Random.nextDouble() * 2 - 1)).toLong()
        return nextBackoff + jitter
    }

    /**
     * Check if a method's response can be cached.
     */
    private fun isCacheable(method: String): Boolean = method in CACHEABLE_METHODS

    /**
     * Mask sensitive parts of URL for logging.
     */
    private fun maskUrl(url: String): String {
        return url.replace(Regex("(api[_-]?key|key|token|secret)=([^&]+)", RegexOption.IGNORE_CASE)) {
            "${it.groupValues[1]}=***"
        }
    }

    override fun close() {
        cache?.clear()
    }

    companion object {
        private const val INITIAL_BACKOFF_MS = 1000L
        private const val MAX_BACKOFF_MS = 32000L

        // Methods whose responses can be cached (immutable data)
        private val CACHEABLE_METHODS = setOf(
            "eth_getBlockByNumber",
            "eth_getBlockByHash",
            "eth_getTransactionByHash",
            "eth_getTransactionReceipt",
            "eth_getLogs"
            // Note: eth_getBalance, eth_getCode, eth_getTransactionCount are NOT cached
            // because they can change per block
        )
    }
}

/**
 * Exception for rate limiting with retry-after hint.
 */
class RateLimitException(message: String, val retryAfterSeconds: Long) : SQLException(message)
