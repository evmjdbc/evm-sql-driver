package com.evmjdbc.rpc

import com.google.gson.Gson
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.JsonParser

/**
 * JSON-RPC 2.0 response from Ethereum RPC calls.
 */
data class RpcResponse(
    val jsonrpc: String,
    val id: Long?,
    val result: JsonElement?,
    val error: RpcError?
) {
    companion object {
        @PublishedApi
        internal val gson = Gson()

        /**
         * Parse a JSON-RPC response from raw JSON string.
         */
        fun fromJson(json: String): RpcResponse {
            val obj = JsonParser.parseString(json).asJsonObject
            return RpcResponse(
                jsonrpc = obj.get("jsonrpc")?.asString ?: "2.0",
                id = obj.get("id")?.asLong,
                result = obj.get("result"),
                error = obj.get("error")?.let { RpcError.fromJson(it.asJsonObject) }
            )
        }
    }

    /**
     * Check if the response contains an error.
     */
    fun isError(): Boolean = error != null

    /**
     * Check if the response was successful.
     */
    fun isSuccess(): Boolean = error == null && result != null

    /**
     * Get the result as a string (for simple hex values).
     */
    fun resultAsString(): String? = result?.asString

    /**
     * Get the result as a JsonObject.
     */
    fun resultAsObject(): JsonObject? = result?.takeIf { it.isJsonObject }?.asJsonObject

    /**
     * Get the result as a list of JsonObjects.
     */
    fun resultAsList(): List<JsonObject> =
        result?.takeIf { it.isJsonArray }?.asJsonArray?.map { it.asJsonObject } ?: emptyList()

    /**
     * Get the result parsed as a specific type.
     */
    inline fun <reified T> resultAs(): T? = result?.let { gson.fromJson(it, T::class.java) }
}
