"""
ChromaDB connection pooling and caching utilities.

This module provides connection pooling, query caching, and enhanced error handling
for ChromaDB operations in the Multi-Agent System.
"""

import time
import threading
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import hashlib
import json
from queue import Queue, Empty, Full

import chromadb
from chromadb.config import Settings
from chromadb.api.models.Collection import Collection

from .logging_config import get_logger
from .exceptions import ChromaDBConnectionError, ValidationError

logger = get_logger('chromadb_pool')


@dataclass
class CacheEntry:
    """Represents a cached query result with TTL."""
    result: Any
    created_at: datetime
    ttl_seconds: int = 300  # Default 5 minutes TTL
    access_count: int = 0
    
    def is_expired(self) -> bool:
        """Check if the cache entry has expired."""
        return datetime.now() > self.created_at + timedelta(seconds=self.ttl_seconds)
    
    def mark_access(self):
        """Mark this entry as accessed (for LRU tracking)."""
        self.access_count += 1


@dataclass
class ConnectionMetrics:
    """Metrics for ChromaDB connection performance."""
    total_queries: int = 0
    successful_queries: int = 0
    failed_queries: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    total_query_time: float = 0.0
    connection_failures: int = 0
    retry_attempts: int = 0
    
    def success_rate(self) -> float:
        """Calculate query success rate."""
        if self.total_queries == 0:
            return 0.0
        return self.successful_queries / self.total_queries
    
    def cache_hit_rate(self) -> float:
        """Calculate cache hit rate."""
        total_cache_requests = self.cache_hits + self.cache_misses
        if total_cache_requests == 0:
            return 0.0
        return self.cache_hits / total_cache_requests
    
    def average_query_time(self) -> float:
        """Calculate average query time."""
        if self.successful_queries == 0:
            return 0.0
        return self.total_query_time / self.successful_queries


class ChromaDBConnectionPool:
    """
    Connection pool manager for ChromaDB with caching and retry logic.
    
    Provides:
    - Connection pooling with configurable pool size
    - Query result caching with TTL
    - Automatic retry with exponential backoff
    - Performance metrics collection
    - Connection health monitoring
    """
    
    def __init__(self, 
                 host: str = 'localhost',
                 port: int = 8000,
                 pool_size: int = 5,
                 max_retries: int = 3,
                 retry_delay: float = 1.0,
                 connection_timeout: int = 30,
                 cache_ttl: int = 300,
                 max_cache_size: int = 1000):
        """
        Initialize ChromaDB connection pool.
        
        Args:
            host: ChromaDB host
            port: ChromaDB port
            pool_size: Maximum number of connections in pool
            max_retries: Maximum retry attempts for failed operations
            retry_delay: Initial retry delay in seconds (exponential backoff)
            connection_timeout: Connection timeout in seconds
            cache_ttl: Default cache TTL in seconds
            max_cache_size: Maximum number of cached entries
        """
        self.host = host
        self.port = port
        self.pool_size = pool_size
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.connection_timeout = connection_timeout
        self.cache_ttl = cache_ttl
        self.max_cache_size = max_cache_size
        
        # Connection pool
        self._connection_pool: Queue = Queue(maxsize=pool_size)
        self._pool_lock = threading.Lock()
        self._connections_created = 0
        
        # Query cache
        self._query_cache: Dict[str, CacheEntry] = {}
        self._cache_lock = threading.Lock()
        
        # Metrics
        self.metrics = ConnectionMetrics()
        self._metrics_lock = threading.Lock()
        
        # Initialize pool with connections
        self._initialize_pool()
        
        logger.info(f"ChromaDB connection pool initialized - Host: {host}:{port}, Pool size: {pool_size}")
    
    def _initialize_pool(self):
        """Initialize the connection pool with ChromaDB clients."""
        for i in range(self.pool_size):
            try:
                client = self._create_client()
                self._connection_pool.put_nowait(client)
                self._connections_created += 1
                logger.debug(f"Created connection {i+1}/{self.pool_size}")
            except Exception as e:
                logger.error(f"Failed to create initial connection {i+1}: {e}")
                with self._metrics_lock:
                    self.metrics.connection_failures += 1
    
    def _create_client(self) -> chromadb.HttpClient:
        """Create a new ChromaDB client with configured settings."""
        try:
            client = chromadb.HttpClient(
                host=self.host,
                port=self.port,
                settings=Settings(
                    anonymized_telemetry=False,
                    allow_reset=False
                )
            )
            
            # Test connection with timeout
            client.heartbeat()
            return client
            
        except Exception as e:
            raise ChromaDBConnectionError(f"Failed to create ChromaDB client: {e}")
    
    def _get_connection(self) -> chromadb.HttpClient:
        """Get a connection from the pool."""
        try:
            # Try to get connection from pool (non-blocking)
            client = self._connection_pool.get_nowait()
            
            # Test connection health
            try:
                client.heartbeat()
                return client
            except Exception as e:
                logger.warning(f"Connection health check failed, creating new one: {e}")
                # Create new connection if health check fails
                return self._create_client()
                
        except Empty:
            # Pool is empty, create new connection if under limit
            with self._pool_lock:
                if self._connections_created < self.pool_size * 2:  # Allow some overflow
                    try:
                        client = self._create_client()
                        self._connections_created += 1
                        return client
                    except Exception as e:
                        logger.error(f"Failed to create overflow connection: {e}")
                        raise ChromaDBConnectionError("Connection pool exhausted and cannot create new connection")
                else:
                    raise ChromaDBConnectionError("Connection pool exhausted")
    
    def _return_connection(self, client: chromadb.HttpClient):
        """Return a connection to the pool."""
        try:
            # Only return if pool has space
            self._connection_pool.put_nowait(client)
        except Full:
            # Pool is full, just discard the connection
            logger.debug("Connection pool full, discarding connection")
    
    def _generate_cache_key(self, query_text: str, n_results: int, collection_name: str) -> str:
        """Generate cache key for query parameters."""
        cache_data = {
            'query_text': query_text,
            'n_results': n_results,
            'collection_name': collection_name
        }
        cache_string = json.dumps(cache_data, sort_keys=True)
        return hashlib.sha256(cache_string.encode()).hexdigest()
    
    def _get_cached_result(self, cache_key: str) -> Optional[Any]:
        """Get cached result if available and not expired."""
        with self._cache_lock:
            if cache_key in self._query_cache:
                entry = self._query_cache[cache_key]
                
                if not entry.is_expired():
                    entry.mark_access()
                    with self._metrics_lock:
                        self.metrics.cache_hits += 1
                    logger.debug(f"Cache hit for key: {cache_key[:16]}...")
                    return entry.result
                else:
                    # Remove expired entry
                    del self._query_cache[cache_key]
                    logger.debug(f"Cache entry expired: {cache_key[:16]}...")
            
            with self._metrics_lock:
                self.metrics.cache_misses += 1
            return None
    
    def _cache_result(self, cache_key: str, result: Any, ttl: int = None):
        """Cache query result with TTL."""
        if ttl is None:
            ttl = self.cache_ttl
            
        with self._cache_lock:
            # Implement LRU eviction if cache is full
            if len(self._query_cache) >= self.max_cache_size:
                # Remove oldest entry (by creation time)
                oldest_key = min(self._query_cache.keys(), 
                               key=lambda k: self._query_cache[k].created_at)
                del self._query_cache[oldest_key]
                logger.debug(f"Evicted cache entry: {oldest_key[:16]}...")
            
            self._query_cache[cache_key] = CacheEntry(
                result=result,
                created_at=datetime.now(),
                ttl_seconds=ttl
            )
            logger.debug(f"Cached result for key: {cache_key[:16]}...")
    
    def _execute_with_retry(self, operation, *args, **kwargs):
        """Execute operation with exponential backoff retry."""
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                return operation(*args, **kwargs)
            except Exception as e:
                last_exception = e
                
                with self._metrics_lock:
                    self.metrics.retry_attempts += 1
                
                if attempt == self.max_retries:
                    logger.error(f"Operation failed after {self.max_retries} retries: {e}")
                    break
                    
                # Exponential backoff
                delay = self.retry_delay * (2 ** attempt)
                logger.warning(f"Operation failed (attempt {attempt + 1}), retrying in {delay:.2f}s: {e}")
                time.sleep(delay)
        
        # All retries failed
        with self._metrics_lock:
            self.metrics.failed_queries += 1
        raise ChromaDBConnectionError(f"Operation failed after {self.max_retries} retries: {last_exception}")
    
    def query_collection(self, 
                        collection_name: str,
                        query_text: str,
                        n_results: int = 5,
                        similarity_threshold: float = 0.0,
                        use_cache: bool = True,
                        cache_ttl: int = None) -> Dict[str, Any]:
        """
        Query ChromaDB collection with caching and retry logic.
        
        Args:
            collection_name: Name of the ChromaDB collection
            query_text: Query text for semantic search
            n_results: Maximum number of results to return
            similarity_threshold: Minimum similarity threshold
            use_cache: Whether to use query caching
            cache_ttl: Custom cache TTL (uses default if None)
            
        Returns:
            Dictionary containing query results
            
        Raises:
            ChromaDBConnectionError: If connection or query fails
            ValidationError: If parameters are invalid
        """
        # Validate inputs
        if not collection_name:
            raise ValidationError("collection_name cannot be empty")
        if not query_text:
            raise ValidationError("query_text cannot be empty")
        if n_results <= 0:
            raise ValidationError("n_results must be positive")
        if not (0.0 <= similarity_threshold <= 1.0):
            raise ValidationError("similarity_threshold must be between 0.0 and 1.0")
        
        with self._metrics_lock:
            self.metrics.total_queries += 1
        
        start_time = time.time()
        
        try:
            # Check cache first
            cache_key = None
            if use_cache:
                cache_key = self._generate_cache_key(query_text, n_results, collection_name)
                cached_result = self._get_cached_result(cache_key)
                if cached_result is not None:
                    return cached_result
            
            # Execute query with retry logic
            def _execute_query():
                client = self._get_connection()
                try:
                    # Get collection
                    collection = client.get_collection(name=collection_name)
                    
                    # Perform query
                    results = collection.query(
                        query_texts=[query_text],
                        n_results=n_results,
                        include=['documents', 'metadatas', 'distances']
                    )
                    
                    # Filter by similarity threshold if specified
                    if similarity_threshold > 0.0 and results['distances'] and results['distances'][0]:
                        filtered_results = {'documents': [[]], 'metadatas': [[]], 'distances': [[]]}
                        
                        documents = results['documents'][0] if results['documents'] else []
                        metadatas = results['metadatas'][0] if results['metadatas'] else []
                        distances = results['distances'][0] if results['distances'] else []
                        
                        for doc, metadata, distance in zip(documents, metadatas, distances):
                            similarity = 1.0 - distance
                            if similarity >= similarity_threshold:
                                filtered_results['documents'][0].append(doc)
                                filtered_results['metadatas'][0].append(metadata)
                                filtered_results['distances'][0].append(distance)
                        
                        results = filtered_results
                    
                    return results
                    
                finally:
                    self._return_connection(client)
            
            # Execute with retry
            result = self._execute_with_retry(_execute_query)
            
            # Cache result if enabled
            if use_cache and cache_key:
                self._cache_result(cache_key, result, cache_ttl)
            
            # Update metrics
            query_time = time.time() - start_time
            with self._metrics_lock:
                self.metrics.successful_queries += 1
                self.metrics.total_query_time += query_time
            
            logger.info(f"Query completed successfully in {query_time:.3f}s - Collection: {collection_name}")
            
            return result
            
        except Exception as e:
            query_time = time.time() - start_time
            with self._metrics_lock:
                self.metrics.failed_queries += 1
            
            logger.error(f"Query failed after {query_time:.3f}s - Collection: {collection_name}, Error: {e}")
            raise ChromaDBConnectionError(f"Query failed: {e}")
    
    def get_collection_info(self, collection_name: str) -> Dict[str, Any]:
        """
        Get information about a ChromaDB collection.
        
        Args:
            collection_name: Name of the collection
            
        Returns:
            Dictionary containing collection information
        """
        def _get_info():
            client = self._get_connection()
            try:
                collection = client.get_collection(name=collection_name)
                return {
                    'name': collection.name,
                    'count': collection.count(),
                    'metadata': collection.metadata
                }
            finally:
                self._return_connection(client)
        
        return self._execute_with_retry(_get_info)
    
    def health_check(self) -> Dict[str, Any]:
        """
        Check the health of the ChromaDB connection pool.
        
        Returns:
            Dictionary containing health status and metrics
        """
        try:
            # Test connection
            def _test_connection():
                client = self._get_connection()
                try:
                    client.heartbeat()
                    return True
                finally:
                    self._return_connection(client)
            
            connection_healthy = self._execute_with_retry(_test_connection)
            
            # Get pool status
            pool_size = self._connection_pool.qsize()
            
            return {
                'status': 'healthy' if connection_healthy else 'unhealthy',
                'host': f"{self.host}:{self.port}",
                'pool_size': pool_size,
                'connections_created': self._connections_created,
                'cache_size': len(self._query_cache),
                'metrics': {
                    'total_queries': self.metrics.total_queries,
                    'success_rate': self.metrics.success_rate(),
                    'cache_hit_rate': self.metrics.cache_hit_rate(),
                    'average_query_time': self.metrics.average_query_time(),
                    'connection_failures': self.metrics.connection_failures,
                    'retry_attempts': self.metrics.retry_attempts
                }
            }
            
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'host': f"{self.host}:{self.port}"
            }
    
    def clear_cache(self):
        """Clear all cached query results."""
        with self._cache_lock:
            cache_size = len(self._query_cache)
            self._query_cache.clear()
            logger.info(f"Cleared {cache_size} cached entries")
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get detailed performance metrics.
        
        Returns:
            Dictionary containing performance metrics
        """
        with self._metrics_lock:
            return {
                'queries': {
                    'total': self.metrics.total_queries,
                    'successful': self.metrics.successful_queries,
                    'failed': self.metrics.failed_queries,
                    'success_rate': self.metrics.success_rate()
                },
                'cache': {
                    'hits': self.metrics.cache_hits,
                    'misses': self.metrics.cache_misses,
                    'hit_rate': self.metrics.cache_hit_rate(),
                    'size': len(self._query_cache)
                },
                'performance': {
                    'total_query_time': self.metrics.total_query_time,
                    'average_query_time': self.metrics.average_query_time()
                },
                'connections': {
                    'pool_size': self._connection_pool.qsize(),
                    'created': self._connections_created,
                    'failures': self.metrics.connection_failures,
                    'retry_attempts': self.metrics.retry_attempts
                }
            }
    
    def close(self):
        """Close all connections in the pool."""
        logger.info("Closing ChromaDB connection pool")
        
        while not self._connection_pool.empty():
            try:
                client = self._connection_pool.get_nowait()
                # ChromaDB HttpClient doesn't have explicit close method
                # Connection will be closed when object is garbage collected
                del client
            except Empty:
                break
        
        self.clear_cache()
        logger.info("ChromaDB connection pool closed")