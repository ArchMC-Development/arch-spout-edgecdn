use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::{Duration, Instant};
use bytes::Bytes;
use sha2::{Sha256, Digest};
use std::path::Path;

// Cache entry with metadata
#[derive(Clone)]
struct CacheEntry {
    data: Bytes,
    etag: String,
    last_modified: String,
    expires: Instant,
    content_type: String,
    size: usize,
    version: Option<u32>,
}

// Cache statistics
#[derive(Default)]
struct CacheStats {
    hits: u64,
    misses: u64,
    bytes_served: u64,
    requests: u64,
}

// Main CDN cache structure
struct JarCdn {
    cache: Arc<RwLock<HashMap<String, CacheEntry>>>,
    stats: Arc<RwLock<CacheStats>>,
    max_cache_size: usize,
    ttl: Duration,
    upstream_url: String,
    metadata_url: String,
    download_url: String,
}

impl JarCdn {
    fn new(upstream_url: String, max_cache_size: usize, ttl_seconds: u64, metadata_url: String, download_url: String) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(CacheStats::default())),
            max_cache_size,
            ttl: Duration::from_secs(ttl_seconds),
            upstream_url,
            metadata_url,
            download_url,
        }
    }

    // Generate ETag from content
    fn generate_etag(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        format!("\"{}\"", hex::encode(&hasher.finalize()[..8]))
    }

    // Check current version from metadata endpoint
    async fn get_current_version(&self) -> Result<u32, Box<dyn std::error::Error + Send + Sync>> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;

        let response = client.get(&self.metadata_url).send().await?;
        if !response.status().is_success() {
            return Err(format!("Metadata endpoint returned {}", response.status()).into());
        }

        let version_text = response.text().await?;
        let version = version_text.trim().parse::<u32>()?;

        println!("Current version from metadata: {}", version);
        Ok(version)
    }

    // Check if we need to download based on version
    async fn needs_update(&self, path: &str) -> Result<(bool, Option<u32>), Box<dyn std::error::Error + Send + Sync>> {
        // For tropicspigot.jar, check version. For other files, use standard logic
        if path.contains("tropicspigot.jar") {
            let current_version = self.get_current_version().await?;

            let cache = self.cache.read().await;
            if let Some(entry) = cache.get(path) {
                if self.is_valid(entry) {
                    if let Some(cached_version) = entry.version {
                        if cached_version >= current_version {
                            println!("Version {} is up to date (cached: {})", current_version, cached_version);
                            return Ok((false, Some(current_version)));
                        } else {
                            println!("New version {} available (cached: {})", current_version, cached_version);
                            return Ok((true, Some(current_version)));
                        }
                    }
                }
            }

            println!("No cached version, need to download version {}", current_version);
            Ok((true, Some(current_version)))
        } else {
            // Standard cache logic for other files
            let cache = self.cache.read().await;
            if let Some(entry) = cache.get(path) {
                if self.is_valid(entry) {
                    Ok((false, None))
                } else {
                    Ok((true, None))
                }
            } else {
                Ok((true, None))
            }
        }
    }

    // Check if cache entry is still valid
    fn is_valid(&self, entry: &CacheEntry) -> bool {
        Instant::now() < entry.expires
    }

    // Fetch from upstream origin with streaming for large files
    async fn fetch_upstream(&self, path: &str) -> Result<Bytes, Box<dyn std::error::Error + Send + Sync>> {
        let url = if path.contains("tropicspigot.jar") {
            // Use the specific download URL for tropicspigot
            self.download_url.clone()
        } else {
            // Use standard upstream URL for other files
            format!("{}{}", self.upstream_url, path)
        };

        println!("Fetching from upstream: {}", url);

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(300)) // 5 minute timeout for large files
            .build()?;

        let response = client.get(&url).send().await?;
        if !response.status().is_success() {
            return Err(format!("Upstream returned {}", response.status()).into());
        }

        // Check content length for large file handling
        if let Some(content_length) = response.content_length() {
            if content_length > 100_000_000 { // 100MB
                println!("Warning: Large file detected ({} bytes)", content_length);
            }
        }

        let data = response.bytes().await?;
        println!("Downloaded {} bytes from upstream", data.len());
        Ok(data)
    }

    // Predownload and cache a file on startup
    async fn predownload(&self, path: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("🔄 Predownloading {}...", path);

        // Check if we need to download
        let (needs_update, current_version) = self.needs_update(path).await?;

        if !needs_update {
            let cache = self.cache.read().await;
            if let Some(entry) = cache.get(path) {
                println!("✅ {} already cached with version {:?} ({} bytes)",
                         path, entry.version, entry.size);
                return Ok(());
            }
        }

        // Download the file
        let data = self.fetch_upstream(path).await?;
        let etag = Self::generate_etag(&data);
        let now = Instant::now();

        let entry = CacheEntry {
            data: data.clone(),
            etag,
            last_modified: chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string(),
            expires: now + self.ttl,
            content_type: if path.ends_with(".jar") {
                "application/java-archive".to_string()
            } else {
                "application/octet-stream".to_string()
            },
            size: data.len(),
            version: current_version,
        };

        // Store in cache
        {
            let mut cache = self.cache.write().await;

            // Simple eviction: remove oldest entries if cache is full
            if cache.len() >= self.max_cache_size {
                // Find and remove the entry that expires first
                if let Some(oldest_key) = cache.iter()
                    .min_by_key(|(_, v)| v.expires)
                    .map(|(k, _)| k.clone()) {
                    cache.remove(&oldest_key);
                    println!("Evicted cache entry: {}", oldest_key);
                }
            }

            cache.insert(path.to_string(), entry.clone());
        }

        let version_info = if let Some(v) = current_version {
            format!(" (version {})", v)
        } else {
            String::new()
        };

        println!("✅ Predownloaded and cached {} ({} bytes{})", path, entry.size, version_info);
        Ok(())
    }

    // Get from cache or fetch from upstream with version checking
    async fn get(&self, path: &str) -> Result<CacheEntry, Box<dyn std::error::Error + Send + Sync>> {
        // Check if we need to update based on version or cache validity
        let (needs_update, current_version) = self.needs_update(path).await?;

        if !needs_update {
            // Cache hit - return existing entry
            let cache = self.cache.read().await;
            if let Some(entry) = cache.get(path) {
                let mut stats = self.stats.write().await;
                stats.hits += 1;
                stats.bytes_served += entry.size as u64;
                stats.requests += 1;
                println!("CACHE HIT: {}", path);
                return Ok(entry.clone());
            }
        }

        // Cache miss or version update needed - fetch from upstream
        let data = self.fetch_upstream(path).await?;
        let etag = Self::generate_etag(&data);
        let now = Instant::now();

        let entry = CacheEntry {
            data: data.clone(),
            etag,
            last_modified: chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string(),
            expires: now + self.ttl,
            content_type: if path.ends_with(".jar") {
                "application/java-archive".to_string()
            } else {
                "application/octet-stream".to_string()
            },
            size: data.len(),
            version: current_version,
        };

        // Store in cache
        {
            let mut cache = self.cache.write().await;

            // Simple eviction: remove oldest entries if cache is full
            if cache.len() >= self.max_cache_size {
                // Find and remove the entry that expires first
                if let Some(oldest_key) = cache.iter()
                    .min_by_key(|(_, v)| v.expires)
                    .map(|(k, _)| k.clone()) {
                    cache.remove(&oldest_key);
                    println!("Evicted cache entry: {}", oldest_key);
                }
            }

            cache.insert(path.to_string(), entry.clone());
        }

        // Update stats
        let mut stats = self.stats.write().await;
        stats.misses += 1;
        stats.bytes_served += entry.size as u64;
        stats.requests += 1;

        let version_info = if let Some(v) = current_version {
            format!(" (version {})", v)
        } else {
            String::new()
        };

        println!("CACHE MISS: {} (cached {} bytes{})", path, entry.size, version_info);
        Ok(entry)
    }

    // Get cache statistics
    async fn get_stats(&self) -> CacheStats {
        let stats = self.stats.read().await;
        CacheStats {
            hits: stats.hits,
            misses: stats.misses,
            bytes_served: stats.bytes_served,
            requests: stats.requests,
        }
    }

    // Clear cache
    async fn clear_cache(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();
        println!("Cache cleared");
    }
}

// HTTP response builder
struct HttpResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl HttpResponse {
    fn new(status: u16) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }

    fn body(mut self, data: Vec<u8>) -> Self {
        self.body = data;
        self
    }

    fn build(self) -> Vec<u8> {
        let status_line = match self.status {
            200 => "HTTP/1.1 200 OK\r\n",
            304 => "HTTP/1.1 304 Not Modified\r\n",
            404 => "HTTP/1.1 404 Not Found\r\n",
            500 => "HTTP/1.1 500 Internal Server Error\r\n",
            _ => "HTTP/1.1 500 Internal Server Error\r\n",
        };

        let mut response = status_line.as_bytes().to_vec();

        // Add headers
        for (name, value) in &self.headers {
            response.extend_from_slice(format!("{}: {}\r\n", name, value).as_bytes());
        }

        // Content-Length header
        response.extend_from_slice(format!("Content-Length: {}\r\n", self.body.len()).as_bytes());

        // End headers
        response.extend_from_slice(b"\r\n");

        // Add body
        response.extend_from_slice(&self.body);

        response
    }
}

// Parse HTTP request
fn parse_request(data: &[u8]) -> Option<(String, String, HashMap<String, String>)> {
    let request = String::from_utf8_lossy(data);
    let lines: Vec<&str> = request.lines().collect();

    if lines.is_empty() {
        return None;
    }

    let first_line: Vec<&str> = lines[0].split_whitespace().collect();
    if first_line.len() < 2 {
        return None;
    }

    let method = first_line[0].to_string();
    let path = first_line[1].to_string();

    let mut headers = HashMap::new();
    for line in lines.iter().skip(1) {
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(": ") {
            headers.insert(name.to_lowercase(), value.to_string());
        }
    }

    Some((method, path, headers))
}

// Handle individual client connection with optimized large file serving
async fn handle_connection(
    mut stream: tokio::net::TcpStream,
    cdn: Arc<JarCdn>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut buffer = vec![0; 16384]; // Increased buffer for large files
    let n = stream.read(&mut buffer).await?;

    if n == 0 {
        return Ok(());
    }

    let (method, path, headers) = match parse_request(&buffer[..n]) {
        Some(parsed) => parsed,
        None => {
            let response = HttpResponse::new(400)
                .header("Content-Type", "text/plain")
                .body(b"Bad Request".to_vec())
                .build();
            stream.write_all(&response).await?;
            return Ok(());
        }
    };

    // Get client IP for logging (similar to your X-Forwarded-For handling)
    let client_addr = stream.peer_addr().unwrap_or_else(|_| "unknown:0".parse().unwrap());
    println!("[CDN] {} is accessing {}", client_addr, path);

    // Handle different endpoints
    match (method.as_str(), path.as_str()) {
        ("GET", "/") => {
            let cache_size = {
                let cache = cdn.cache.read().await;
                cache.len()
            };
            let stats = cdn.get_stats().await;

            let status_json = format!(
                "{{\"assets\":{{\"status\":\"OPERATIONAL\",\"count\":\"{}\",\"cache_size\":\"{}\",\"hit_rate\":\"{:.2}%\"}}}}",
                cache_size,
                cache_size,
                if stats.requests > 0 {
                    (stats.hits as f64 / stats.requests as f64) * 100.0
                } else {
                    0.0
                }
            );

            let response = HttpResponse::new(200)
                .header("Content-Type", "application/json")
                .header("Cache-Control", "no-cache")
                .body(status_json.into_bytes())
                .build();
            stream.write_all(&response).await?;
        }

        ("GET", "/stats") => {
            let stats = cdn.get_stats().await;
            let cache_size = {
                let cache = cdn.cache.read().await;
                cache.len()
            };

            let stats_json = format!(
                "{{\"hits\":{},\"misses\":{},\"bytes_served\":{},\"requests\":{},\"hit_rate\":{:.2},\"cache_entries\":{},\"avg_response_size\":{:.0}}}",
                stats.hits,
                stats.misses,
                stats.bytes_served,
                stats.requests,
                if stats.requests > 0 {
                    (stats.hits as f64 / stats.requests as f64) * 100.0
                } else {
                    0.0
                },
                cache_size,
                if stats.requests > 0 {
                    stats.bytes_served as f64 / stats.requests as f64
                } else {
                    0.0
                }
            );

            let response = HttpResponse::new(200)
                .header("Content-Type", "application/json")
                .header("Cache-Control", "no-cache")
                .body(stats_json.into_bytes())
                .build();
            stream.write_all(&response).await?;
        }

        ("GET", "/version") => {
            match cdn.get_current_version().await {
                Ok(version) => {
                    let response = HttpResponse::new(200)
                        .header("Content-Type", "application/json")
                        .body(format!("{{\"current_version\":{}}}", version).into_bytes())
                        .build();
                    stream.write_all(&response).await?;
                }
                Err(e) => {
                    println!("[CDN] Error fetching version: {}", e);
                    let response = HttpResponse::new(500)
                        .header("Content-Type", "application/json")
                        .body(b"{\"error\":\"Failed to fetch current version\"}".to_vec())
                        .build();
                    stream.write_all(&response).await?;
                }
            }
        }

        ("POST", "/check-update") => {
            match cdn.needs_update("/tropicspigot.jar").await {
                Ok((needs_update, version)) => {
                    let response_json = format!(
                        "{{\"needs_update\":{},\"current_version\":{}}}",
                        needs_update,
                        version.unwrap_or(0)
                    );
                    let response = HttpResponse::new(200)
                        .header("Content-Type", "application/json")
                        .body(response_json.into_bytes())
                        .build();
                    stream.write_all(&response).await?;
                }
                Err(e) => {
                    println!("[CDN] Error checking update: {}", e);
                    let response = HttpResponse::new(500)
                        .header("Content-Type", "application/json")
                        .body(b"{\"error\":\"Failed to check for updates\"}".to_vec())
                        .build();
                    stream.write_all(&response).await?;
                }
            }
        }

        ("POST", "/clear-cache") => {
            cdn.clear_cache().await;
            let response = HttpResponse::new(200)
                .header("Content-Type", "text/plain")
                .body(b"Cache cleared".to_vec())
                .build();
            stream.write_all(&response).await?;
        }

        ("GET", path) if path.ends_with(".jar") || path.contains("/") => {
            let start_time = Instant::now();

            // Check if-none-match header for conditional requests
            let if_none_match = headers.get("if-none-match");

            match cdn.get(&path).await {
                Ok(entry) => {
                    let fetch_duration = start_time.elapsed();

                    // Handle conditional request
                    if let Some(client_etag) = if_none_match {
                        if client_etag == &entry.etag {
                            println!("[CDN] 304 Not Modified for {} ({}ms)", path, fetch_duration.as_millis());
                            let response = HttpResponse::new(304)
                                .header("ETag", &entry.etag)
                                .header("Cache-Control", "public, max-age=3600")
                                .header("X-Cache", "HIT")
                                .build();
                            stream.write_all(&response).await?;
                            return Ok(());
                        }
                    }

                    println!("[CDN] Delivering {} bytes to {} ({}ms)",
                             entry.size, client_addr, fetch_duration.as_millis());

                    let response = HttpResponse::new(200)
                        .header("Content-Type", &entry.content_type)
                        .header("ETag", &entry.etag)
                        .header("Last-Modified", &entry.last_modified)
                        .header("Cache-Control", "public, max-age=3600")
                        .header("X-Cache", "HIT")
                        .header("Accept-Ranges", "bytes")
                        .body(entry.data.to_vec())
                        .build();
                    stream.write_all(&response).await?;
                }
                Err(e) => {
                    println!("[CDN] Asset not found: {} - {}", path, e);
                    let response = HttpResponse::new(404)
                        .header("Content-Type", "application/json")
                        .body(b"{\"description\":\"Asset was not found internally.\"}".to_vec())
                        .build();
                    stream.write_all(&response).await?;
                }
            }
        }

        _ => {
            let response = HttpResponse::new(404)
                .header("Content-Type", "application/json")
                .body(b"{\"description\":\"Endpoint not found.\"}".to_vec())
                .build();
            stream.write_all(&response).await?;
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configuration optimized for TropicsSpigot JAR
    let bind_addr = "127.0.0.1:8080";
    let upstream_url = "https://repo1.maven.org/maven2"; // Fallback for other files
    let metadata_url = "https://spout.liftgate.io/metadata/tropicspigot/currentBuildVersion/".to_string();
    let download_url = "https://spout.liftgate.io/delivery/tropicspigot/tropicspigot.jar".to_string();
    let max_cache_size = 50; // Reduced for large files
    let ttl_seconds = 7200; // 2 hours TTL

    println!("🚀 Starting TropicsSpigot CDN Cache Server");
    println!("📍 Listening on: {}", bind_addr);
    println!("🔗 Metadata endpoint: {}", metadata_url);
    println!("📦 Download endpoint: {}", download_url);
    println!("💾 Max cache size: {} items", max_cache_size);
    println!("⏰ TTL: {} seconds ({} hours)", ttl_seconds, ttl_seconds / 3600);
    println!("🎯 Version-aware caching for TropicsSpigot JAR");

    // Create CDN instance
    let cdn = Arc::new(JarCdn::new(
        upstream_url.to_string(),
        max_cache_size,
        ttl_seconds,
        metadata_url,
        download_url,
    ));

    // Check initial version
    println!("🔍 Checking current TropicsSpigot version...");
    match cdn.get_current_version().await {
        Ok(version) => println!("✅ Current TropicsSpigot version: {}", version),
        Err(e) => println!("⚠️  Could not fetch initial version: {}", e),
    }

    // Predownload TropicsSpigot JAR
    println!("📥 Predownloading TropicsSpigot JAR...");
    match cdn.predownload("/tropicspigot.jar").await {
        Ok(()) => println!("✅ TropicsSpigot JAR predownloaded and cached successfully!"),
        Err(e) => {
            println!("❌ Failed to predownload TropicsSpigot JAR: {}", e);
            println!("⚠️  Server will still work, file will be downloaded on first request");
        }
    }

    // Bind to address
    let listener = TcpListener::bind(bind_addr).await?;
    println!("✅ Server ready! Endpoints:");
    println!("   GET  http://localhost:8080/                                      (Status)");
    println!("   GET  http://localhost:8080/tropicspigot.jar                      (TropicsSpigot JAR - version-aware)");
    println!("   GET  http://localhost:8080/version                              (Current version from metadata)");
    println!("   POST http://localhost:8080/check-update                         (Check if update needed)");
    println!("   GET  http://localhost:8080/stats                                (Cache statistics)");
    println!("   POST http://localhost:8080/clear-cache                          (Clear cache)");
    println!();
    println!("💡 Smart Caching:");
    println!("   - Predownloads TropicsSpigot JAR on startup");
    println!("   - Checks version before each download");
    println!("   - Only downloads when new version available");
    println!("   - Serves cached version at lightning speed");
    println!("   - Automatic version tracking and logging");

    // Accept connections
    loop {
        let (stream, addr) = listener.accept().await?;
        let cdn_clone = Arc::clone(&cdn);

        // Spawn task for each connection
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, cdn_clone).await {
                eprintln!("❌ Error handling connection from {}: {}", addr, e);
            }
        });
    }
}