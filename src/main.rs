use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::fs;
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
    is_local_fallback: bool, // New field to track if this is from local file
}

// Cache statistics
#[derive(Default)]
struct CacheStats {
    hits: u64,
    misses: u64,
    bytes_served: u64,
    requests: u64,
    fallback_serves: u64,
    local_fallback_serves: u64, // New field for local file serves
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
    local_jar_path: String, // Path to local fallback jar
}

impl JarCdn {
    fn new(upstream_url: String, max_cache_size: usize, ttl_seconds: u64, metadata_url: String, download_url: String, local_jar_path: String) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(CacheStats::default())),
            max_cache_size,
            ttl: Duration::from_secs(ttl_seconds),
            upstream_url,
            metadata_url,
            download_url,
            local_jar_path,
        }
    }

    // Generate ETag from content
    fn generate_etag(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        format!("\"{}\"", hex::encode(&hasher.finalize()[..8]))
    }

    // Load local jar file into cache
    async fn load_local_jar(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("📁 Loading local fallback jar from: {}", self.local_jar_path);

        let data = fs::read(&self.local_jar_path).await?;
        let etag = Self::generate_etag(&data);
        let now = Instant::now();
        let size = data.len();

        let entry = CacheEntry {
            data: Bytes::from(data),
            etag,
            last_modified: chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string(),
            expires: now + Duration::from_secs(u64::MAX), // Never expires for local fallback
            content_type: "application/java-archive".to_string(),
            size,
            version: None, // We don't know the version of the local file
            is_local_fallback: true,
        };

        let mut cache = self.cache.write().await;
        cache.insert("/tropicspigot.jar".to_string(), entry.clone());

        println!("✅ Local fallback jar loaded ({} bytes)", entry.size);
        Ok(())
    }

    // Check current version from metadata endpoint (non-blocking)
    async fn get_current_version(&self) -> Result<u32, Box<dyn std::error::Error + Send + Sync>> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10)) // Shorter timeout for background checks
            .build()?;

        let response = client.get(&self.metadata_url).send().await?;
        if !response.status().is_success() {
            return Err(format!("Metadata endpoint returned {}", response.status()).into());
        }

        let version_text = response.text().await?;
        let version = version_text.trim().parse::<u32>()?;

        println!("📊 Current version from metadata: {}", version);
        Ok(version)
    }

    // Background version check and update
    async fn background_version_check(&self) {
        println!("🔄 Starting background version check...");

        match self.get_current_version().await {
            Ok(current_version) => {
                let cache = self.cache.read().await;
                if let Some(entry) = cache.get("/tropicspigot.jar") {
                    if !entry.is_local_fallback {
                        if let Some(cached_version) = entry.version {
                            if cached_version >= current_version {
                                println!("✅ Version {} is up to date (cached: {})", current_version, cached_version);
                                return;
                            } else {
                                println!("🆕 New version {} available (cached: {})", current_version, cached_version);
                            }
                        }
                    } else {
                        println!("🔄 Local fallback detected, attempting to download version {}", current_version);
                    }
                } else {
                    println!("📥 No cached version, downloading version {}", current_version);
                }
                drop(cache);

                // Attempt to download new version
                match self.fetch_upstream_silent("/tropicspigot.jar").await {
                    Ok(data) => {
                        let etag = Self::generate_etag(&data);
                        let now = Instant::now();

                        let entry = CacheEntry {
                            data: data.clone(),
                            etag,
                            last_modified: chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string(),
                            expires: now + self.ttl,
                            content_type: "application/java-archive".to_string(),
                            size: data.len(),
                            version: Some(current_version),
                            is_local_fallback: false,
                        };

                        let mut cache = self.cache.write().await;
                        cache.insert("/tropicspigot.jar".to_string(), entry.clone());
                        println!("✅ Background update completed: version {} ({} bytes)", current_version, entry.size);
                    }
                    Err(e) => {
                        println!("⚠️  Background download failed: {}", e);
                    }
                }
            }
            Err(e) => {
                println!("⚠️  Background version check failed: {}", e);
            }
        }
    }

    // Check if cache entry is still valid
    fn is_valid(&self, entry: &CacheEntry) -> bool {
        entry.is_local_fallback || Instant::now() < entry.expires
    }

    // Fetch from upstream origin silently (for background updates)
    async fn fetch_upstream_silent(&self, path: &str) -> Result<Bytes, Box<dyn std::error::Error + Send + Sync>> {
        let url = if path.contains("tropicspigot.jar") {
            self.download_url.clone()
        } else {
            format!("{}{}", self.upstream_url, path)
        };

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(120)) // Shorter timeout for background
            .build()?;

        let response = client.get(&url).send().await?;

        if response.status().as_u16() == 522 {
            return Err("Upstream returned 522 (Connection timed out)".into());
        }

        if !response.status().is_success() {
            return Err(format!("Upstream returned {}", response.status()).into());
        }

        let data = response.bytes().await?;
        Ok(data)
    }

    // Fast fetch with immediate 522 fallback (no waiting)
    async fn fetch_upstream_fast(&self, path: &str) -> Result<Bytes, Box<dyn std::error::Error + Send + Sync>> {
        let url = if path.contains("tropicspigot.jar") {
            self.download_url.clone()
        } else {
            format!("{}{}", self.upstream_url, path)
        };

        println!("⚡ Fast fetch from upstream: {}", url);

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5)) // Very short timeout - fail fast
            .build()?;

        let response = client.get(&url).send().await?;

        // Immediately check for 522 or any error
        if response.status().as_u16() == 522 {
            return Err("522".into()); // Special error code for 522
        }

        if !response.status().is_success() {
            return Err(format!("HTTP {}", response.status().as_u16()).into());
        }

        let data = response.bytes().await?;
        println!("✅ Fast fetch completed: {} bytes", data.len());
        Ok(data)
    }

    // Get from cache or fetch with immediate fallback
    async fn get(&self, path: &str) -> Result<CacheEntry, Box<dyn std::error::Error + Send + Sync>> {
        // First check cache
        {
            let cache = self.cache.read().await;
            if let Some(entry) = cache.get(path) {
                if self.is_valid(entry) && !entry.is_local_fallback {
                    // Valid non-fallback cache hit
                    let mut stats = self.stats.write().await;
                    stats.hits += 1;
                    stats.bytes_served += entry.size as u64;
                    stats.requests += 1;
                    println!("⚡ CACHE HIT: {}", path);
                    return Ok(entry.clone());
                }
            }
        }

        // For tropicspigot.jar, try fast fetch with immediate 522 fallback
        if path.contains("tropicspigot.jar") {
            match self.fetch_upstream_fast(path).await {
                Ok(data) => {
                    // Successful fast fetch
                    let etag = Self::generate_etag(&data);
                    let now = Instant::now();

                    let entry = CacheEntry {
                        data: data.clone(),
                        etag,
                        last_modified: chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string(),
                        expires: now + self.ttl,
                        content_type: "application/java-archive".to_string(),
                        size: data.len(),
                        version: None, // We'll get this in background check
                        is_local_fallback: false,
                    };

                    // Store in cache
                    {
                        let mut cache = self.cache.write().await;
                        cache.insert(path.to_string(), entry.clone());
                    }

                    let mut stats = self.stats.write().await;
                    stats.misses += 1;
                    stats.bytes_served += entry.size as u64;
                    stats.requests += 1;

                    println!("⚡ FRESH DOWNLOAD: {} ({} bytes)", path, entry.size);
                    Ok(entry)
                }
                Err(e) => {
                    // Fast fetch failed - immediately serve local fallback
                    println!("⚠️  Fast fetch failed ({}), serving local fallback immediately", e);

                    let cache = self.cache.read().await;
                    if let Some(entry) = cache.get(path) {
                        if entry.is_local_fallback {
                            let mut stats = self.stats.write().await;
                            stats.local_fallback_serves += 1;
                            stats.bytes_served += entry.size as u64;
                            stats.requests += 1;

                            println!("🔄 SERVING LOCAL FALLBACK: {} ({} bytes)", path, entry.size);
                            return Ok(entry.clone());
                        }
                    }

                    return Err("No local fallback available".into());
                }
            }
        } else {
            // For other files, use normal logic
            return Err("File not found".into());
        }
    }

    // Get cache statistics
    async fn get_stats(&self) -> CacheStats {
        let stats = self.stats.read().await;
        CacheStats {
            hits: stats.hits,
            misses: stats.misses,
            bytes_served: stats.bytes_served,
            requests: stats.requests,
            fallback_serves: stats.fallback_serves,
            local_fallback_serves: stats.local_fallback_serves,
        }
    }

    // Clear cache but keep local fallback
    async fn clear_cache(&self) {
        let mut cache = self.cache.write().await;

        // Keep local fallback entry
        let local_entry = cache.get("/tropicspigot.jar").filter(|e| e.is_local_fallback).cloned();
        cache.clear();

        if let Some(entry) = local_entry {
            cache.insert("/tropicspigot.jar".to_string(), entry);
            println!("Cache cleared (kept local fallback)");
        } else {
            println!("Cache cleared");
        }
    }

    // Start background version checking task
    async fn start_background_tasks(self: Arc<Self>) {
        let cdn = Arc::clone(&self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(300)); // Check every 5 minutes

            loop {
                interval.tick().await;
                cdn.background_version_check().await;
            }
        });
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

// Handle individual client connection
async fn handle_connection(
    mut stream: tokio::net::TcpStream,
    cdn: Arc<JarCdn>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut buffer = vec![0; 16384];
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
                "{{\"assets\":{{\"status\":\"OPERATIONAL\",\"count\":\"{}\",\"cache_size\":\"{}\",\"hit_rate\":\"{:.2}%\",\"local_fallback_serves\":\"{}\"}}}}",
                cache_size,
                cache_size,
                if stats.requests > 0 {
                    (stats.hits as f64 / stats.requests as f64) * 100.0
                } else {
                    0.0
                },
                stats.local_fallback_serves
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
                "{{\"hits\":{},\"misses\":{},\"bytes_served\":{},\"requests\":{},\"hit_rate\":{:.2},\"cache_entries\":{},\"avg_response_size\":{:.0},\"local_fallback_serves\":{}}}",
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
                },
                stats.local_fallback_serves
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
                                .header("X-Cache", if entry.is_local_fallback { "LOCAL-FALLBACK" } else { "HIT" })
                                .build();
                            stream.write_all(&response).await?;
                            return Ok(());
                        }
                    }

                    println!("[CDN] Delivering {} bytes to {} ({}ms)",
                             entry.size, client_addr, fetch_duration.as_millis());

                    let cache_status = if entry.is_local_fallback { "LOCAL-FALLBACK" } else { "HIT" };

                    let response = HttpResponse::new(200)
                        .header("Content-Type", &entry.content_type)
                        .header("ETag", &entry.etag)
                        .header("Last-Modified", &entry.last_modified)
                        .header("Cache-Control", "public, max-age=3600")
                        .header("X-Cache", cache_status)
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
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Configuration
    let bind_addr = "172.16.1.12:1438";
    let upstream_url = "https://repo1.maven.org/maven2";
    let metadata_url = "https://spout.liftgate.io/metadata/tropicspigot/currentBuildVersion/".to_string();
    let download_url = "https://spout.liftgate.io/delivery/tropicspigot/tropicspigot.jar".to_string();
    let local_jar_path = "./tropicspigot.jar".to_string(); // Local file path
    let max_cache_size = 50;
    let ttl_seconds = 7200;

    println!("🚀 Starting TropicsSpigot CDN Cache Server with Local Fallback");
    println!("📍 Listening on: {}", bind_addr);
    println!("🔗 Metadata endpoint: {}", metadata_url);
    println!("📦 Download endpoint: {}", download_url);
    println!("📁 Local fallback file: {}", local_jar_path);
    println!("💾 Max cache size: {} items", max_cache_size);
    println!("⏰ TTL: {} seconds ({} hours)", ttl_seconds, ttl_seconds / 3600);
    println!("⚡ Fast 522 fallback: Instant response with local file");

    // Create CDN instance
    let cdn = Arc::new(JarCdn::new(
        upstream_url.to_string(),
        max_cache_size,
        ttl_seconds,
        metadata_url,
        download_url,
        local_jar_path,
    ));

    // Load local jar file
    println!("📁 Loading local fallback jar...");
    match cdn.load_local_jar().await {
        Ok(()) => println!("✅ Local fallback jar loaded successfully!"),
        Err(e) => {
            println!("❌ Failed to load local fallback jar: {}", e);
            println!("⚠️  Make sure 'tropicspigot.jar' exists in the current directory");
            return Err(e);
        }
    }

    // Start background tasks
    println!("🔄 Starting background version checking (every 5 minutes)...");
    let cdn_clone = Arc::clone(&cdn);
    cdn_clone.start_background_tasks().await;

    // Bind to address
    let listener = TcpListener::bind(bind_addr).await?;
    println!("✅ Server ready! Endpoints:");
    println!("   GET  http://localhost:8080/                                      (Status)");
    println!("   GET  http://localhost:1438/tropicspigot.jar                      (TropicsSpigot JAR - instant 522 fallback)");
    println!("   GET  http://localhost:8080/version                              (Current version from metadata)");
    println!("   GET  http://localhost:8080/stats                                (Cache statistics)");
    println!("   POST http://localhost:8080/clear-cache                          (Clear cache)");
    println!();
    println!("⚡ Ultra-Fast Serving:");
    println!("   - Local fallback file loaded on startup");
    println!("   - 5-second timeout for upstream requests");
    println!("   - Instant 522 fallback (no waiting!)");
    println!("   - Background version checks every 5 minutes");
    println!("   - Automatic cache updates when new versions available");

    // Accept connections
    loop {
        let (stream, addr) = listener.accept().await?;
        let cdn_clone = Arc::clone(&cdn);

        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, cdn_clone).await {
                eprintln!("❌ Error handling connection from {}: {}", addr, e);
            }
        });
    }
}