//! Shared HTTP transport: client construction, timeouts, and retries.

use std::{sync::Arc, time::Duration};

use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{Error, Result};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay_ms: 500,
            max_delay_ms: 10_000,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    pub base_url: String,
    pub timeout_secs: u64,
    pub user_agent: String,
    pub retry: RetryPolicy,
    pub max_concurrent_requests: usize,
}

impl Config {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: trim_base(base_url.into()),
            timeout_secs: 30,
            user_agent: "polyrover/0.1".into(),
            retry: RetryPolicy::default(),
            max_concurrent_requests: 8,
        }
    }
}

#[derive(Clone)]
pub struct Client {
    base_url: String,
    http: reqwest::Client,
    retry: RetryPolicy,
    concurrency: Arc<Semaphore>,
}

impl Client {
    pub fn new(config: Config) -> Result<Self> {
        if config.max_concurrent_requests == 0 {
            return Err(Error::Invalid(
                "max_concurrent_requests must be greater than zero".into(),
            ));
        }
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .user_agent(config.user_agent)
            .build()?;
        Ok(Self {
            base_url: config.base_url,
            http,
            retry: config.retry,
            concurrency: Arc::new(Semaphore::new(config.max_concurrent_requests)),
        })
    }

    pub fn with_base_url(&self, base_url: impl Into<String>) -> Self {
        Self {
            base_url: trim_base(base_url.into()),
            ..self.clone()
        }
    }

    async fn acquire_concurrency(&self) -> Result<OwnedSemaphorePermit> {
        self.concurrency
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Http("HTTP concurrency limiter closed".into()))
    }

    pub async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let body = self.execute(self.http.get(self.url(path)?), true).await?;
        Ok(serde_json::from_str(&body)?)
    }

    pub async fn post_json<B: Serialize, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T> {
        let body = self
            .execute(self.http.post(self.url(path)?).json(body), false)
            .await?;
        Ok(serde_json::from_str(&body)?)
    }

    pub async fn post_json_idempotent<B: Serialize, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T> {
        let body = self
            .execute(self.http.post(self.url(path)?).json(body), true)
            .await?;
        Ok(serde_json::from_str(&body)?)
    }

    pub async fn get_raw(&self, path: &str) -> Result<String> {
        self.execute(self.http.get(self.url(path)?), true).await
    }

    async fn execute(&self, request: reqwest::RequestBuilder, retryable: bool) -> Result<String> {
        let mut attempt = 0;
        loop {
            let request = request
                .try_clone()
                .ok_or_else(|| Error::Invalid("HTTP request body cannot be retried".into()))?;
            let permit = self.acquire_concurrency().await?;
            let response = request.send().await;
            drop(permit);
            let response = response?;
            let status = response.status().as_u16();
            let retry_after = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok());
            if retryable {
                if let Some(delay) = retry_delay(status, attempt, retry_after, &self.retry) {
                    attempt += 1;
                    tokio::time::sleep(delay).await;
                    continue;
                }
            }
            return checked_body(response).await;
        }
    }

    fn url(&self, path: &str) -> Result<String> {
        if !path.starts_with('/') {
            return Err(Error::Url(format!("path must start with /: {path}")));
        }
        Ok(format!("{}{}", self.base_url, path))
    }
}

async fn checked_body(response: reqwest::Response) -> Result<String> {
    let status = response.status();
    let retry_after_secs = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    let body = response.text().await?;
    if status.as_u16() == 429 {
        return Err(Error::RateLimited { retry_after_secs });
    }
    if !status.is_success() {
        return Err(Error::Api {
            status: status.as_u16(),
            body,
        });
    }
    Ok(body)
}

fn retry_delay(
    status: u16,
    attempt: u32,
    retry_after: Option<&str>,
    policy: &RetryPolicy,
) -> Option<Duration> {
    if !matches!(status, 425 | 429) || attempt >= policy.max_retries {
        return None;
    }
    let millis = retry_after
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value >= 0.0)
        .map(|seconds| (seconds * 1_000.0) as u64)
        .unwrap_or_else(|| {
            policy
                .base_delay_ms
                .saturating_mul(1_u64 << attempt.min(16))
        })
        .min(policy.max_delay_ms);
    Some(Duration::from_millis(millis))
}

fn trim_base(mut base: String) -> String {
    while base.ends_with('/') {
        base.pop();
    }
    base
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trims_base_url_slash() {
        assert_eq!(
            Config::new("https://example.test///").base_url,
            "https://example.test"
        );
    }

    #[test]
    fn retry_policy_handles_429_425_fractional_header_and_cap() {
        let policy = RetryPolicy {
            max_retries: 2,
            base_delay_ms: 100,
            max_delay_ms: 500,
        };
        assert_eq!(
            retry_delay(429, 0, Some("0.25"), &policy),
            Some(std::time::Duration::from_millis(250))
        );
        assert_eq!(
            retry_delay(425, 1, None, &policy),
            Some(std::time::Duration::from_millis(200))
        );
        assert_eq!(
            retry_delay(429, 0, Some("60"), &policy),
            Some(std::time::Duration::from_millis(500))
        );
        assert_eq!(retry_delay(500, 0, None, &policy), None);
        assert_eq!(retry_delay(429, 2, None, &policy), None);
    }

    #[tokio::test]
    async fn concurrency_limit_is_shared_across_retargeted_clients() {
        let client = Client::new(Config {
            max_concurrent_requests: 1,
            ..Config::new("https://gamma-api.polymarket.com")
        })
        .unwrap();
        let sibling = client.with_base_url("https://clob.polymarket.com");
        let permit = client.acquire_concurrency().await.unwrap();
        let blocked =
            tokio::time::timeout(Duration::from_millis(20), sibling.acquire_concurrency()).await;
        assert!(blocked.is_err());
        drop(permit);
    }

    #[tokio::test]
    async fn rate_limit_preserves_retry_after() {
        use std::{
            io::{Read, Write},
            net::TcpListener,
            thread,
        };
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0; 1024];
            let _ = stream.read(&mut request).unwrap();
            stream.write_all(
                b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 3\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            )
            .unwrap();
        });
        let client = Client::new(Config {
            retry: RetryPolicy {
                max_retries: 0,
                ..RetryPolicy::default()
            },
            ..Config::new(format!("http://{address}"))
        })
        .unwrap();
        assert!(matches!(
            client.get_raw("/limited").await,
            Err(Error::RateLimited {
                retry_after_secs: Some(3)
            })
        ));
        server.join().unwrap();
    }
}
