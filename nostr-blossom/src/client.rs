//! Implements a Blossom client for interacting with Blossom servers

use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bitcoin_hashes::sha256::Hash as Sha256Hash;
use nostr::prelude::*;
use nostr::types::Url;
use reqwest::header::{
    AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, HeaderMap, HeaderValue, LOCATION, RANGE,
};
#[cfg(not(target_arch = "wasm32"))]
use reqwest::redirect::Policy;
use reqwest::{Response, StatusCode};
use serde::Serialize;

use crate::bud01::{BlossomAuthorization, BlossomAuthorizationScope, BlossomAuthorizationVerb};
use crate::bud02::BlobDescriptor;
use crate::error::{Error, ErrorKind};

/// A client for interacting with a Blossom server
///
/// <https://github.com/hzrd149/blossom>
#[derive(Debug, Clone)]
pub struct BlossomClient {
    base_url: Url,
    client: reqwest::Client,
}

impl BlossomClient {
    /// Creates a new `BlossomClient` with the given base URL.
    pub fn new(mut base_url: Url) -> Self {
        base_url.set_path("/");
        base_url.set_query(None);
        base_url.set_fragment(None);
        Self {
            base_url,
            client: Self::build_client().unwrap(),
        }
    }

    /// Builds the reqwest client
    fn build_client() -> reqwest::Result<reqwest::Client> {
        let builder = reqwest::Client::builder();
        #[cfg(not(target_arch = "wasm32"))]
        let builder = builder.redirect(Policy::none());
        builder.build()
    }

    /// Uploads a blob to the Blossom server.
    ///
    /// <https://github.com/hzrd149/blossom/blob/master/buds/02.md>
    pub async fn upload_blob<T>(
        &self,
        data: Vec<u8>,
        content_type: Option<String>,
        authorization_options: Option<BlossomAuthorizationOptions>,
        signer: Option<&T>,
    ) -> Result<BlobDescriptor, Error>
    where
        T: AsyncGetPublicKey + AsyncSignEvent,
    {
        let url: Url = self.base_url.join("upload")?;

        let hash: Sha256Hash = Sha256Hash::hash(&data);
        let file_hashes: Vec<Sha256Hash> = vec![hash];

        let data_len = data.len();
        let mut request = self.client.put(url).body(data);
        let mut headers = HeaderMap::new();

        headers.insert(CONTENT_LENGTH, HeaderValue::from(data_len));
        headers.insert("X-SHA-256", HeaderValue::from_str(&hash.to_string())?);

        if let Some(ct) = content_type {
            headers.insert(CONTENT_TYPE, HeaderValue::from_str(&ct)?);
        }

        if let Some(signer) = signer {
            let default_auth = self.default_auth(
                BlossomAuthorizationVerb::Upload,
                "Blossom upload authorization",
                BlossomAuthorizationScope::BlobSha256Hashes(file_hashes),
            );
            let final_auth = authorization_options
                .map(|opts| Self::update_authorization_fixture(&default_auth, opts))
                .unwrap_or(default_auth);
            let auth_header = Self::build_auth_header(signer, final_auth).await?;
            headers.insert(AUTHORIZATION, auth_header);
        }

        request = request.headers(headers);

        let response: Response = request.send().await?;

        match response.status() {
            StatusCode::OK | StatusCode::CREATED => {
                let descriptor: BlobDescriptor = response.json().await?;
                Ok(descriptor)
            }
            _ => Err(Error::response("Failed to upload blob", response)),
        }
    }

    /// Checks whether the server would accept a blob upload without sending its body.
    ///
    /// This implements the optional BUD-06 `HEAD /upload` preflight endpoint.
    pub async fn upload_requirements<T>(
        &self,
        sha256: Sha256Hash,
        size: u64,
        content_type: Option<&str>,
        authorization_options: Option<BlossomAuthorizationOptions>,
        signer: Option<&T>,
    ) -> Result<BlossomPreflight, Error>
    where
        T: AsyncGetPublicKey + AsyncSignEvent,
    {
        self.preflight(
            "upload",
            sha256,
            size,
            content_type,
            BlossomAuthorizationVerb::Upload,
            authorization_options,
            signer,
        )
        .await
    }

    /// Lists blobs uploaded by a specific pubkey.
    ///
    /// <https://github.com/hzrd149/blossom/blob/master/buds/02.md>
    pub async fn list_blobs<T>(
        &self,
        pubkey: &PublicKey,
        since: Option<Timestamp>,
        until: Option<Timestamp>,
        authorization_options: Option<BlossomAuthorizationOptions>,
        signer: Option<&T>,
    ) -> Result<Vec<BlobDescriptor>, Error>
    where
        T: AsyncGetPublicKey + AsyncSignEvent,
    {
        let mut url: Url = self.base_url.join(&format!("list/{}", pubkey.to_hex()))?;

        if let Some(since) = since {
            url.query_pairs_mut()
                .append_pair("since", since.to_string().as_str());
        }

        if let Some(until) = until {
            url.query_pairs_mut()
                .append_pair("until", until.to_string().as_str());
        }

        let mut request = self.client.get(url);
        let mut headers = HeaderMap::new();

        if let Some(signer) = signer {
            let default_auth = self.default_auth(
                BlossomAuthorizationVerb::List,
                "Blossom list authorization",
                BlossomAuthorizationScope::ServerUrl(self.base_url.clone()),
            );
            let final_auth = authorization_options
                .map(|opts| Self::update_authorization_fixture(&default_auth, opts))
                .unwrap_or(default_auth);
            let auth_header = Self::build_auth_header(signer, final_auth).await?;
            headers.insert(AUTHORIZATION, auth_header);
        }

        request = request.headers(headers);

        let response: Response = request.send().await?;

        match response.status() {
            StatusCode::OK => {
                let descriptors: Vec<BlobDescriptor> = response.json().await?;
                Ok(descriptors)
            }
            _ => Err(Error::response("Failed to list blobs", response)),
        }
    }

    /// Retrieves a blob from the Blossom server, with optional authorization.
    ///
    /// <https://github.com/hzrd149/blossom/blob/master/buds/01.md>
    pub async fn get_blob<T>(
        &self,
        sha256: Sha256Hash,
        range: Option<String>,
        authorization_options: Option<BlossomAuthorizationOptions>,
        signer: Option<&T>,
    ) -> Result<Vec<u8>, Error>
    where
        T: AsyncGetPublicKey + AsyncSignEvent,
    {
        let url: Url = self.base_url.join(sha256.to_string().as_str())?;

        let mut request = self.client.get(url);
        let mut headers = HeaderMap::new();

        let verify_hash = range.is_none();
        if let Some(range_value) = range {
            headers.insert(RANGE, HeaderValue::from_str(&range_value)?);
        }

        if let Some(signer) = signer {
            let default_auth = self.default_auth(
                BlossomAuthorizationVerb::Get,
                "Blossom get authorization",
                BlossomAuthorizationScope::BlobSha256Hashes(vec![sha256]),
            );
            let final_auth = authorization_options
                .map(|opts| Self::update_authorization_fixture(&default_auth, opts))
                .unwrap_or(default_auth);
            let auth_header = Self::build_auth_header(signer, final_auth).await?;
            headers.insert(AUTHORIZATION, auth_header);
        }

        request = request.headers(headers.clone());

        let mut response: Response = request.send().await?;

        #[cfg(not(target_arch = "wasm32"))]
        for _ in 0..10 {
            if !response.status().is_redirection() {
                break;
            }

            let location = response.headers().get(LOCATION).ok_or_else(|| {
                Error::with_static_message(
                    ErrorKind::Invalid,
                    "Redirect response missing 'Location' header",
                )
            })?;
            let next_url = response.url().join(location.to_str()?)?;
            if !next_url.as_str().contains(&sha256.to_string()) {
                return Err(Error::with_static_message(
                    ErrorKind::Invalid,
                    "Redirect URL does not contain SHA256",
                ));
            }

            let same_origin = next_url.origin() == response.url().origin();
            let mut next_headers = headers.clone();
            if !same_origin {
                next_headers.remove(AUTHORIZATION);
            }
            response = self
                .client
                .get(next_url)
                .headers(next_headers)
                .send()
                .await?;
        }

        if response.status().is_redirection() {
            return Err(Error::with_static_message(
                ErrorKind::Invalid,
                "Too many blob redirects",
            ));
        }

        match response.status() {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let data = response.bytes().await?.to_vec();
                if verify_hash && Sha256Hash::hash(&data) != sha256 {
                    return Err(Error::with_static_message(
                        ErrorKind::Invalid,
                        "Downloaded blob does not match requested SHA256",
                    ));
                }
                Ok(data)
            }
            _ => Err(Error::response("Failed to get blob", response)),
        }
    }

    /// Checks if a blob exists on the Blossom server.
    ///
    /// <https://github.com/hzrd149/blossom/blob/master/buds/01.md>
    pub async fn has_blob<T>(
        &self,
        sha256: Sha256Hash,
        authorization_options: Option<BlossomAuthorizationOptions>,
        signer: Option<&T>,
    ) -> Result<bool, Error>
    where
        T: AsyncGetPublicKey + AsyncSignEvent,
    {
        let url: Url = self.base_url.join(sha256.to_string().as_str())?;

        let mut request = self.client.head(url);

        if let Some(signer) = signer {
            let default_auth = self.default_auth(
                BlossomAuthorizationVerb::Get,
                "Blossom get authorization",
                BlossomAuthorizationScope::BlobSha256Hashes(vec![sha256]),
            );

            let final_auth = authorization_options
                .map(|opts| Self::update_authorization_fixture(&default_auth, opts))
                .unwrap_or(default_auth);

            let mut headers = HeaderMap::new();
            let auth_header = Self::build_auth_header(signer, final_auth).await?;
            headers.insert(AUTHORIZATION, auth_header);

            request = request.headers(headers);
        }

        let response: Response = request.send().await?;

        match response.status() {
            StatusCode::OK => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            _ => Err(Error::response("Unexpected HTTP status code", response)),
        }
    }

    /// Deletes a blob from the Blossom server.
    ///
    /// <https://github.com/hzrd149/blossom/blob/master/buds/02.md>
    pub async fn delete_blob<T>(
        &self,
        sha256: Sha256Hash,
        authorization_options: Option<BlossomAuthorizationOptions>,
        signer: &T,
    ) -> Result<(), Error>
    where
        T: AsyncGetPublicKey + AsyncSignEvent,
    {
        let url: Url = self.base_url.join(sha256.to_string().as_str())?;

        let mut headers = HeaderMap::new();
        let default_auth = self.default_auth(
            BlossomAuthorizationVerb::Delete,
            "Blossom delete authorization",
            BlossomAuthorizationScope::BlobSha256Hashes(vec![sha256]),
        );

        let final_auth = authorization_options
            .map(|opts| Self::update_authorization_fixture(&default_auth, opts))
            .unwrap_or(default_auth);

        let auth_header = Self::build_auth_header(signer, final_auth).await?;
        headers.insert(AUTHORIZATION, auth_header);

        let response: Response = self.client.delete(url).headers(headers).send().await?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(Error::response("Failed to delete blob", response))
        }
    }

    /// Mirrors an existing blob from its public URL.
    ///
    /// This implements BUD-04. The authorization uses the `upload` verb and the
    /// mirrored blob's hash as required by BUD-11.
    pub async fn mirror_blob<T>(
        &self,
        blob: &BlobDescriptor,
        authorization_options: Option<BlossomAuthorizationOptions>,
        signer: Option<&T>,
        payment: Option<&BlossomPaymentProof>,
    ) -> Result<BlobDescriptor, Error>
    where
        T: AsyncGetPublicKey + AsyncSignEvent,
    {
        #[derive(Serialize)]
        struct MirrorRequest<'a> {
            url: &'a Url,
        }

        let url = self.base_url.join("mirror")?;
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(
            "X-SHA-256",
            HeaderValue::from_str(&blob.sha256.to_string())?,
        );
        headers.insert("X-Content-Length", HeaderValue::from(blob.size));
        headers.insert("X-Content-Type", HeaderValue::from_str(&blob.mime_type)?);
        Self::add_payment_header(&mut headers, payment)?;
        self.add_authorization_header(
            &mut headers,
            BlossomAuthorizationVerb::Upload,
            "Blossom mirror authorization",
            BlossomAuthorizationScope::BlobSha256Hashes(vec![blob.sha256]),
            authorization_options,
            signer,
        )
        .await?;

        let response = self
            .client
            .put(url)
            .headers(headers)
            .json(&MirrorRequest { url: &blob.url })
            .send()
            .await?;
        Self::descriptor_response("Failed to mirror blob", response).await
    }

    /// Checks whether the server would accept media for optimization.
    pub async fn media_requirements<T>(
        &self,
        sha256: Sha256Hash,
        size: u64,
        content_type: Option<&str>,
        authorization_options: Option<BlossomAuthorizationOptions>,
        signer: Option<&T>,
    ) -> Result<BlossomPreflight, Error>
    where
        T: AsyncGetPublicKey + AsyncSignEvent,
    {
        self.preflight(
            "media",
            sha256,
            size,
            content_type,
            BlossomAuthorizationVerb::Media,
            authorization_options,
            signer,
        )
        .await
    }

    /// Uploads media for server-selected optimization according to BUD-05.
    pub async fn upload_media<T>(
        &self,
        data: Vec<u8>,
        content_type: Option<String>,
        authorization_options: Option<BlossomAuthorizationOptions>,
        signer: Option<&T>,
        payment: Option<&BlossomPaymentProof>,
    ) -> Result<BlobDescriptor, Error>
    where
        T: AsyncGetPublicKey + AsyncSignEvent,
    {
        let url = self.base_url.join("media")?;
        let sha256 = Sha256Hash::hash(&data);
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_LENGTH, HeaderValue::from(data.len()));
        headers.insert("X-SHA-256", HeaderValue::from_str(&sha256.to_string())?);
        if let Some(content_type) = content_type {
            headers.insert(CONTENT_TYPE, HeaderValue::from_str(&content_type)?);
        }
        Self::add_payment_header(&mut headers, payment)?;
        self.add_authorization_header(
            &mut headers,
            BlossomAuthorizationVerb::Media,
            "Blossom media authorization",
            BlossomAuthorizationScope::BlobSha256Hashes(vec![sha256]),
            authorization_options,
            signer,
        )
        .await?;

        let response = self
            .client
            .put(url)
            .headers(headers)
            .body(data)
            .send()
            .await?;
        Self::descriptor_response("Failed to optimize media", response).await
    }

    async fn preflight<T>(
        &self,
        endpoint: &str,
        sha256: Sha256Hash,
        size: u64,
        content_type: Option<&str>,
        verb: BlossomAuthorizationVerb,
        authorization_options: Option<BlossomAuthorizationOptions>,
        signer: Option<&T>,
    ) -> Result<BlossomPreflight, Error>
    where
        T: AsyncGetPublicKey + AsyncSignEvent,
    {
        let url = self.base_url.join(endpoint)?;
        let mut headers = HeaderMap::new();
        headers.insert("X-SHA-256", HeaderValue::from_str(&sha256.to_string())?);
        headers.insert("X-Content-Length", HeaderValue::from(size));
        if let Some(content_type) = content_type {
            headers.insert("X-Content-Type", HeaderValue::from_str(content_type)?);
        }
        self.add_authorization_header(
            &mut headers,
            verb,
            "Blossom upload preflight authorization",
            BlossomAuthorizationScope::BlobSha256Hashes(vec![sha256]),
            authorization_options,
            signer,
        )
        .await?;

        let response = self.client.head(url).headers(headers).send().await?;
        match response.status() {
            StatusCode::OK => Ok(BlossomPreflight::Accepted),
            StatusCode::PAYMENT_REQUIRED => {
                let requests = response
                    .headers()
                    .iter()
                    .filter_map(|(name, value)| {
                        let method = name.as_str().strip_prefix("x-")?;
                        if method == "reason" {
                            return None;
                        }
                        Some(BlossomPaymentRequest {
                            method: method.to_owned(),
                            request: value.to_str().ok()?.to_owned(),
                        })
                    })
                    .collect();
                Ok(BlossomPreflight::PaymentRequired(requests))
            }
            StatusCode::NOT_FOUND => Ok(BlossomPreflight::Unsupported),
            _ => Err(Error::response("Upload preflight failed", response)),
        }
    }

    async fn add_authorization_header<T>(
        &self,
        headers: &mut HeaderMap,
        verb: BlossomAuthorizationVerb,
        content: &'static str,
        scope: BlossomAuthorizationScope,
        authorization_options: Option<BlossomAuthorizationOptions>,
        signer: Option<&T>,
    ) -> Result<(), Error>
    where
        T: AsyncGetPublicKey + AsyncSignEvent,
    {
        if let Some(signer) = signer {
            let default_auth = self.default_auth(verb, content, scope);
            let authorization = authorization_options
                .map(|options| Self::update_authorization_fixture(&default_auth, options))
                .unwrap_or(default_auth);
            headers.insert(
                AUTHORIZATION,
                Self::build_auth_header(signer, authorization).await?,
            );
        }
        Ok(())
    }

    fn add_payment_header(
        headers: &mut HeaderMap,
        payment: Option<&BlossomPaymentProof>,
    ) -> Result<(), Error> {
        if let Some(payment) = payment {
            let name = reqwest::header::HeaderName::from_bytes(
                format!("X-{}", payment.method).as_bytes(),
            )?;
            headers.insert(name, HeaderValue::from_str(&payment.proof)?);
        }
        Ok(())
    }

    async fn descriptor_response(
        error_message: &'static str,
        response: Response,
    ) -> Result<BlobDescriptor, Error> {
        match response.status() {
            StatusCode::OK | StatusCode::CREATED => Ok(response.json().await?),
            _ => Err(Error::response(error_message, response)),
        }
    }

    /// Returns a default BlossomAuthorization object based on the parameters provided.
    fn default_auth<T>(
        &self,
        action: BlossomAuthorizationVerb,
        default_content: T,
        default_scope: BlossomAuthorizationScope,
    ) -> BlossomAuthorization
    where
        T: Into<String>,
    {
        let expiration_timestamp: Timestamp = Timestamp::now() + Duration::from_secs(300);
        BlossomAuthorization::new(
            default_content.into(),
            expiration_timestamp,
            action,
            default_scope,
        )
    }

    /// Updates a default BlossomAuthorization fixture with the provided options.
    pub fn update_authorization_fixture(
        default: &BlossomAuthorization,
        options: BlossomAuthorizationOptions,
    ) -> BlossomAuthorization {
        BlossomAuthorization {
            content: options.content.unwrap_or(default.content.clone()),
            expiration: options.expiration.unwrap_or(default.expiration),
            action: options.action.unwrap_or(default.action),
            scope: options.scope.unwrap_or(default.scope.clone()),
        }
    }

    /// Helper function to build authorization header.
    ///
    /// <https://github.com/hzrd149/blossom/blob/master/buds/01.md>
    async fn build_auth_header<T>(
        signer: &T,
        authz: BlossomAuthorization,
    ) -> Result<HeaderValue, Error>
    where
        T: AsyncGetPublicKey + AsyncSignEvent,
    {
        let auth_event: Event = authz.finalize_async(signer).await?;
        let encoded_auth: String = URL_SAFE_NO_PAD.encode(auth_event.as_json());
        let value: String = format!("Nostr {}", encoded_auth);
        Ok(HeaderValue::try_from(value)?)
    }
}

/// Options for customizing BlossomAuthorization. All fields are optional.
#[derive(Debug, Clone, Default)]
pub struct BlossomAuthorizationOptions {
    /// A human readable string explaining to the user what the events intended use is
    pub content: Option<String>,
    /// A UNIX timestamp (in seconds) indicating when the authorization should be expired
    pub expiration: Option<Timestamp>,
    /// The type of action authorized by the user
    pub action: Option<BlossomAuthorizationVerb>,
    /// The scope of the authorization
    pub scope: Option<BlossomAuthorizationScope>,
}

/// Result of a BUD-05 or BUD-06 preflight request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlossomPreflight {
    /// The server indicates that the subsequent upload may proceed.
    Accepted,
    /// The server does not implement the optional preflight endpoint.
    Unsupported,
    /// The server requires one of the advertised payment methods.
    PaymentRequired(Vec<BlossomPaymentRequest>),
}

/// A BUD-07 payment request advertised by a server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlossomPaymentRequest {
    /// Payment method suffix from the `X-{method}` header.
    pub method: String,
    /// Encoded payment request supplied by the server.
    pub request: String,
}

/// A BUD-07 payment proof to attach to a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlossomPaymentProof {
    /// Payment method suffix, such as `cashu` or `lightning`.
    pub method: String,
    /// Encoded proof defined by the selected payment method.
    pub proof: String,
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;
    use tokio::time::{Duration, timeout};

    use super::*;

    async fn mock_server(response: &'static str) -> (Url, JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let (mut stream, _) = timeout(Duration::from_secs(2), listener.accept())
                .await
                .expect("request deadline elapsed")
                .unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 4096];
            loop {
                let read = timeout(Duration::from_secs(2), stream.read(&mut buffer))
                    .await
                    .expect("read deadline elapsed")
                    .unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                if let Some(headers_end) =
                    request.windows(4).position(|window| window == b"\r\n\r\n")
                {
                    let headers = String::from_utf8_lossy(&request[..headers_end]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    if request.len() >= headers_end + 4 + content_length {
                        break;
                    }
                }
            }
            stream.write_all(response.as_bytes()).await.unwrap();
            String::from_utf8(request).unwrap()
        });
        (
            Url::parse(&format!("http://{address}/ignored/path")).unwrap(),
            handle,
        )
    }

    #[tokio::test]
    async fn mirror_uses_root_endpoint_and_metadata_headers() {
        let hash = Sha256Hash::hash(b"blob");
        let body = format!(
            r#"{{"url":"https://cdn.example/{hash}.bin","sha256":"{hash}","size":4,"type":"application/octet-stream","uploaded":1}}"#
        );
        let response = Box::leak(
            format!(
                "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .into_boxed_str(),
        );
        let (base_url, request) = mock_server(response).await;
        let descriptor = BlobDescriptor {
            url: Url::parse(&format!("https://origin.example/{hash}.bin")).unwrap(),
            sha256: hash,
            size: 4,
            mime_type: "application/octet-stream".to_owned(),
            uploaded: Timestamp::from_secs(1),
            nip94: None,
        };

        BlossomClient::new(base_url)
            .mirror_blob(
                &descriptor,
                None,
                None::<&Keys>,
                Some(&BlossomPaymentProof {
                    method: "cashu".to_owned(),
                    proof: "cashuBproof".to_owned(),
                }),
            )
            .await
            .unwrap();

        let request = request.await.unwrap();
        let request_lowercase = request.to_ascii_lowercase();
        assert!(request.starts_with("PUT /mirror HTTP/1.1"));
        assert!(request_lowercase.contains(&format!("x-sha-256: {hash}")));
        assert!(request_lowercase.contains("x-content-length: 4"));
        assert!(request_lowercase.contains("x-cashu: cashubproof"));
        assert!(request.contains(&format!(r#"{{"url":"https://origin.example/{hash}.bin"}}"#)));
    }

    #[tokio::test]
    async fn preflight_returns_payment_challenges() {
        let response = "HTTP/1.1 402 Payment Required\r\nX-Cashu: creqArequest\r\nX-Reason: payment needed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        let (base_url, request) = mock_server(response).await;
        let hash = Sha256Hash::hash(b"blob");

        let result = BlossomClient::new(base_url)
            .upload_requirements(
                hash,
                4,
                Some("application/octet-stream"),
                None,
                None::<&Keys>,
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            BlossomPreflight::PaymentRequired(vec![BlossomPaymentRequest {
                method: "cashu".to_owned(),
                request: "creqArequest".to_owned(),
            }])
        );
        let request = request.await.unwrap().to_ascii_lowercase();
        assert!(request.starts_with("head /upload http/1.1"));
        assert!(request.contains(&format!("x-sha-256: {hash}")));
        assert!(request.contains("x-content-length: 4"));
        assert!(request.contains("x-content-type: application/octet-stream"));
    }
}
