use std::{fs, path::Path};

use anyhow::{Context, Result, bail};
use base64::{Engine, engine::general_purpose::STANDARD};
use ed25519_dalek::{Signer, SigningKey, pkcs8::DecodePrivateKey};
use rsa::{
    RsaPrivateKey,
    pkcs1::DecodeRsaPrivateKey,
    pss::BlindedSigningKey,
    signature::{RandomizedSigner, SignatureEncoding},
};
use sha2::Sha256;
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, handshake::client::Request};

pub enum PrivateKey {
    Rsa(Box<RsaPrivateKey>),
    Ed25519(Box<SigningKey>),
}

impl PrivateKey {
    pub fn from_pem(pem: &str) -> Result<Self> {
        if let Ok(key) = SigningKey::from_pkcs8_pem(pem) {
            return Ok(Self::Ed25519(Box::new(key)));
        }
        if let Ok(key) = RsaPrivateKey::from_pkcs8_pem(pem) {
            return Ok(Self::Rsa(Box::new(key)));
        }
        if let Ok(key) = RsaPrivateKey::from_pkcs1_pem(pem) {
            return Ok(Self::Rsa(Box::new(key)));
        }
        bail!(
            "private key must be an unencrypted RSA (PKCS#1/PKCS#8) or Ed25519 (PKCS#8) PEM; key contents are never logged"
        )
    }

    pub fn load(path: &Path) -> Result<Self> {
        let pem = fs::read_to_string(path)
            .with_context(|| format!("cannot read private-key file {}", path.display()))?;
        if pem.trim().is_empty() {
            bail!(
                "private-key file {} is empty; save the private key from Kalshi here",
                path.display()
            );
        }
        Self::from_pem(&pem)
    }

    pub fn sign(&self, timestamp_ms: i64, method: &str, path: &str) -> String {
        let message = format!(
            "{timestamp_ms}{method}{}",
            path.split('?').next().unwrap_or(path)
        );
        let bytes = match self {
            Self::Ed25519(key) => key.sign(message.as_bytes()).to_bytes().to_vec(),
            Self::Rsa(key) => BlindedSigningKey::<Sha256>::new_with_salt_len((**key).clone(), 32)
                .sign_with_rng(&mut rand::rngs::OsRng, message.as_bytes())
                .to_vec(),
        };
        STANDARD.encode(bytes)
    }
}

pub struct Credentials {
    pub id: String,
    pub key: PrivateKey,
}

impl Credentials {
    pub fn request(&self, endpoint: &str, timestamp_ms: i64) -> Result<Request> {
        let path = url::Url::parse(endpoint)?.path().to_owned();
        let mut request = endpoint.into_client_request()?;
        for (name, value) in [
            ("KALSHI-ACCESS-KEY", self.id.clone()),
            ("KALSHI-ACCESS-TIMESTAMP", timestamp_ms.to_string()),
            (
                "KALSHI-ACCESS-SIGNATURE",
                self.key.sign(timestamp_ms, "GET", &path),
            ),
        ] {
            request.headers_mut().insert(name, value.parse()?);
        }
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signature, Verifier};

    #[test]
    fn ed25519_signature_uses_path_without_query() {
        let key = SigningKey::from_bytes(&[7; 32]);
        let verifier = key.verifying_key();
        let signed = PrivateKey::Ed25519(Box::new(key)).sign(123, "GET", "/trade-api/ws/v2?x=1");
        let signature = Signature::from_slice(&STANDARD.decode(signed).unwrap()).unwrap();
        verifier
            .verify(b"123GET/trade-api/ws/v2", &signature)
            .unwrap();
    }

    #[test]
    fn rsa_signature_has_digest_length_salt() {
        let key = RsaPrivateKey::new(&mut rand::rngs::OsRng, 2048).unwrap();
        let verify = rsa::pss::VerifyingKey::<Sha256>::new_with_salt_len(key.to_public_key(), 32);
        let signed = PrivateKey::Rsa(Box::new(key)).sign(123, "GET", "/trade-api/ws/v2");
        let sig =
            rsa::pss::Signature::try_from(STANDARD.decode(signed).unwrap().as_slice()).unwrap();
        verify.verify(b"123GET/trade-api/ws/v2", &sig).unwrap();
    }
}
