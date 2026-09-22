// Test-only TLS FIX venue. Mints an ephemeral CA+leaf (SAN=localhost),
// accepts `tokio-native-tls` connections on 127.0.0.1:{ephemeral}, and
// speaks a minimal Logon ACK (35=A) + Heartbeat (35=0). Production
// `mock_gateway.rs` stays plain TCP.

use anyhow::{anyhow, Result};
use openssl::asn1::Asn1Time;
use openssl::bn::{BigNum, MsbOption};
use openssl::hash::MessageDigest;
use openssl::nid::Nid;
use openssl::pkey::{PKey, Private};
use openssl::rsa::Rsa;
use openssl::x509::extension::{BasicConstraints, ExtendedKeyUsage, KeyUsage, SubjectAlternativeName, SubjectKeyIdentifier};
use openssl::x509::{X509NameBuilder, X509};
use std::path::PathBuf;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use crate::fix_protocol::{tags, FIXMessage};

pub struct TlsTestHandle {
    pub port: u16,
    pub ca_pem_path: PathBuf,
    join: Option<JoinHandle<()>>,
}

impl Drop for TlsTestHandle {
    fn drop(&mut self) {
        if let Some(join) = self.join.take() {
            join.abort();
        }
        let _ = std::fs::remove_file(&self.ca_pem_path);
    }
}

pub async fn start_trusted() -> Result<TlsTestHandle> {
    start_with_ca_cn("hft-trusted-ca").await
}

pub async fn start_untrusted() -> Result<TlsTestHandle> {
    start_with_ca_cn("hft-untrusted-ca").await
}

pub fn mint_self_signed_ca_pem(cn: &str) -> Result<Vec<u8>> {
    let (_key, cert) = mint_ca(cn)?;
    Ok(cert.to_pem()?)
}

pub fn mint_self_signed_ca_der(cn: &str) -> Result<Vec<u8>> {
    let (_key, cert) = mint_ca(cn)?;
    Ok(cert.to_der()?)
}

pub fn write_temp_bytes(prefix: &str, bytes: &[u8]) -> Result<PathBuf> {
    let path = std::env::temp_dir().join(format!("{prefix}-{}.bin", uuid::Uuid::new_v4()));
    std::fs::write(&path, bytes)?;
    Ok(path)
}

async fn start_with_ca_cn(ca_cn: &str) -> Result<TlsTestHandle> {
    let (ca_key, ca_cert) = mint_ca(ca_cn)?;
    let (leaf_key, leaf_cert) = mint_leaf(&ca_key, &ca_cert, "localhost")?;
    let cert_pem = leaf_cert.to_pem()?;
    let key_pem = leaf_key.private_key_to_pem_pkcs8()?;
    let identity = native_tls::Identity::from_pkcs8(&cert_pem, &key_pem)
        .map_err(|e| anyhow!("tls test identity: {e}"))?;
    let acceptor = native_tls::TlsAcceptor::builder(identity)
        .build()
        .map_err(|e| anyhow!("tls test acceptor: {e}"))?;
    let acceptor = tokio_native_tls::TlsAcceptor::from(acceptor);

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();

    let ca_pem_path =
        std::env::temp_dir().join(format!("hft-tls-ca-{}.pem", uuid::Uuid::new_v4()));
    std::fs::write(&ca_pem_path, ca_cert.to_pem()?)?;

    let join = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((tcp, _)) => {
                    let acceptor = acceptor.clone();
                    tokio::spawn(async move {
                        match acceptor.accept(tcp).await {
                            Ok(tls) => {
                                let _ = handle_fix(tls).await;
                            }
                            Err(_) => {}
                        }
                    });
                }
                Err(_) => break,
            }
        }
    });

    Ok(TlsTestHandle {
        port,
        ca_pem_path,
        join: Some(join),
    })
}

fn mint_ca(cn: &str) -> Result<(PKey<Private>, X509)> {
    let rsa = Rsa::generate(2048)?;
    let key = PKey::from_rsa(rsa)?;

    let mut name = X509NameBuilder::new()?;
    name.append_entry_by_nid(Nid::COMMONNAME, cn)?;
    let name = name.build();

    let mut cert = X509::builder()?;
    cert.set_version(2)?;
    let serial = {
        let mut bn = BigNum::new()?;
        bn.rand(64, MsbOption::MAYBE_ZERO, false)?;
        bn.to_asn1_integer()?
    };
    cert.set_serial_number(&serial)?;
    cert.set_subject_name(&name)?;
    cert.set_issuer_name(&name)?;
    cert.set_pubkey(&key)?;
    cert.set_not_before(Asn1Time::days_from_now(0)?.as_ref())?;
    cert.set_not_after(Asn1Time::days_from_now(30)?.as_ref())?;
    cert.append_extension(BasicConstraints::new().critical().ca().build()?)?;
    cert.append_extension(
        KeyUsage::new()
            .critical()
            .key_cert_sign()
            .crl_sign()
            .build()?,
    )?;
    let ski = SubjectKeyIdentifier::new().build(&cert.x509v3_context(None, None))?;
    cert.append_extension(ski)?;
    cert.sign(&key, MessageDigest::sha256())?;
    Ok((key, cert.build()))
}

fn mint_leaf(ca_key: &PKey<Private>, ca_cert: &X509, cn: &str) -> Result<(PKey<Private>, X509)> {
    let rsa = Rsa::generate(2048)?;
    let key = PKey::from_rsa(rsa)?;

    let mut name = X509NameBuilder::new()?;
    name.append_entry_by_nid(Nid::COMMONNAME, cn)?;
    let name = name.build();

    let mut cert = X509::builder()?;
    cert.set_version(2)?;
    let serial = {
        let mut bn = BigNum::new()?;
        bn.rand(64, MsbOption::MAYBE_ZERO, false)?;
        bn.to_asn1_integer()?
    };
    cert.set_serial_number(&serial)?;
    cert.set_subject_name(&name)?;
    cert.set_issuer_name(ca_cert.subject_name())?;
    cert.set_pubkey(&key)?;
    cert.set_not_before(Asn1Time::days_from_now(0)?.as_ref())?;
    cert.set_not_after(Asn1Time::days_from_now(30)?.as_ref())?;
    cert.append_extension(
        KeyUsage::new()
            .digital_signature()
            .key_encipherment()
            .build()?,
    )?;
    cert.append_extension(ExtendedKeyUsage::new().server_auth().build()?)?;
    let ski = SubjectKeyIdentifier::new().build(&cert.x509v3_context(Some(ca_cert), None))?;
    cert.append_extension(ski)?;
    let san = {
        let ctx = cert.x509v3_context(Some(ca_cert), None);
        SubjectAlternativeName::new().dns(cn).build(&ctx)?
    };
    cert.append_extension(san)?;
    cert.sign(ca_key, MessageDigest::sha256())?;
    Ok((key, cert.build()))
}

async fn handle_fix<S>(mut socket: S) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut buffer = Vec::new();
    let mut read_buf = [0u8; 4096];
    let mut out_seq: u32 = 0;

    loop {
        let n = socket.read(&mut read_buf).await?;
        if n == 0 {
            return Ok(());
        }
        buffer.extend_from_slice(&read_buf[..n]);

        while let Some((msg, consumed)) = try_extract(&buffer) {
            buffer.drain(..consumed);
            match msg.msg_type {
                'A' => {
                    out_seq += 1;
                    let mut resp = FIXMessage::new('A', "MOCK.VENUE", "TEST.CLIENT", out_seq);
                    resp.set_field(tags::ENCRYPT_METHOD, "0");
                    resp.set_field(tags::HEARTBEAT_INT, "30");
                    send(&mut socket, &resp).await?;
                }
                '0' => {
                    out_seq += 1;
                    let resp = FIXMessage::new('0', "MOCK.VENUE", "TEST.CLIENT", out_seq);
                    send(&mut socket, &resp).await?;
                }
                '5' => {
                    out_seq += 1;
                    let resp = FIXMessage::new('5', "MOCK.VENUE", "TEST.CLIENT", out_seq);
                    send(&mut socket, &resp).await?;
                    return Ok(());
                }
                _ => {}
            }
        }
    }
}

async fn send<S: AsyncWrite + Unpin>(socket: &mut S, msg: &FIXMessage) -> Result<()> {
    let encoded = msg.encode()?;
    socket.write_all(&encoded).await?;
    socket.flush().await?;
    Ok(())
}

fn try_extract(buf: &[u8]) -> Option<(FIXMessage, usize)> {
    const SOH: u8 = 0x01;
    const TRAILER_LEN: usize = 7;
    if buf.len() < 10 || !buf.starts_with(b"8=") {
        return None;
    }
    let p1 = buf.iter().position(|&b| b == SOH)?;
    if buf.len() < p1 + 3 || &buf[p1 + 1..p1 + 3] != b"9=" {
        return None;
    }
    let rel = buf[p1 + 3..].iter().position(|&b| b == SOH)?;
    let p2 = p1 + 3 + rel;
    let body_len: usize = std::str::from_utf8(&buf[p1 + 3..p2]).ok()?.parse().ok()?;
    let trailer_start = p2 + 1 + body_len;
    let message_end = trailer_start + TRAILER_LEN;
    if buf.len() < message_end {
        return None;
    }
    if &buf[trailer_start..trailer_start + 3] != b"10=" || buf[message_end - 1] != SOH {
        return None;
    }
    let msg = FIXMessage::decode(&buf[..message_end]).ok()?;
    Some((msg, message_end))
}
