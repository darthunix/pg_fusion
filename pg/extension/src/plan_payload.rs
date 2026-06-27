use datafusion::logical_expr::LogicalPlan;
use plan_codec::{EncodeProgress, PlanEncodeSession};
use thiserror::Error;

const PAYLOAD_PREFIX: &str = "PGFUSION_CUSTOM_SCAN_V1\n";
const LEGACY_FRONTEND_TAG: &str = "frontend\n";
const FRONTEND_PLAN_TAG: &str = "frontend_plan\n";
const PLAN_ENCODE_CHUNK_LEN: usize = 8192;

#[derive(Debug, Clone)]
pub(crate) enum CustomScanPlanSource {
    FrontendPlan(Vec<u8>),
}

impl CustomScanPlanSource {
    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::FrontendPlan(_) => "pg_frontend",
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum PlanPayloadError {
    #[error("PostgreSQL hybrid plan codec failed: {0}")]
    PlanCodec(String),
    #[error("invalid custom scan plan payload: {0}")]
    Invalid(String),
}

pub(crate) fn encode_frontend_plan(plan: &LogicalPlan) -> Result<String, PlanPayloadError> {
    let bytes = encode_plan(plan)?;
    Ok(format!(
        "{PAYLOAD_PREFIX}{FRONTEND_PLAN_TAG}{}",
        hex_encode(&bytes)
    ))
}

pub(crate) fn decode_plan_source(value: &str) -> Result<CustomScanPlanSource, PlanPayloadError> {
    let Some(rest) = value.strip_prefix(PAYLOAD_PREFIX) else {
        return Err(PlanPayloadError::Invalid(
            "legacy SQL-text payload is no longer supported".into(),
        ));
    };
    if let Some(hex) = rest.strip_prefix(FRONTEND_PLAN_TAG) {
        return Ok(CustomScanPlanSource::FrontendPlan(hex_decode(hex)?));
    }
    if rest.strip_prefix(LEGACY_FRONTEND_TAG).is_some() {
        return Err(PlanPayloadError::Invalid(
            "legacy typed query payload is no longer supported".into(),
        ));
    }
    Err(PlanPayloadError::Invalid(
        "unknown custom scan payload tag".into(),
    ))
}

fn encode_plan(plan: &LogicalPlan) -> Result<Vec<u8>, PlanPayloadError> {
    let mut session =
        PlanEncodeSession::new(plan).map_err(|err| PlanPayloadError::PlanCodec(err.to_string()))?;
    let mut encoded = Vec::new();

    loop {
        let mut chunk = [0_u8; PLAN_ENCODE_CHUNK_LEN];
        match session
            .write_chunk(&mut chunk)
            .map_err(|err| PlanPayloadError::PlanCodec(err.to_string()))?
        {
            EncodeProgress::NeedMoreOutput { written } => {
                encoded.extend_from_slice(&chunk[..written]);
            }
            EncodeProgress::Done { written } => {
                encoded.extend_from_slice(&chunk[..written]);
                break;
            }
        }
    }

    Ok(encoded)
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn hex_decode(value: &str) -> Result<Vec<u8>, PlanPayloadError> {
    if !value.len().is_multiple_of(2) {
        return Err(PlanPayloadError::Invalid(
            "hex payload has odd length".into(),
        ));
    }
    let mut decoded = Vec::with_capacity(value.len() / 2);
    let bytes = value.as_bytes();
    for chunk in bytes.chunks_exact(2) {
        let high = hex_value(chunk[0])?;
        let low = hex_value(chunk[1])?;
        decoded.push((high << 4) | low);
    }
    Ok(decoded)
}

fn hex_value(byte: u8) -> Result<u8, PlanPayloadError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(PlanPayloadError::Invalid(format!(
            "non-hex byte {byte:#x} in payload"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_sql_payload_is_rejected() {
        let err = decode_plan_source("SELECT 1").expect_err("legacy SQL payload must be rejected");
        assert!(
            err.to_string().contains("legacy SQL-text payload"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn hex_roundtrips() {
        let bytes = b"\x00hello\xff";
        let encoded = hex_encode(bytes);
        assert_eq!(hex_decode(&encoded).unwrap(), bytes);
    }

    #[test]
    fn frontend_plan_payload_decodes_raw_plan_bytes() {
        let decoded =
            decode_plan_source(&format!("{PAYLOAD_PREFIX}{FRONTEND_PLAN_TAG}0001ff")).unwrap();
        let CustomScanPlanSource::FrontendPlan(bytes) = decoded;
        assert_eq!(bytes, vec![0x00, 0x01, 0xff]);
    }

    #[test]
    fn legacy_frontend_query_payload_is_rejected() {
        let err = decode_plan_source(&format!("{PAYLOAD_PREFIX}{LEGACY_FRONTEND_TAG}0001ff"))
            .expect_err("legacy typed query payload must be rejected");
        assert!(
            err.to_string().contains("legacy typed query payload"),
            "unexpected error: {err}"
        );
    }
}
