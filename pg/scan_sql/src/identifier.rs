use crate::error::CompileError;

/// Validate that an identifier fits within the backend PostgreSQL byte limit.
pub(crate) fn validate_identifier(
    identifier: &str,
    max_identifier_bytes: usize,
    kind: &'static str,
) -> Result<(), CompileError> {
    if identifier.len() > max_identifier_bytes {
        return Err(CompileError::OverlongIdentifier {
            kind,
            identifier: identifier.to_owned(),
            max_bytes: max_identifier_bytes,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_identifier;
    use crate::CompileError;

    const DEFAULT_PG_IDENTIFIER_BYTES: usize = 63;

    #[test]
    fn accepts_short_identifier() {
        assert_eq!(
            validate_identifier("orders", DEFAULT_PG_IDENTIFIER_BYTES, "table"),
            Ok(())
        );
    }

    #[test]
    fn rejects_long_identifier() {
        let input = format!("orders_{}", "x".repeat(80));
        assert_eq!(
            validate_identifier(&input, DEFAULT_PG_IDENTIFIER_BYTES, "table"),
            Err(CompileError::OverlongIdentifier {
                kind: "table",
                identifier: input,
                max_bytes: DEFAULT_PG_IDENTIFIER_BYTES,
            })
        );
    }

    #[test]
    fn respects_custom_identifier_limit() {
        assert_eq!(validate_identifier("abcdefg", 7, "column"), Ok(()));
        assert_eq!(
            validate_identifier("abcdefgh", 7, "column"),
            Err(CompileError::OverlongIdentifier {
                kind: "column",
                identifier: "abcdefgh".into(),
                max_bytes: 7,
            })
        );
    }
}
