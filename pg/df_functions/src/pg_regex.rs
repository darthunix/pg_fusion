use std::any::Any;
use std::sync::Arc;

use arrow_array::builder::BooleanBuilder;
use arrow_array::{Array, ArrayRef, LargeStringArray, StringArray, StringViewArray};
use arrow_schema::DataType;
use datafusion_common::{exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use regex::{Regex, RegexBuilder};

const PG_REGEX_MAX_REPEAT: usize = 255;

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
enum PgRegexOp {
    Match,
    NotMatch,
}

/// PostgreSQL-oriented regex boolean operator for residual DataFusion
/// execution. The worker cannot call PostgreSQL's regex engine, so this UDF
/// supports a deterministic safe subset and rejects known PostgreSQL ARE
/// constructs whose semantics do not map cleanly to Rust regex.
#[derive(Debug, Eq, Hash, PartialEq)]
pub struct PgRegexMatch {
    name: &'static str,
    op: PgRegexOp,
    signature: Signature,
}

impl PgRegexMatch {
    fn new(name: &'static str, op: PgRegexOp) -> Self {
        Self {
            name,
            op,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

pub fn pg_regex_match_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(PgRegexMatch::new(
        "pg_fusion_regex_match",
        PgRegexOp::Match,
    )))
}

pub fn pg_regex_not_match_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(PgRegexMatch::new(
        "pg_fusion_regex_not_match",
        PgRegexOp::NotMatch,
    )))
}

impl ScalarUDFImpl for PgRegexMatch {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        validate_arg_count(self.name, arg_types.len())?;
        Ok(DataType::Boolean)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        validate_arg_count(self.name, arg_types.len())?;
        Ok(vec![DataType::Utf8View, DataType::Utf8View])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return exec_err!("{} expects exactly two arguments", self.name);
        }

        match (&args.args[0], &args.args[1]) {
            (ColumnarValue::Scalar(value), ColumnarValue::Scalar(pattern)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(
                    match_regex_scalar(self.op, scalar_text(value)?, scalar_text(pattern)?)?,
                )))
            }
            (ColumnarValue::Array(values), ColumnarValue::Scalar(pattern)) => {
                Ok(ColumnarValue::Array(match_regex_array_scalar(
                    self.op,
                    values,
                    scalar_text(pattern)?,
                )?))
            }
            _ => {
                let arrays = ColumnarValue::values_to_arrays(&args.args)?;
                Ok(ColumnarValue::Array(match_regex_arrays(
                    self.op, &arrays[0], &arrays[1],
                )?))
            }
        }
    }
}

fn validate_arg_count(name: &str, count: usize) -> Result<()> {
    if count == 2 {
        Ok(())
    } else {
        plan_err!("{name} expects exactly two arguments")
    }
}

fn scalar_text(value: &ScalarValue) -> Result<Option<&str>> {
    match value {
        ScalarValue::Utf8(value) | ScalarValue::LargeUtf8(value) | ScalarValue::Utf8View(value) => {
            Ok(value.as_deref())
        }
        ScalarValue::Null => Ok(None),
        other => exec_err!("pg_fusion regex expected text argument after coercion, got {other:?}"),
    }
}

fn match_regex_scalar(
    op: PgRegexOp,
    value: Option<&str>,
    pattern: Option<&str>,
) -> Result<Option<bool>> {
    let (Some(value), Some(pattern)) = (value, pattern) else {
        return Ok(None);
    };
    let regex = compile_pg_regex_subset(pattern)?;
    Ok(Some(apply_op(op, regex.is_match(value))))
}

fn match_regex_array_scalar(
    op: PgRegexOp,
    values: &ArrayRef,
    pattern: Option<&str>,
) -> Result<ArrayRef> {
    let mut builder = BooleanBuilder::new();
    let Some(pattern) = pattern else {
        for _ in 0..values.len() {
            builder.append_null();
        }
        return Ok(Arc::new(builder.finish()));
    };
    let regex = compile_pg_regex_subset(pattern)?;
    for row in 0..values.len() {
        match array_text(values, row)? {
            Some(value) => builder.append_value(apply_op(op, regex.is_match(value))),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn match_regex_arrays(op: PgRegexOp, values: &ArrayRef, patterns: &ArrayRef) -> Result<ArrayRef> {
    let mut builder = BooleanBuilder::new();
    for row in 0..values.len() {
        let value = array_text(values, row)?;
        let pattern = array_text(patterns, row)?;
        match (value, pattern) {
            (Some(value), Some(pattern)) => {
                let regex = compile_pg_regex_subset(pattern)?;
                builder.append_value(apply_op(op, regex.is_match(value)));
            }
            _ => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn array_text(array: &ArrayRef, row: usize) -> Result<Option<&str>> {
    if array.is_null(row) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Utf8 => {
            let array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution(
                        "pg_fusion regex expected Utf8 array".into(),
                    )
                })?;
            Ok(Some(array.value(row)))
        }
        DataType::Utf8View => {
            let array = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution(
                        "pg_fusion regex expected Utf8View array".into(),
                    )
                })?;
            Ok(Some(array.value(row)))
        }
        DataType::LargeUtf8 => {
            let array = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution(
                        "pg_fusion regex expected LargeUtf8 array".into(),
                    )
                })?;
            Ok(Some(array.value(row)))
        }
        other => exec_err!("pg_fusion regex expected text array after coercion, got {other:?}"),
    }
}

fn compile_pg_regex_subset(pattern: &str) -> Result<Regex> {
    validate_pg_regex_safe_subset(pattern)?;
    RegexBuilder::new(pattern)
        .dot_matches_new_line(true)
        .build()
        .map_err(|err| {
            datafusion_common::DataFusionError::Execution(format!(
                "pg_fusion regex pattern did not compile in the supported subset: {err}"
            ))
        })
}

fn validate_pg_regex_safe_subset(pattern: &str) -> Result<()> {
    if pattern.contains("(?") {
        return unsupported_regex_pattern("ARE extension groups and lookaround are unsupported");
    }
    if pattern.contains("[[:")
        || pattern.contains("[[.")
        || pattern.contains("[[=")
        || pattern.contains("[.")
        || pattern.contains("[=")
    {
        return unsupported_regex_pattern(
            "POSIX bracket classes, collating elements, and equivalence classes are unsupported",
        );
    }

    let mut chars = pattern.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            continue;
        }
        let Some(next) = chars.peek().copied() else {
            break;
        };
        if next == '\\' {
            chars.next();
            continue;
        }
        if next.is_ascii_alphanumeric() {
            return unsupported_regex_pattern(
                "alphanumeric escapes, constraint escapes, and backreferences are unsupported",
            );
        }
    }

    validate_pg_regex_quantifiers(pattern)?;

    Ok(())
}

fn validate_pg_regex_quantifiers(pattern: &str) -> Result<()> {
    let chars = pattern.chars().collect::<Vec<_>>();
    let mut idx = 0;
    let mut can_quantify = false;

    while idx < chars.len() {
        match chars[idx] {
            '\\' => {
                idx = (idx + 2).min(chars.len());
                can_quantify = true;
            }
            '[' => {
                idx = skip_bracket_expression(&chars, idx);
                can_quantify = true;
            }
            '(' | '|' | '^' | '$' => {
                idx += 1;
                can_quantify = false;
            }
            ')' => {
                idx += 1;
                can_quantify = true;
            }
            '*' | '+' | '?' => {
                if !can_quantify {
                    return unsupported_regex_pattern(
                        "quantifier has no preceding atom or follows another quantifier",
                    );
                }
                idx += 1;
                if matches!(chars.get(idx), Some('?')) {
                    idx += 1;
                }
                can_quantify = false;
            }
            '{' => {
                let Some((close_idx, lower, upper)) = parse_bound_quantifier(&chars, idx) else {
                    idx += 1;
                    can_quantify = true;
                    continue;
                };
                if !can_quantify {
                    return unsupported_regex_pattern(
                        "quantifier has no preceding atom or follows another quantifier",
                    );
                }
                validate_bound_quantifier_limits(lower, upper)?;
                idx = close_idx + 1;
                if matches!(chars.get(idx), Some('?')) {
                    idx += 1;
                }
                can_quantify = false;
            }
            _ => {
                idx += 1;
                can_quantify = true;
            }
        }
    }

    Ok(())
}

fn skip_bracket_expression(chars: &[char], open_idx: usize) -> usize {
    let mut idx = open_idx + 1;
    while idx < chars.len() {
        match chars[idx] {
            '\\' => {
                idx = (idx + 2).min(chars.len());
            }
            ']' if idx > open_idx + 1 => {
                return idx + 1;
            }
            _ => {
                idx += 1;
            }
        }
    }
    chars.len()
}

fn parse_bound_quantifier(
    chars: &[char],
    open_idx: usize,
) -> Option<(usize, usize, Option<usize>)> {
    let (lower, mut idx) = parse_decimal(chars, open_idx + 1)?;
    match chars.get(idx).copied()? {
        '}' => Some((idx, lower, Some(lower))),
        ',' => {
            idx += 1;
            if matches!(chars.get(idx), Some('}')) {
                return Some((idx, lower, None));
            }
            let (upper, next_idx) = parse_decimal(chars, idx)?;
            if matches!(chars.get(next_idx), Some('}')) {
                Some((next_idx, lower, Some(upper)))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn parse_decimal(chars: &[char], mut idx: usize) -> Option<(usize, usize)> {
    let mut value = 0usize;
    let start_idx = idx;
    while let Some(digit) = chars.get(idx).and_then(|ch| ch.to_digit(10)) {
        value = value.saturating_mul(10).saturating_add(digit as usize);
        idx += 1;
    }
    (idx > start_idx).then_some((value, idx))
}

fn validate_bound_quantifier_limits(lower: usize, upper: Option<usize>) -> Result<()> {
    if lower > PG_REGEX_MAX_REPEAT || upper.is_some_and(|upper| upper > PG_REGEX_MAX_REPEAT) {
        return unsupported_regex_pattern("repetition bounds above 255 are unsupported");
    }
    if upper.is_some_and(|upper| lower > upper) {
        return unsupported_regex_pattern("repetition lower bound exceeds upper bound");
    }
    Ok(())
}

fn unsupported_regex_pattern<T>(reason: &str) -> Result<T> {
    exec_err!("pg_fusion regex pattern is outside the supported PostgreSQL regex subset: {reason}")
}

fn apply_op(op: PgRegexOp, matched: bool) -> bool {
    match op {
        PgRegexOp::Match => matched,
        PgRegexOp::NotMatch => !matched,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;
    use datafusion_common::config::ConfigOptions;

    fn invoke(udf: PgRegexMatch, args: Vec<ColumnarValue>) -> Result<ColumnarValue> {
        let number_rows = args
            .iter()
            .find_map(|arg| match arg {
                ColumnarValue::Array(array) => Some(array.len()),
                ColumnarValue::Scalar(_) => None,
            })
            .unwrap_or(1);
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| Arc::new(Field::new(format!("arg_{idx}"), arg.data_type(), true)))
            .collect::<Vec<_>>();
        let return_field = Arc::new(Field::new("result", DataType::Boolean, true));
        udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    fn scalar_match(value: Option<&str>, pattern: Option<&str>) -> Result<Option<bool>> {
        match invoke(
            PgRegexMatch::new("pg_fusion_regex_match", PgRegexOp::Match),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(value.map(str::to_owned))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(pattern.map(str::to_owned))),
            ],
        )? {
            ColumnarValue::Scalar(ScalarValue::Boolean(value)) => Ok(value),
            other => exec_err!("unexpected regex result: {other:?}"),
        }
    }

    #[test]
    fn matches_scalar_values() {
        assert_eq!(
            scalar_match(Some("alpha"), Some("^al.*")).unwrap(),
            Some(true)
        );
        assert_eq!(
            scalar_match(Some("beta"), Some("^al.*")).unwrap(),
            Some(false)
        );
        assert_eq!(scalar_match(None, Some("^al.*")).unwrap(), None);
        assert_eq!(scalar_match(Some("alpha"), None).unwrap(), None);
    }

    #[test]
    fn matches_newline_with_dot_in_postgresql_default_mode() {
        assert_eq!(scalar_match(Some("\n"), Some("^.$")).unwrap(), Some(true));
    }

    #[test]
    fn negates_match_for_not_match_operator() {
        let result = invoke(
            PgRegexMatch::new("pg_fusion_regex_not_match", PgRegexOp::NotMatch),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("alpha".into()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("^al.*".into()))),
            ],
        )
        .unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Boolean(value)) => assert_eq!(value, Some(false)),
            other => panic!("unexpected regex result: {other:?}"),
        }
    }

    #[test]
    fn matches_array_values_with_scalar_pattern() {
        let result = invoke(
            PgRegexMatch::new("pg_fusion_regex_match", PgRegexOp::Match),
            vec![
                ColumnarValue::Array(Arc::new(StringViewArray::from(vec![
                    Some("alpha"),
                    Some("beta"),
                    None,
                ]))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some("^a".into()))),
            ],
        )
        .unwrap();
        let ColumnarValue::Array(array) = result else {
            panic!("array input should return array");
        };
        let array = array
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .expect("regex result should be BooleanArray");
        assert!(array.value(0));
        assert!(!array.value(1));
        assert!(array.is_null(2));
    }

    #[test]
    fn matches_array_values_with_array_patterns() {
        let result = invoke(
            PgRegexMatch::new("pg_fusion_regex_match", PgRegexOp::Match),
            vec![
                ColumnarValue::Array(Arc::new(StringArray::from(vec![
                    Some("alpha"),
                    Some("beta"),
                    Some("gamma"),
                ]))),
                ColumnarValue::Array(Arc::new(StringArray::from(vec![
                    Some("^a"),
                    Some("^b"),
                    None,
                ]))),
            ],
        )
        .unwrap();
        let ColumnarValue::Array(array) = result else {
            panic!("array input should return array");
        };
        let array = array
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .expect("regex result should be BooleanArray");
        assert!(array.value(0));
        assert!(array.value(1));
        assert!(array.is_null(2));
    }

    #[test]
    fn accepts_supported_postgresql_quantifiers() {
        for pattern in [
            r"a*?",
            r"a+?",
            r"a??",
            r"a{0}",
            r"a{255}",
            r"a{0,255}",
            r"a{1,}",
            r"a{1,2}?",
        ] {
            scalar_match(Some("aaa"), Some(pattern)).unwrap_or_else(|err| {
                panic!("supported quantifier should compile: {pattern}: {err}")
            });
        }
    }

    #[test]
    fn rejects_postgresql_invalid_quantifiers() {
        for pattern in [
            r"a**",
            r"a++",
            r"a?*",
            r"a{1,2}*",
            r"a{256}",
            r"a{1,256}",
            r"a{256,}",
            r"a{3,2}",
            r"*a",
            r"(|*)",
            r"^*",
            r"$*",
        ] {
            let err = scalar_match(Some("aaa"), Some(pattern))
                .expect_err("PostgreSQL-invalid quantifier should fail closed");
            assert!(
                err.to_string()
                    .contains("supported PostgreSQL regex subset"),
                "unexpected error for {pattern}: {err}"
            );
        }
    }

    #[test]
    fn rejects_known_postgresql_are_only_constructs() {
        for pattern in [
            r"([bc])\1",
            r"(?=a)b",
            r"(?<=a)b",
            r"\mword",
            r"\yword\y",
            r"[[:<:]]word[[:>:]]",
            r"[[=o=]]",
            r"[[.ch.]]",
        ] {
            let err = scalar_match(Some("word"), Some(pattern))
                .expect_err("unsupported PostgreSQL regex construct should fail closed");
            assert!(
                err.to_string()
                    .contains("supported PostgreSQL regex subset"),
                "unexpected error for {pattern}: {err}"
            );
        }
    }
}
