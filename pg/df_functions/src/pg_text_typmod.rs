use std::any::Any;
use std::borrow::Cow;
use std::sync::Arc;

use arrow_array::builder::{Int32Builder, StringViewBuilder};
use arrow_array::{Array, ArrayRef, Int32Array, LargeStringArray, StringArray, StringViewArray};
use arrow_schema::DataType;
use datafusion_common::{exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
enum TextTypmodKind {
    Varchar,
    Bpchar,
}

/// PostgreSQL-compatible text typmod cast for intermediate DataFusion
/// expressions. PostgreSQL backend APIs are intentionally not used here because
/// the function executes in the worker runtime too.
#[derive(Debug, Eq, Hash, PartialEq)]
pub struct PgTextTypmod {
    name: &'static str,
    kind: TextTypmodKind,
    signature: Signature,
}

/// PostgreSQL-compatible comparison key for `bpchar` equality. PostgreSQL
/// ignores trailing ASCII spaces for `bpchar` equality/distinct comparisons.
#[derive(Debug, Eq, Hash, PartialEq)]
pub struct PgBpcharCmpKey {
    signature: Signature,
}

/// PostgreSQL-compatible `length(bpchar)` implementation. PostgreSQL computes
/// the length after ignoring `bpchar` trailing ASCII padding spaces.
#[derive(Debug, Eq, Hash, PartialEq)]
pub struct PgBpcharLength {
    signature: Signature,
}

impl PgTextTypmod {
    fn new(name: &'static str, kind: TextTypmodKind) -> Self {
        Self {
            name,
            kind,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

pub fn pg_varchar_typmod_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(PgTextTypmod::new(
        "pg_fusion_varchar_typmod",
        TextTypmodKind::Varchar,
    )))
}

pub fn pg_bpchar_typmod_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(PgTextTypmod::new(
        "pg_fusion_bpchar_typmod",
        TextTypmodKind::Bpchar,
    )))
}

pub fn pg_bpchar_cmp_key_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(PgBpcharCmpKey {
        signature: Signature::user_defined(Volatility::Immutable),
    }))
}

pub fn pg_bpchar_length_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(PgBpcharLength {
        signature: Signature::user_defined(Volatility::Immutable),
    }))
}

impl ScalarUDFImpl for PgTextTypmod {
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
        validate_arg_types(self.name, arg_types)?;
        Ok(DataType::Utf8View)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        validate_arg_types(self.name, arg_types)?;
        Ok(vec![DataType::Utf8View, DataType::Int32])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return exec_err!("{} expects exactly two arguments", self.name);
        }

        if let [ColumnarValue::Scalar(value), ColumnarValue::Scalar(typmod)] = &args.args[..] {
            let value = scalar_text(value)?;
            let typmod = scalar_typmod(typmod)?;
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(value.map(
                |value| apply_text_typmod(self.kind, &value, typmod).into_owned(),
            ))));
        }

        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let value = &arrays[0];
        let typmod = &arrays[1];
        let mut builder = StringViewBuilder::new();
        for row in 0..value.len() {
            if value.is_null(row) {
                builder.append_null();
                continue;
            }
            let typmod = array_typmod(typmod, row)?;
            let value = array_text(value, row)?;
            builder.append_value(apply_text_typmod(self.kind, &value, typmod).as_ref());
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

impl ScalarUDFImpl for PgBpcharCmpKey {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "pg_fusion_bpchar_cmp_key"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        validate_unary_text_arg_types(self.name(), arg_types)?;
        Ok(DataType::Utf8View)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        validate_unary_text_arg_types(self.name(), arg_types)?;
        Ok(vec![DataType::Utf8View])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return exec_err!("{} expects exactly one argument", self.name());
        }

        if let [ColumnarValue::Scalar(value)] = &args.args[..] {
            let value = scalar_text(value)?;
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(
                value.map(|value| bpchar_cmp_key(&value).to_owned()),
            )));
        }

        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let value = &arrays[0];
        let mut builder = StringViewBuilder::new();
        for row in 0..value.len() {
            if value.is_null(row) {
                builder.append_null();
                continue;
            }
            let value = array_text(value, row)?;
            builder.append_value(bpchar_cmp_key(&value));
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

impl ScalarUDFImpl for PgBpcharLength {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "pg_fusion_bpchar_length"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        validate_unary_text_arg_types(self.name(), arg_types)?;
        Ok(DataType::Int32)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        validate_unary_text_arg_types(self.name(), arg_types)?;
        Ok(vec![DataType::Utf8View])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return exec_err!("{} expects exactly one argument", self.name());
        }

        if let [ColumnarValue::Scalar(value)] = &args.args[..] {
            let value = scalar_text(value)?;
            return Ok(ColumnarValue::Scalar(ScalarValue::Int32(
                value.map(|value| bpchar_length(&value)),
            )));
        }

        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let value = &arrays[0];
        let mut builder = Int32Builder::new();
        for row in 0..value.len() {
            if value.is_null(row) {
                builder.append_null();
                continue;
            }
            let value = array_text(value, row)?;
            builder.append_value(bpchar_length(&value));
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

fn validate_arg_types(name: &str, arg_types: &[DataType]) -> Result<()> {
    if arg_types.len() != 2 {
        return plan_err!("{name} expects exactly two arguments");
    }
    Ok(())
}

fn validate_unary_text_arg_types(name: &str, arg_types: &[DataType]) -> Result<()> {
    if arg_types.len() != 1 {
        return plan_err!("{name} expects exactly one argument");
    }
    Ok(())
}

fn scalar_text(value: &ScalarValue) -> Result<Option<String>> {
    match value {
        ScalarValue::Utf8(value) | ScalarValue::LargeUtf8(value) | ScalarValue::Utf8View(value) => {
            Ok(value.clone())
        }
        ScalarValue::Null => Ok(None),
        other => exec_err!("text typmod cast expected text argument after coercion, got {other:?}"),
    }
}

fn scalar_typmod(value: &ScalarValue) -> Result<i32> {
    match value {
        ScalarValue::Int32(Some(value)) => Ok(*value),
        other => exec_err!("text typmod cast expected non-null int4 typmod, got {other:?}"),
    }
}

fn array_text(array: &ArrayRef, row: usize) -> Result<String> {
    match array.data_type() {
        DataType::Utf8 => {
            let array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution(
                        "text typmod cast expected Utf8 array".into(),
                    )
                })?;
            Ok(array.value(row).to_owned())
        }
        DataType::Utf8View => {
            let array = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution(
                        "text typmod cast expected Utf8View array".into(),
                    )
                })?;
            Ok(array.value(row).to_owned())
        }
        DataType::LargeUtf8 => {
            let array = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution(
                        "text typmod cast expected LargeUtf8 array".into(),
                    )
                })?;
            Ok(array.value(row).to_owned())
        }
        other => exec_err!("text typmod cast expected text array after coercion, got {other:?}"),
    }
}

fn array_typmod(array: &ArrayRef, row: usize) -> Result<i32> {
    if array.is_null(row) {
        return exec_err!("text typmod cast expected non-null int4 typmod");
    }
    let array = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
        datafusion_common::DataFusionError::Execution(
            "text typmod cast expected Int32 typmod array".into(),
        )
    })?;
    Ok(array.value(row))
}

fn apply_text_typmod(kind: TextTypmodKind, value: &str, typmod: i32) -> Cow<'_, str> {
    let Some(length) = pg_type::text_typmod_length(typmod)
        .and_then(|length| usize::try_from(length).ok().filter(|length| *length > 0))
    else {
        return Cow::Borrowed(value);
    };

    match kind {
        TextTypmodKind::Varchar => truncate_chars(value, length),
        TextTypmodKind::Bpchar => bpchar_chars(value, length),
    }
}

fn truncate_chars(value: &str, length: usize) -> Cow<'_, str> {
    match value.char_indices().nth(length) {
        Some((byte_index, _)) => Cow::Owned(value[..byte_index].to_owned()),
        None => Cow::Borrowed(value),
    }
}

fn bpchar_chars(value: &str, length: usize) -> Cow<'_, str> {
    let mut output = String::new();
    let mut chars = value.chars();
    let mut copied = 0usize;
    for _ in 0..length {
        let Some(ch) = chars.next() else {
            break;
        };
        output.push(ch);
        copied += 1;
    }

    if copied == length {
        if chars.next().is_none() {
            Cow::Borrowed(value)
        } else {
            Cow::Owned(output)
        }
    } else {
        output.extend(std::iter::repeat_n(' ', length - copied));
        Cow::Owned(output)
    }
}

fn bpchar_cmp_key(value: &str) -> &str {
    value.trim_end_matches(' ')
}

fn bpchar_length(value: &str) -> i32 {
    i32::try_from(bpchar_cmp_key(value).chars().count()).unwrap_or(i32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, ArrayRef};
    use arrow_schema::Field;
    use datafusion_common::config::ConfigOptions;

    const VARCHAR_2_TYPMOD: i32 = pg_type::VARHDRSZ + 2;
    const BPCHAR_3_TYPMOD: i32 = pg_type::VARHDRSZ + 3;

    fn scalar_args(value: Option<&str>, typmod: i32) -> ScalarFunctionArgs {
        let value_field = Arc::new(Field::new("value", DataType::Utf8View, true));
        let typmod_field = Arc::new(Field::new("typmod", DataType::Int32, false));
        let return_field = Arc::new(Field::new("result", DataType::Utf8View, true));
        ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(value.map(str::to_owned))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(typmod))),
            ],
            arg_fields: vec![value_field, typmod_field],
            number_rows: 1,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    fn array_args(values: ArrayRef, typmods: ArrayRef) -> ScalarFunctionArgs {
        let value_field = Arc::new(Field::new("value", DataType::Utf8View, true));
        let typmod_field = Arc::new(Field::new("typmod", DataType::Int32, false));
        let return_field = Arc::new(Field::new("result", DataType::Utf8View, true));
        ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(values), ColumnarValue::Array(typmods)],
            arg_fields: vec![value_field, typmod_field],
            number_rows: 3,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    fn unary_text_scalar_args(value: Option<&str>, return_type: DataType) -> ScalarFunctionArgs {
        let value_field = Arc::new(Field::new("value", DataType::Utf8View, true));
        let return_field = Arc::new(Field::new("result", return_type, true));
        ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Utf8View(
                value.map(str::to_owned),
            ))],
            arg_fields: vec![value_field],
            number_rows: 1,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    fn unary_text_array_args(values: ArrayRef, return_type: DataType) -> ScalarFunctionArgs {
        let value_field = Arc::new(Field::new("value", DataType::Utf8View, true));
        let return_field = Arc::new(Field::new("result", return_type, true));
        ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(values)],
            arg_fields: vec![value_field],
            number_rows: 3,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    fn invoke_scalar(udf: &PgTextTypmod, value: Option<&str>, typmod: i32) -> Option<String> {
        let result = udf.invoke_with_args(scalar_args(value, typmod)).unwrap();
        let ColumnarValue::Scalar(ScalarValue::Utf8View(value)) = result else {
            panic!("scalar text typmod cast should return Utf8View scalar");
        };
        value
    }

    #[test]
    fn varchar_typmod_truncates_scalars_and_preserves_nulls() {
        let udf = PgTextTypmod::new("pg_fusion_varchar_typmod", TextTypmodKind::Varchar);

        assert_eq!(
            invoke_scalar(&udf, Some("abc"), VARCHAR_2_TYPMOD).as_deref(),
            Some("ab")
        );
        assert_eq!(
            invoke_scalar(&udf, Some("a"), VARCHAR_2_TYPMOD).as_deref(),
            Some("a")
        );
        assert_eq!(invoke_scalar(&udf, None, VARCHAR_2_TYPMOD), None);
    }

    #[test]
    fn varchar_typmod_truncates_at_utf8_char_boundaries() {
        let udf = PgTextTypmod::new("pg_fusion_varchar_typmod", TextTypmodKind::Varchar);

        assert_eq!(
            invoke_scalar(&udf, Some("åβc"), VARCHAR_2_TYPMOD).as_deref(),
            Some("åβ")
        );
    }

    #[test]
    fn bpchar_typmod_truncates_and_pads_scalars() {
        let udf = PgTextTypmod::new("pg_fusion_bpchar_typmod", TextTypmodKind::Bpchar);

        assert_eq!(
            invoke_scalar(&udf, Some("a"), BPCHAR_3_TYPMOD).as_deref(),
            Some("a  ")
        );
        assert_eq!(
            invoke_scalar(&udf, Some("abcd"), BPCHAR_3_TYPMOD).as_deref(),
            Some("abc")
        );
        assert_eq!(invoke_scalar(&udf, None, BPCHAR_3_TYPMOD), None);
    }

    #[test]
    fn text_typmod_casts_arrays() {
        let values =
            Arc::new(StringViewArray::from(vec![Some("abc"), Some("a"), None])) as ArrayRef;
        let typmods = Arc::new(Int32Array::from(vec![
            Some(VARCHAR_2_TYPMOD),
            Some(BPCHAR_3_TYPMOD),
            Some(VARCHAR_2_TYPMOD),
        ])) as ArrayRef;
        let varchar = PgTextTypmod::new("pg_fusion_varchar_typmod", TextTypmodKind::Varchar);
        let result = varchar
            .invoke_with_args(array_args(values, typmods))
            .unwrap();
        let ColumnarValue::Array(result) = result else {
            panic!("array text typmod cast should return array");
        };
        let result = result.as_any().downcast_ref::<StringViewArray>().unwrap();
        assert_eq!(result.value(0), "ab");
        assert_eq!(result.value(1), "a");
        assert!(result.is_null(2));
    }

    #[test]
    fn bpchar_cmp_key_trims_trailing_ascii_spaces() {
        let udf = PgBpcharCmpKey {
            signature: Signature::user_defined(Volatility::Immutable),
        };

        let result = udf
            .invoke_with_args(unary_text_scalar_args(Some("a  "), DataType::Utf8View))
            .unwrap();
        let ColumnarValue::Scalar(ScalarValue::Utf8View(value)) = result else {
            panic!("bpchar comparison key should return Utf8View scalar");
        };
        assert_eq!(value.as_deref(), Some("a"));

        let result = udf
            .invoke_with_args(unary_text_scalar_args(Some("a b  "), DataType::Utf8View))
            .unwrap();
        let ColumnarValue::Scalar(ScalarValue::Utf8View(value)) = result else {
            panic!("bpchar comparison key should return Utf8View scalar");
        };
        assert_eq!(value.as_deref(), Some("a b"));

        let result = udf
            .invoke_with_args(unary_text_scalar_args(None, DataType::Utf8View))
            .unwrap();
        let ColumnarValue::Scalar(ScalarValue::Utf8View(value)) = result else {
            panic!("bpchar comparison key should return Utf8View scalar");
        };
        assert_eq!(value, None);
    }

    #[test]
    fn bpchar_cmp_key_trims_arrays() {
        let values =
            Arc::new(StringViewArray::from(vec![Some("å  "), Some("a b "), None])) as ArrayRef;
        let udf = PgBpcharCmpKey {
            signature: Signature::user_defined(Volatility::Immutable),
        };

        let result = udf
            .invoke_with_args(unary_text_array_args(values, DataType::Utf8View))
            .unwrap();
        let ColumnarValue::Array(result) = result else {
            panic!("bpchar comparison key should return array");
        };
        let result = result.as_any().downcast_ref::<StringViewArray>().unwrap();
        assert_eq!(result.value(0), "å");
        assert_eq!(result.value(1), "a b");
        assert!(result.is_null(2));
    }

    #[test]
    fn bpchar_length_ignores_trailing_ascii_spaces() {
        let udf = PgBpcharLength {
            signature: Signature::user_defined(Volatility::Immutable),
        };

        let result = udf
            .invoke_with_args(unary_text_scalar_args(Some("a  "), DataType::Int32))
            .unwrap();
        let ColumnarValue::Scalar(ScalarValue::Int32(value)) = result else {
            panic!("bpchar length should return Int32 scalar");
        };
        assert_eq!(value, Some(1));

        let result = udf
            .invoke_with_args(unary_text_scalar_args(Some("å b  "), DataType::Int32))
            .unwrap();
        let ColumnarValue::Scalar(ScalarValue::Int32(value)) = result else {
            panic!("bpchar length should return Int32 scalar");
        };
        assert_eq!(value, Some(3));

        let result = udf
            .invoke_with_args(unary_text_scalar_args(None, DataType::Int32))
            .unwrap();
        let ColumnarValue::Scalar(ScalarValue::Int32(value)) = result else {
            panic!("bpchar length should return Int32 scalar");
        };
        assert_eq!(value, None);
    }

    #[test]
    fn bpchar_length_handles_arrays() {
        let values =
            Arc::new(StringViewArray::from(vec![Some("a  "), Some("å b "), None])) as ArrayRef;
        let udf = PgBpcharLength {
            signature: Signature::user_defined(Volatility::Immutable),
        };

        let result = udf
            .invoke_with_args(unary_text_array_args(values, DataType::Int32))
            .unwrap();
        let ColumnarValue::Array(result) = result else {
            panic!("bpchar length should return array");
        };
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(result.value(0), 1);
        assert_eq!(result.value(1), 3);
        assert!(result.is_null(2));
    }
}
