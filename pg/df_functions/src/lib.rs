//! PostgreSQL-compatible DataFusion function definitions.

mod pg_avg;
mod pg_boolout;
mod pg_checked_int_arithmetic;
mod pg_format;
mod pg_interval_out;
mod pg_numeric_round_trunc;
mod pg_quote_literal;
mod pg_regex;
mod pg_scalar_subquery_value;
mod pg_text_typmod;

pub use pg_avg::{pg_avg_udaf, PgAvg};
pub use pg_boolout::{pg_boolout_udf, PgBoolOut};
pub use pg_checked_int_arithmetic::{
    pg_int_add_checked_udf, pg_int_mul_checked_udf, pg_int_sub_checked_udf, PgCheckedIntArithmetic,
};
pub use pg_format::{pg_format_udf, PgFormat};
pub use pg_interval_out::{pg_interval_out_udf, PgIntervalOut};
pub use pg_numeric_round_trunc::{
    pg_numeric_round_scale_udf, pg_numeric_trunc_scale_udf, PgNumericRoundTrunc,
};
pub use pg_quote_literal::{pg_quote_literal_udf, PgQuoteLiteral};
pub use pg_regex::{pg_regex_match_udf, pg_regex_not_match_udf, PgRegexMatch};
pub use pg_scalar_subquery_value::{pg_scalar_subquery_value_udaf, PgScalarSubqueryValue};
pub use pg_text_typmod::{
    pg_bpchar_cmp_key_udf, pg_bpchar_length_udf, pg_bpchar_typmod_udf, pg_varchar_typmod_udf,
    PgBpcharCmpKey, PgBpcharLength, PgTextTypmod,
};
