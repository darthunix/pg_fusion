use super::*;

pub(super) unsafe fn read_window_specs(
    window_clause: *mut pg_sys::List,
) -> Result<Vec<WindowSpec>, PgFrontendError> {
    let mut specs = Vec::new();
    for index in 0..unsafe { list_len(window_clause) } {
        let clause = unsafe { list_ptr_at(window_clause, index) as *mut pg_sys::WindowClause };
        if clause.is_null() {
            return Err(PgFrontendError::unsupported("null window clause"));
        }
        let clause_ref = unsafe { &*clause };
        specs.push(WindowSpec {
            ref_id: clause_ref.winref,
            partition_refs: unsafe { read_sort_group_refs(clause_ref.partitionClause) }?,
            order: unsafe { read_sort_clause(clause_ref.orderClause) }?,
            frame: unsafe { read_window_frame(clause_ref) }?,
        });
    }
    Ok(specs)
}

pub(super) unsafe fn read_window_frame(
    clause: &pg_sys::WindowClause,
) -> Result<WindowFrameSpec, PgFrontendError> {
    let options = clause.frameOptions as u32;
    if options & pg_sys::FRAMEOPTION_EXCLUSION != 0 {
        return Err(PgFrontendError::unsupported(
            "window frame exclusion is not supported by pg_frontend v1",
        ));
    }
    let units = if options & pg_sys::FRAMEOPTION_ROWS != 0 {
        WindowFrameUnits::Rows
    } else if options & pg_sys::FRAMEOPTION_GROUPS != 0 {
        WindowFrameUnits::Groups
    } else {
        WindowFrameUnits::Range
    };
    Ok(WindowFrameSpec {
        units,
        start: unsafe { window_frame_start_bound(options, clause.startOffset.cast(), units) }?,
        end: unsafe { window_frame_end_bound(options, clause.endOffset.cast(), units) }?,
    })
}

pub(super) unsafe fn window_frame_start_bound(
    options: u32,
    offset: *mut pg_sys::Node,
    units: WindowFrameUnits,
) -> Result<WindowFrameBound, PgFrontendError> {
    if options & pg_sys::FRAMEOPTION_START_UNBOUNDED_PRECEDING != 0 {
        Ok(WindowFrameBound::UnboundedPreceding)
    } else if options & pg_sys::FRAMEOPTION_START_CURRENT_ROW != 0 {
        Ok(WindowFrameBound::CurrentRow)
    } else if options & pg_sys::FRAMEOPTION_START_UNBOUNDED_FOLLOWING != 0 {
        Ok(WindowFrameBound::UnboundedFollowing)
    } else if options & pg_sys::FRAMEOPTION_START_OFFSET_PRECEDING != 0 {
        unsafe { read_window_frame_offset(offset, units) }.map(WindowFrameBound::Preceding)
    } else if options & pg_sys::FRAMEOPTION_START_OFFSET_FOLLOWING != 0 {
        unsafe { read_window_frame_offset(offset, units) }.map(WindowFrameBound::Following)
    } else {
        Err(PgFrontendError::unsupported(
            "window frame start bound is not supported by pg_frontend v1",
        ))
    }
}

pub(super) unsafe fn window_frame_end_bound(
    options: u32,
    offset: *mut pg_sys::Node,
    units: WindowFrameUnits,
) -> Result<WindowFrameBound, PgFrontendError> {
    if options & pg_sys::FRAMEOPTION_END_UNBOUNDED_FOLLOWING != 0 {
        Ok(WindowFrameBound::UnboundedFollowing)
    } else if options & pg_sys::FRAMEOPTION_END_CURRENT_ROW != 0 {
        Ok(WindowFrameBound::CurrentRow)
    } else if options & pg_sys::FRAMEOPTION_END_UNBOUNDED_PRECEDING != 0 {
        Ok(WindowFrameBound::UnboundedPreceding)
    } else if options & pg_sys::FRAMEOPTION_END_OFFSET_PRECEDING != 0 {
        unsafe { read_window_frame_offset(offset, units) }.map(WindowFrameBound::Preceding)
    } else if options & pg_sys::FRAMEOPTION_END_OFFSET_FOLLOWING != 0 {
        unsafe { read_window_frame_offset(offset, units) }.map(WindowFrameBound::Following)
    } else {
        Err(PgFrontendError::unsupported(
            "window frame end bound is not supported by pg_frontend v1",
        ))
    }
}

pub(super) unsafe fn read_window_frame_offset(
    offset: *mut pg_sys::Node,
    units: WindowFrameUnits,
) -> Result<ScalarValue, PgFrontendError> {
    if offset.is_null() {
        return Err(PgFrontendError::unsupported(
            "window frame offset expression is missing",
        ));
    }
    let expr = unsafe { read_expr(offset, &CteScope::default()) }?;
    match units {
        WindowFrameUnits::Rows | WindowFrameUnits::Groups => row_window_frame_offset(&expr),
        WindowFrameUnits::Range => range_window_frame_offset(&expr),
    }
}

pub(super) fn row_window_frame_offset(expr: &QueryExpr) -> Result<ScalarValue, PgFrontendError> {
    let constant = frame_offset_const(expr)?;
    let Some(value) = constant.value.as_ref() else {
        return Err(PgFrontendError::unsupported(
            "NULL window frame offsets are not supported by pg_frontend v1",
        ));
    };
    let value = match value {
        PgConstValue::Int16(value) => i64::from(*value),
        PgConstValue::Int32(value) => i64::from(*value),
        PgConstValue::Int64(value) => *value,
        _ => {
            return Err(PgFrontendError::unsupported(
                "ROWS/GROUPS window frame offsets must be integer constants",
            ))
        }
    };
    let value = u64::try_from(value)
        .map_err(|_| PgFrontendError::unsupported("window frame offsets must be non-negative"))?;
    Ok(ScalarValue::UInt64(Some(value)))
}

pub(super) fn range_window_frame_offset(expr: &QueryExpr) -> Result<ScalarValue, PgFrontendError> {
    let constant = frame_offset_const(expr)?;
    if window_frame_offset_is_negative(constant) {
        return Err(PgFrontendError::unsupported(
            "window frame offsets must be non-negative",
        ));
    }
    scalar_for_pg_const(constant.value.as_ref(), constant.pg_type)
        .map_err(|err| PgFrontendError::unsupported(err.to_string()))
}

pub(super) fn window_frame_offset_is_negative(constant: &Const) -> bool {
    match constant.value.as_ref() {
        Some(PgConstValue::Int16(value)) => *value < 0,
        Some(PgConstValue::Int32(value)) => *value < 0,
        Some(PgConstValue::Int64(value)) => *value < 0,
        Some(PgConstValue::Float32(value)) => value.is_sign_negative(),
        Some(PgConstValue::Float64(value)) => value.is_sign_negative(),
        Some(PgConstValue::Numeric(value)) => value.value < 0,
        _ => false,
    }
}

pub(super) fn frame_offset_const(expr: &QueryExpr) -> Result<&Const, PgFrontendError> {
    match expr {
        QueryExpr::Const(constant) => Ok(constant),
        QueryExpr::RelabelType(inner) | QueryExpr::Cast { arg: inner, .. } => {
            frame_offset_const(inner)
        }
        _ => Err(PgFrontendError::unsupported(
            "only constant window frame offsets are supported by pg_frontend v1",
        )),
    }
}
