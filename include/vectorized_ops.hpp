#pragma once

#include "table.hpp"
#include <vector>
#include <cstdint>

namespace joy {

// ============================================================================
// Vectorized Column Operations
// ============================================================================
// Process entire columns at once instead of row-at-a-time
// Compiler auto-vectorization applied where possible
// NULL values return false for all comparisons

// Selection vector indicating which rows pass the filter
using SelectionVector = std::vector<bool>;

// ============================================================================
// Column > Scalar
// ============================================================================

SelectionVector vec_gt_int64(const Column& col, int64_t value);
SelectionVector vec_gt_double(const Column& col, double value);
SelectionVector vec_gt_string(const Column& col, const std::string& value);

// ============================================================================
// Column < Scalar
// ============================================================================

SelectionVector vec_lt_int64(const Column& col, int64_t value);
SelectionVector vec_lt_double(const Column& col, double value);
SelectionVector vec_lt_string(const Column& col, const std::string& value);

// ============================================================================
// Column >= Scalar
// ============================================================================

SelectionVector vec_gte_int64(const Column& col, int64_t value);
SelectionVector vec_gte_double(const Column& col, double value);
SelectionVector vec_gte_string(const Column& col, const std::string& value);

// ============================================================================
// Column <= Scalar
// ============================================================================

SelectionVector vec_lte_int64(const Column& col, int64_t value);
SelectionVector vec_lte_double(const Column& col, double value);
SelectionVector vec_lte_string(const Column& col, const std::string& value);

// ============================================================================
// Column == Scalar
// ============================================================================

SelectionVector vec_eq_int64(const Column& col, int64_t value);
SelectionVector vec_eq_double(const Column& col, double value);
SelectionVector vec_eq_string(const Column& col, const std::string& value);

// ============================================================================
// Column != Scalar
// ============================================================================

SelectionVector vec_neq_int64(const Column& col, int64_t value);
SelectionVector vec_neq_double(const Column& col, double value);
SelectionVector vec_neq_string(const Column& col, const std::string& value);

} // namespace joy
