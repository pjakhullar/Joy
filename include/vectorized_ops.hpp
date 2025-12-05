#pragma once

#include <cstdint>
#include <vector>

#include "table.hpp"

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

// ============================================================================
// Vectorized Arithmetic Operations (for TRANSFORM)
// ============================================================================
// These operations produce new columns (not selection vectors)
// Used for simple transform expressions like: transform total = price * quantity

enum class VectorArithOp { ADD, SUB, MUL, DIV };

// Column op Column
Column vec_arith_int64(VectorArithOp op, const Column& left, const Column& right);
Column vec_arith_double(VectorArithOp op, const Column& left, const Column& right);

// Column op Scalar
Column vec_arith_int64_scalar(VectorArithOp op, const Column& col, int64_t scalar);
Column vec_arith_double_scalar(VectorArithOp op, const Column& col, double scalar);

// Scalar op Column
Column vec_arith_scalar_int64(VectorArithOp op, int64_t scalar, const Column& col);
Column vec_arith_scalar_double(VectorArithOp op, double scalar, const Column& col);

// ============================================================================
// Vectorized Select/Blend (for ternary operator)
// ============================================================================
// Implements: condition ? true_val : false_val (vectorized)
// selection[i] == true  → result[i] = true_col[i]
// selection[i] == false → result[i] = false_col[i]

Column vec_select_int64(const SelectionVector& condition, const Column& true_col,
                        const Column& false_col);
Column vec_select_double(const SelectionVector& condition, const Column& true_col,
                         const Column& false_col);
Column vec_select_string(const SelectionVector& condition, const Column& true_col,
                         const Column& false_col);

}  // namespace joy
