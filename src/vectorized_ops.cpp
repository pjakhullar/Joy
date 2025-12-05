#include "vectorized_ops.hpp"

#include <algorithm>

namespace joy {

// ============================================================================
// Vectorized GT Operations
// ============================================================================

SelectionVector vec_gt_int64(const Column& col, int64_t value) {
    const auto& data = std::get<std::vector<std::optional<int64_t>>>(col.data);
    SelectionVector result(data.size());

    for (size_t i = 0; i < data.size(); ++i) {
        result[i] = data[i].has_value() && data[i].value() > value;
    }

    return result;
}

SelectionVector vec_gt_double(const Column& col, double value) {
    const auto& data = std::get<std::vector<std::optional<double>>>(col.data);
    SelectionVector result(data.size());

    for (size_t i = 0; i < data.size(); ++i) {
        result[i] = data[i].has_value() && data[i].value() > value;
    }

    return result;
}

SelectionVector vec_gt_string(const Column& col, const std::string& value) {
    const auto& data = std::get<std::vector<std::optional<std::string>>>(col.data);
    SelectionVector result(data.size());

    for (size_t i = 0; i < data.size(); ++i) {
        result[i] = data[i].has_value() && data[i].value() > value;
    }

    return result;
}

// ============================================================================
// Vectorized LT Operations
// ============================================================================

SelectionVector vec_lt_int64(const Column& col, int64_t value) {
    const auto& data = std::get<std::vector<std::optional<int64_t>>>(col.data);
    SelectionVector result(data.size());

    for (size_t i = 0; i < data.size(); ++i) {
        result[i] = data[i].has_value() && data[i].value() < value;
    }

    return result;
}

SelectionVector vec_lt_double(const Column& col, double value) {
    const auto& data = std::get<std::vector<std::optional<double>>>(col.data);
    SelectionVector result(data.size());

    for (size_t i = 0; i < data.size(); ++i) {
        result[i] = data[i].has_value() && data[i].value() < value;
    }

    return result;
}

SelectionVector vec_lt_string(const Column& col, const std::string& value) {
    const auto& data = std::get<std::vector<std::optional<std::string>>>(col.data);
    SelectionVector result(data.size());

    for (size_t i = 0; i < data.size(); ++i) {
        result[i] = data[i].has_value() && data[i].value() < value;
    }

    return result;
}

// ============================================================================
// Vectorized GTE Operations
// ============================================================================

SelectionVector vec_gte_int64(const Column& col, int64_t value) {
    const auto& data = std::get<std::vector<std::optional<int64_t>>>(col.data);
    SelectionVector result(data.size());

    for (size_t i = 0; i < data.size(); ++i) {
        result[i] = data[i].has_value() && data[i].value() >= value;
    }

    return result;
}

SelectionVector vec_gte_double(const Column& col, double value) {
    const auto& data = std::get<std::vector<std::optional<double>>>(col.data);
    SelectionVector result(data.size());

    for (size_t i = 0; i < data.size(); ++i) {
        result[i] = data[i].has_value() && data[i].value() >= value;
    }

    return result;
}

SelectionVector vec_gte_string(const Column& col, const std::string& value) {
    const auto& data = std::get<std::vector<std::optional<std::string>>>(col.data);
    SelectionVector result(data.size());

    for (size_t i = 0; i < data.size(); ++i) {
        result[i] = data[i].has_value() && data[i].value() >= value;
    }

    return result;
}

// ============================================================================
// Vectorized LTE Operations
// ============================================================================

SelectionVector vec_lte_int64(const Column& col, int64_t value) {
    const auto& data = std::get<std::vector<std::optional<int64_t>>>(col.data);
    SelectionVector result(data.size());

    for (size_t i = 0; i < data.size(); ++i) {
        result[i] = data[i].has_value() && data[i].value() <= value;
    }

    return result;
}

SelectionVector vec_lte_double(const Column& col, double value) {
    const auto& data = std::get<std::vector<std::optional<double>>>(col.data);
    SelectionVector result(data.size());

    for (size_t i = 0; i < data.size(); ++i) {
        result[i] = data[i].has_value() && data[i].value() <= value;
    }

    return result;
}

SelectionVector vec_lte_string(const Column& col, const std::string& value) {
    const auto& data = std::get<std::vector<std::optional<std::string>>>(col.data);
    SelectionVector result(data.size());

    for (size_t i = 0; i < data.size(); ++i) {
        result[i] = data[i].has_value() && data[i].value() <= value;
    }

    return result;
}

// ============================================================================
// Vectorized EQ Operations
// ============================================================================

SelectionVector vec_eq_int64(const Column& col, int64_t value) {
    const auto& data = std::get<std::vector<std::optional<int64_t>>>(col.data);
    SelectionVector result(data.size());

    for (size_t i = 0; i < data.size(); ++i) {
        result[i] = data[i].has_value() && data[i].value() == value;
    }

    return result;
}

SelectionVector vec_eq_double(const Column& col, double value) {
    const auto& data = std::get<std::vector<std::optional<double>>>(col.data);
    SelectionVector result(data.size());

    for (size_t i = 0; i < data.size(); ++i) {
        result[i] = data[i].has_value() && data[i].value() == value;
    }

    return result;
}

SelectionVector vec_eq_string(const Column& col, const std::string& value) {
    const auto& data = std::get<std::vector<std::optional<std::string>>>(col.data);
    SelectionVector result(data.size());

    for (size_t i = 0; i < data.size(); ++i) {
        result[i] = data[i].has_value() && data[i].value() == value;
    }

    return result;
}

// ============================================================================
// Vectorized NEQ Operations
// ============================================================================

SelectionVector vec_neq_int64(const Column& col, int64_t value) {
    const auto& data = std::get<std::vector<std::optional<int64_t>>>(col.data);
    SelectionVector result(data.size());

    for (size_t i = 0; i < data.size(); ++i) {
        result[i] = data[i].has_value() && data[i].value() != value;
    }

    return result;
}

SelectionVector vec_neq_double(const Column& col, double value) {
    const auto& data = std::get<std::vector<std::optional<double>>>(col.data);
    SelectionVector result(data.size());

    for (size_t i = 0; i < data.size(); ++i) {
        result[i] = data[i].has_value() && data[i].value() != value;
    }

    return result;
}

SelectionVector vec_neq_string(const Column& col, const std::string& value) {
    const auto& data = std::get<std::vector<std::optional<std::string>>>(col.data);
    SelectionVector result(data.size());

    for (size_t i = 0; i < data.size(); ++i) {
        result[i] = data[i].has_value() && data[i].value() != value;
    }

    return result;
}

// ============================================================================
// Vectorized Arithmetic Operations
// ============================================================================
// These create new columns with computed values
// NULL propagation: NULL in any operand produces NULL in result

// Helper: Apply binary operation to two values
template <typename T>
std::optional<T> apply_arith_op(VectorArithOp op, std::optional<T> left, std::optional<T> right) {
    // NULL propagation
    if (!left.has_value() || !right.has_value()) {
        return std::nullopt;
    }

    T l = left.value();
    T r = right.value();

    switch (op) {
    case VectorArithOp::ADD:
        return l + r;
    case VectorArithOp::SUB:
        return l - r;
    case VectorArithOp::MUL:
        return l * r;
    case VectorArithOp::DIV:
        // Division by zero returns NULL (SQL semantics)
        if (r == 0) {
            return std::nullopt;
        }
        return l / r;
    }
    return std::nullopt;  // Unreachable
}

// Column op Column (INT64)
Column vec_arith_int64(VectorArithOp op, const Column& left, const Column& right) {
    const auto& left_data = std::get<std::vector<std::optional<int64_t>>>(left.data);
    const auto& right_data = std::get<std::vector<std::optional<int64_t>>>(right.data);

    Column result;
    result.type = ColumnType::INT64;
    result.data = std::vector<std::optional<int64_t>>();
    auto& result_data = std::get<std::vector<std::optional<int64_t>>>(result.data);
    result_data.reserve(left_data.size());

    for (size_t i = 0; i < left_data.size(); ++i) {
        result_data.push_back(apply_arith_op(op, left_data[i], right_data[i]));
    }

    return result;
}

// Column op Column (DOUBLE)
Column vec_arith_double(VectorArithOp op, const Column& left, const Column& right) {
    const auto& left_data = std::get<std::vector<std::optional<double>>>(left.data);
    const auto& right_data = std::get<std::vector<std::optional<double>>>(right.data);

    Column result;
    result.type = ColumnType::DOUBLE;
    result.data = std::vector<std::optional<double>>();
    auto& result_data = std::get<std::vector<std::optional<double>>>(result.data);
    result_data.reserve(left_data.size());

    for (size_t i = 0; i < left_data.size(); ++i) {
        result_data.push_back(apply_arith_op(op, left_data[i], right_data[i]));
    }

    return result;
}

// Column op Scalar (INT64)
Column vec_arith_int64_scalar(VectorArithOp op, const Column& col, int64_t scalar) {
    const auto& col_data = std::get<std::vector<std::optional<int64_t>>>(col.data);

    Column result;
    result.type = ColumnType::INT64;
    result.data = std::vector<std::optional<int64_t>>();
    auto& result_data = std::get<std::vector<std::optional<int64_t>>>(result.data);
    result_data.reserve(col_data.size());

    for (size_t i = 0; i < col_data.size(); ++i) {
        result_data.push_back(apply_arith_op(op, col_data[i], std::optional<int64_t>(scalar)));
    }

    return result;
}

// Column op Scalar (DOUBLE)
Column vec_arith_double_scalar(VectorArithOp op, const Column& col, double scalar) {
    const auto& col_data = std::get<std::vector<std::optional<double>>>(col.data);

    Column result;
    result.type = ColumnType::DOUBLE;
    result.data = std::vector<std::optional<double>>();
    auto& result_data = std::get<std::vector<std::optional<double>>>(result.data);
    result_data.reserve(col_data.size());

    for (size_t i = 0; i < col_data.size(); ++i) {
        result_data.push_back(apply_arith_op(op, col_data[i], std::optional<double>(scalar)));
    }

    return result;
}

// Scalar op Column (INT64)
Column vec_arith_scalar_int64(VectorArithOp op, int64_t scalar, const Column& col) {
    const auto& col_data = std::get<std::vector<std::optional<int64_t>>>(col.data);

    Column result;
    result.type = ColumnType::INT64;
    result.data = std::vector<std::optional<int64_t>>();
    auto& result_data = std::get<std::vector<std::optional<int64_t>>>(result.data);
    result_data.reserve(col_data.size());

    for (size_t i = 0; i < col_data.size(); ++i) {
        result_data.push_back(apply_arith_op(op, std::optional<int64_t>(scalar), col_data[i]));
    }

    return result;
}

// Scalar op Column (DOUBLE)
Column vec_arith_scalar_double(VectorArithOp op, double scalar, const Column& col) {
    const auto& col_data = std::get<std::vector<std::optional<double>>>(col.data);

    Column result;
    result.type = ColumnType::DOUBLE;
    result.data = std::vector<std::optional<double>>();
    auto& result_data = std::get<std::vector<std::optional<double>>>(result.data);
    result_data.reserve(col_data.size());

    for (size_t i = 0; i < col_data.size(); ++i) {
        result_data.push_back(apply_arith_op(op, std::optional<double>(scalar), col_data[i]));
    }

    return result;
}

// ============================================================================
// Vectorized Select/Blend Operations (for ternary)
// ============================================================================

Column vec_select_int64(const SelectionVector& condition, const Column& true_col,
                        const Column& false_col) {
    const auto& true_data = std::get<std::vector<std::optional<int64_t>>>(true_col.data);
    const auto& false_data = std::get<std::vector<std::optional<int64_t>>>(false_col.data);

    Column result;
    result.type = ColumnType::INT64;
    result.data = std::vector<std::optional<int64_t>>();
    auto& result_data = std::get<std::vector<std::optional<int64_t>>>(result.data);
    result_data.reserve(condition.size());

    for (size_t i = 0; i < condition.size(); ++i) {
        result_data.push_back(condition[i] ? true_data[i] : false_data[i]);
    }

    return result;
}

Column vec_select_double(const SelectionVector& condition, const Column& true_col,
                         const Column& false_col) {
    const auto& true_data = std::get<std::vector<std::optional<double>>>(true_col.data);
    const auto& false_data = std::get<std::vector<std::optional<double>>>(false_col.data);

    Column result;
    result.type = ColumnType::DOUBLE;
    result.data = std::vector<std::optional<double>>();
    auto& result_data = std::get<std::vector<std::optional<double>>>(result.data);
    result_data.reserve(condition.size());

    for (size_t i = 0; i < condition.size(); ++i) {
        result_data.push_back(condition[i] ? true_data[i] : false_data[i]);
    }

    return result;
}

Column vec_select_string(const SelectionVector& condition, const Column& true_col,
                         const Column& false_col) {
    const auto& true_data = std::get<std::vector<std::optional<std::string>>>(true_col.data);
    const auto& false_data = std::get<std::vector<std::optional<std::string>>>(false_col.data);

    Column result;
    result.type = ColumnType::STRING;
    result.data = std::vector<std::optional<std::string>>();
    auto& result_data = std::get<std::vector<std::optional<std::string>>>(result.data);
    result_data.reserve(condition.size());

    for (size_t i = 0; i < condition.size(); ++i) {
        result_data.push_back(condition[i] ? true_data[i] : false_data[i]);
    }

    return result;
}

}  // namespace joy
