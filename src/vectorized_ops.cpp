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

} // namespace joy
