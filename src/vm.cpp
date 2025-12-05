#include "vm.hpp"

#include <iostream>
#include <variant>

#include "vectorized_ops.hpp"

namespace joy {

// ============================================================================
// Value Implementation - Runtime Value Representation
// ============================================================================
// Value is a type-erased runtime value used during bytecode execution
// Uses std::variant to hold one of five possible types (including NULL)
// This is similar to Python's PyObject or JavaScript's JSValue
// The VM stack contains these Values during expression evaluation
//
// NULL Semantics (SQL-style):
//   - Empty CSV cells are stored as NULL
//   - NULL propagates through operations: NULL + 5 = NULL, NULL > 10 = NULL
//   - Comparisons with NULL always return false (except IS NULL checks)

// Type checking methods - safely check what type is currently held
// Uses std::holds_alternative to query the variant without extracting

bool Value::is_null() const {
    return std::holds_alternative<std::monostate>(data);
}

bool Value::is_int() const {
    return std::holds_alternative<int64_t>(data);
}

bool Value::is_double() const {
    return std::holds_alternative<double>(data);
}

bool Value::is_string() const {
    return std::holds_alternative<std::string>(data);
}

bool Value::is_bool() const {
    return std::holds_alternative<bool>(data);
}

// Type extraction methods - get the actual value
// Throws std::bad_variant_access if wrong type is accessed
// Always check is_*() before calling as_*() to be safe

int64_t Value::as_int() const {
    return std::get<int64_t>(data);
}

double Value::as_double() const {
    return std::get<double>(data);
}

const std::string& Value::as_string() const {
    return std::get<std::string>(data);
}

bool Value::as_bool() const {
    return std::get<bool>(data);
}

// Factory methods - construct Values of specific types
// These are static methods for convenient Value creation

Value Value::make_null() {
    return Value{std::monostate{}};
}

Value Value::make_int(int64_t val) {
    return Value{val};
}

Value Value::make_double(double val) {
    return Value{val};
}

Value Value::make_string(std::string val) {
    return Value{std::move(val)};  // Move to avoid copy
}

Value Value::make_bool(bool val) {
    return Value{val};
}

// ============================================================================
// VM Implementation - The Execution Engine
// ============================================================================
// The VM executes a pipeline of physical operators sequentially
// Each operator transforms current_table_ and passes it to the next operator
// This is a "pipeline" execution model (like Unix pipes)
//
// Execution flow:
//   SCAN → loads data into current_table_
//   FILTER → evaluates predicate, keeps matching rows
//   PROJECT → selects subset of columns
//   WRITE → outputs current_table_ to file

// Main execution entry point
// Executes operators sequentially - each operator modifies current_table_
// Pipeline model: data flows through operators like water through pipes
void VM::execute(const ExecutionPlan& plan) {
    for (const auto& op : plan.operators) {
        // Pattern match on operator type and dispatch to appropriate handler
        std::visit(
            [this](const auto& op_data) {
                using T = std::decay_t<decltype(op_data)>;

                if constexpr (std::is_same_v<T, PhysicalOp::ScanOp>) {
                    execute_scan(op_data);
                } else if constexpr (std::is_same_v<T, PhysicalOp::FilterOp>) {
                    execute_filter(op_data);
                } else if constexpr (std::is_same_v<T, PhysicalOp::VectorizedFilterOp>) {
                    execute_vectorized_filter(op_data);
                } else if constexpr (std::is_same_v<T, PhysicalOp::ProjectOp>) {
                    execute_project(op_data);
                } else if constexpr (std::is_same_v<T, PhysicalOp::TransformOp>) {
                    execute_transform(op_data);
                } else if constexpr (std::is_same_v<T, PhysicalOp::VectorizedTransformOp>) {
                    execute_vectorized_transform(op_data);
                } else if constexpr (std::is_same_v<T, PhysicalOp::VectorizedTernaryTransformOp>) {
                    execute_vectorized_ternary_transform(op_data);
                } else if constexpr (std::is_same_v<T, PhysicalOp::WriteOp>) {
                    execute_write(op_data);
                }
            },
            op.data);
    }
}

// ============================================================================
// Physical Operator Implementations
// ============================================================================
// Each operator takes current_table_ as input and produces new current_table_

// SCAN operator: Load CSV file into current_table_
// This is the data source - first operator in every pipeline
// Example: from "employees.csv"
void VM::execute_scan(const PhysicalOp::ScanOp& op) {
    current_table_ = read_csv(op.filepath);
}

// FILTER operator: Keep only rows where predicate evaluates to true
// This is the most complex operator - it evaluates bytecode per row
// Example: filter age > 30
//
// Strategy:
//   1. Create empty result table with same column structure
//   2. For each row in input:
//      a. Evaluate predicate bytecode for this row
//      b. If true, copy row to result
//   3. Replace current_table_ with result
void VM::execute_filter(const PhysicalOp::FilterOp& op) {
    Table result;

    // Step 1: Create result table with same column schema (but empty data)
    // Result columns support NULL values (std::optional)
    for (const auto& col : current_table_.columns) {
        Column new_col;
        new_col.name = col.name;
        new_col.type = col.type;

        // Initialize empty vector of the appropriate optional type
        // We don't know final size yet, so start empty
        switch (col.type) {
        case ColumnType::INT64:
            new_col.data = std::vector<std::optional<int64_t>>{};
            break;
        case ColumnType::DOUBLE:
            new_col.data = std::vector<std::optional<double>>{};
            break;
        case ColumnType::STRING:
            new_col.data = std::vector<std::optional<std::string>>{};
            break;
        case ColumnType::BOOL:
            new_col.data = std::vector<std::optional<bool>>{};
            break;
        }

        result.add_column(std::move(new_col));
    }

    // Step 2: Evaluate predicate for each row
    // This is ROW-AT-A-TIME execution (not vectorized yet)
    // Future optimization: evaluate in batches of 1000 rows
    for (size_t row = 0; row < current_table_.num_rows; ++row) {
        // Evaluate the predicate bytecode for this specific row
        // eval_expr is the stack-based bytecode interpreter
        Value predicate_result = eval_expr(op.predicate, row);

        // Convert result to boolean
        // SQL NULL semantics: NULL in filter predicate is treated as false
        // Allows both bool and int (0 = false, non-zero = true)
        bool keep_row = false;
        if (predicate_result.is_null()) {
            keep_row = false;  // NULL predicate = false (row not included)
        } else if (predicate_result.is_bool()) {
            keep_row = predicate_result.as_bool();
        } else if (predicate_result.is_int()) {
            keep_row = predicate_result.as_int() != 0;
        } else {
            throw RuntimeError("Filter predicate must return boolean");
        }

        // Step 2b: If predicate is true, copy this row to result
        if (keep_row) {
            // Copy each column's value for this row (handle NULL values)
            for (size_t col_idx = 0; col_idx < current_table_.columns.size(); ++col_idx) {
                const Column& src_col = current_table_.columns[col_idx];
                Column& dst_col = result.columns[col_idx];

                // Check if value is NULL
                if (src_col.is_null(row)) {
                    // Copy NULL value
                    switch (src_col.type) {
                    case ColumnType::INT64:
                        dst_col.append_int(std::nullopt);
                        break;
                    case ColumnType::DOUBLE:
                        dst_col.append_double(std::nullopt);
                        break;
                    case ColumnType::STRING:
                        dst_col.append_string(std::nullopt);
                        break;
                    case ColumnType::BOOL:
                        dst_col.append_bool(std::nullopt);
                        break;
                    }
                } else {
                    // Copy non-NULL value
                    switch (src_col.type) {
                    case ColumnType::INT64:
                        dst_col.append_int(src_col.get_int(row));
                        break;
                    case ColumnType::DOUBLE:
                        dst_col.append_double(src_col.get_double(row));
                        break;
                    case ColumnType::STRING:
                        dst_col.append_string(src_col.get_string(row));
                        break;
                    case ColumnType::BOOL:
                        dst_col.append_bool(src_col.get_bool(row));
                        break;
                    }
                }
            }
            result.num_rows++;
        }
    }

    // Step 3: Replace current table with filtered result
    current_table_ = std::move(result);
}

// VECTORIZED_FILTER operator: Filter using column-at-a-time operations
// Much faster than row-at-a-time for simple comparisons
// Example: filter age > 30 → processes entire age column at once
void VM::execute_vectorized_filter(const PhysicalOp::VectorizedFilterOp& op) {
    // Find the column
    const Column* col = current_table_.get_column(op.column_name);
    if (!col) {
        throw RuntimeError("Column not found: " + op.column_name);
    }

    // Handle empty tables - nothing to filter
    if (current_table_.num_rows == 0) {
        return;  // Table is already empty, nothing to do
    }

    // Call appropriate vectorized comparison based on column type and operation
    SelectionVector selection;

    // Dispatch to type-specific vectorized operations
    // Handle numeric type promotion: INT64 <-> DOUBLE
    if (col->type == ColumnType::INT64) {
        if (std::holds_alternative<int64_t>(op.value)) {
            // INT64 column + INT64 literal (exact match)
            int64_t value = std::get<int64_t>(op.value);
            switch (op.op) {
            case VectorOp::GT:
                selection = vec_gt_int64(*col, value);
                break;
            case VectorOp::LT:
                selection = vec_lt_int64(*col, value);
                break;
            case VectorOp::GTE:
                selection = vec_gte_int64(*col, value);
                break;
            case VectorOp::LTE:
                selection = vec_lte_int64(*col, value);
                break;
            case VectorOp::EQ:
                selection = vec_eq_int64(*col, value);
                break;
            case VectorOp::NEQ:
                selection = vec_neq_int64(*col, value);
                break;
            }
        } else if (std::holds_alternative<double>(op.value)) {
            // INT64 column + DOUBLE literal: Promote int column to double for comparison
            // This matches scalar execution behavior
            double value = std::get<double>(op.value);
            // Convert INT64 column to double on-the-fly for comparison
            const auto& int_data = std::get<std::vector<std::optional<int64_t>>>(col->data);
            selection.resize(int_data.size());
            for (size_t i = 0; i < int_data.size(); ++i) {
                if (!int_data[i].has_value()) {
                    selection[i] = false;
                    continue;
                }
                double promoted_val = static_cast<double>(int_data[i].value());
                switch (op.op) {
                case VectorOp::GT:
                    selection[i] = promoted_val > value;
                    break;
                case VectorOp::LT:
                    selection[i] = promoted_val < value;
                    break;
                case VectorOp::GTE:
                    selection[i] = promoted_val >= value;
                    break;
                case VectorOp::LTE:
                    selection[i] = promoted_val <= value;
                    break;
                case VectorOp::EQ:
                    selection[i] = promoted_val == value;
                    break;
                case VectorOp::NEQ:
                    selection[i] = promoted_val != value;
                    break;
                }
            }
        } else {
            throw RuntimeError("Type mismatch: INT64 column requires numeric value");
        }
    } else if (col->type == ColumnType::DOUBLE) {
        // DOUBLE column accepts both DOUBLE and INT64 literals
        double value;
        if (std::holds_alternative<double>(op.value)) {
            value = std::get<double>(op.value);
        } else if (std::holds_alternative<int64_t>(op.value)) {
            // Promote INT64 literal to DOUBLE
            value = static_cast<double>(std::get<int64_t>(op.value));
        } else {
            throw RuntimeError("Type mismatch: DOUBLE column requires numeric value");
        }

        switch (op.op) {
        case VectorOp::GT:
            selection = vec_gt_double(*col, value);
            break;
        case VectorOp::LT:
            selection = vec_lt_double(*col, value);
            break;
        case VectorOp::GTE:
            selection = vec_gte_double(*col, value);
            break;
        case VectorOp::LTE:
            selection = vec_lte_double(*col, value);
            break;
        case VectorOp::EQ:
            selection = vec_eq_double(*col, value);
            break;
        case VectorOp::NEQ:
            selection = vec_neq_double(*col, value);
            break;
        }
    } else if (col->type == ColumnType::STRING) {
        if (!std::holds_alternative<std::string>(op.value)) {
            throw RuntimeError("Type mismatch: column is STRING but value is not");
        }
        const std::string& value = std::get<std::string>(op.value);
        switch (op.op) {
        case VectorOp::GT:
            selection = vec_gt_string(*col, value);
            break;
        case VectorOp::LT:
            selection = vec_lt_string(*col, value);
            break;
        case VectorOp::GTE:
            selection = vec_gte_string(*col, value);
            break;
        case VectorOp::LTE:
            selection = vec_lte_string(*col, value);
            break;
        case VectorOp::EQ:
            selection = vec_eq_string(*col, value);
            break;
        case VectorOp::NEQ:
            selection = vec_neq_string(*col, value);
            break;
        }
    } else {
        throw RuntimeError("Unsupported column type for vectorized filter");
    }

    // Build result table with matching rows (same approach as scalar filter)
    Table result;

    // Create empty result columns with same schema
    for (const auto& src_col : current_table_.columns) {
        Column new_col;
        new_col.name = src_col.name;
        new_col.type = src_col.type;

        switch (src_col.type) {
        case ColumnType::INT64:
            new_col.data = std::vector<std::optional<int64_t>>{};
            break;
        case ColumnType::DOUBLE:
            new_col.data = std::vector<std::optional<double>>{};
            break;
        case ColumnType::STRING:
            new_col.data = std::vector<std::optional<std::string>>{};
            break;
        case ColumnType::BOOL:
            new_col.data = std::vector<std::optional<bool>>{};
            break;
        }

        result.add_column(std::move(new_col));
    }

    // Copy rows that passed the filter
    for (size_t row = 0; row < current_table_.num_rows; ++row) {
        if (selection[row]) {
            // Copy this row
            for (size_t col_idx = 0; col_idx < current_table_.columns.size(); ++col_idx) {
                const Column& src_col = current_table_.columns[col_idx];
                Column& dst_col = result.columns[col_idx];

                if (src_col.is_null(row)) {
                    switch (src_col.type) {
                    case ColumnType::INT64:
                        dst_col.append_int(std::nullopt);
                        break;
                    case ColumnType::DOUBLE:
                        dst_col.append_double(std::nullopt);
                        break;
                    case ColumnType::STRING:
                        dst_col.append_string(std::nullopt);
                        break;
                    case ColumnType::BOOL:
                        dst_col.append_bool(std::nullopt);
                        break;
                    }
                } else {
                    switch (src_col.type) {
                    case ColumnType::INT64:
                        dst_col.append_int(src_col.get_int(row));
                        break;
                    case ColumnType::DOUBLE:
                        dst_col.append_double(src_col.get_double(row));
                        break;
                    case ColumnType::STRING:
                        dst_col.append_string(src_col.get_string(row));
                        break;
                    case ColumnType::BOOL:
                        dst_col.append_bool(src_col.get_bool(row));
                        break;
                    }
                }
            }
            result.num_rows++;
        }
    }

    current_table_ = std::move(result);
}

// PROJECT operator: Select subset of columns
// Example: select name, age
// Delegates to table.project() which does the actual work
void VM::execute_project(const PhysicalOp::ProjectOp& op) {
    current_table_ = current_table_.project(op.columns);
}

// TRANSFORM operator: Add or update column with expression result
// Evaluates expression for each row and stores result in column
// Example: transform gain_class = gain > 1000 ? "high" : "low"
//
// Strategy:
//   1. Evaluate expression for first row to infer result type
//   2. Create column with inferred type
//   3. Evaluate and append for each row (including first)
//   4. Replace existing column or add new one
void VM::execute_transform(const PhysicalOp::TransformOp& op) {
    if (current_table_.num_rows == 0) {
        // Empty table - nothing to transform
        return;
    }

    // Step 1: Evaluate first row to infer result type
    Value first_val = eval_expr(op.expression, 0);

    // Find first non-NULL value if first is NULL
    Value type_sample = first_val;
    if (type_sample.is_null() && current_table_.num_rows > 1) {
        for (size_t i = 1; i < current_table_.num_rows; i++) {
            type_sample = eval_expr(op.expression, i);
            if (!type_sample.is_null()) {
                break;
            }
        }
    }

    // Infer column type from first non-NULL value
    ColumnType result_type;
    if (type_sample.is_int()) {
        result_type = ColumnType::INT64;
    } else if (type_sample.is_double()) {
        result_type = ColumnType::DOUBLE;
    } else if (type_sample.is_string()) {
        result_type = ColumnType::STRING;
    } else if (type_sample.is_bool()) {
        result_type = ColumnType::BOOL;
    } else {
        // All NULLs - default to STRING
        result_type = ColumnType::STRING;
    }

    // Step 2: Create new column
    Column new_col;
    new_col.name = op.column_name;
    new_col.type = result_type;
    new_col.reserve(current_table_.num_rows);

    // Initialize column data storage
    switch (result_type) {
    case ColumnType::INT64:
        new_col.data = std::vector<std::optional<int64_t>>();
        break;
    case ColumnType::DOUBLE:
        new_col.data = std::vector<std::optional<double>>();
        break;
    case ColumnType::STRING:
        new_col.data = std::vector<std::optional<std::string>>();
        break;
    case ColumnType::BOOL:
        new_col.data = std::vector<std::optional<bool>>();
        break;
    }

    // Step 3: Evaluate expression and populate column for each row
    for (size_t i = 0; i < current_table_.num_rows; i++) {
        // Reuse first_val for row 0 to avoid re-evaluation
        Value val = (i == 0) ? first_val : eval_expr(op.expression, i);

        if (val.is_null()) {
            // Append NULL
            switch (result_type) {
            case ColumnType::INT64:
                new_col.append_int(std::nullopt);
                break;
            case ColumnType::DOUBLE:
                new_col.append_double(std::nullopt);
                break;
            case ColumnType::STRING:
                new_col.append_string(std::nullopt);
                break;
            case ColumnType::BOOL:
                new_col.append_bool(std::nullopt);
                break;
            }
        } else {
            // Append value with type coercion
            switch (result_type) {
            case ColumnType::INT64:
                if (val.is_int()) {
                    new_col.append_int(val.as_int());
                } else if (val.is_double()) {
                    new_col.append_int(static_cast<int64_t>(val.as_double()));
                } else {
                    throw RuntimeError("Type mismatch in transform");
                }
                break;
            case ColumnType::DOUBLE:
                if (val.is_double()) {
                    new_col.append_double(val.as_double());
                } else if (val.is_int()) {
                    new_col.append_double(static_cast<double>(val.as_int()));
                } else {
                    throw RuntimeError("Type mismatch in transform");
                }
                break;
            case ColumnType::STRING:
                if (val.is_string()) {
                    new_col.append_string(val.as_string());
                } else {
                    throw RuntimeError("Type mismatch in transform");
                }
                break;
            case ColumnType::BOOL:
                if (val.is_bool()) {
                    new_col.append_bool(val.as_bool());
                } else {
                    throw RuntimeError("Type mismatch in transform");
                }
                break;
            }
        }
    }

    // Step 4: Replace existing column or add new one
    int col_idx = current_table_.get_column_index(op.column_name);
    if (col_idx >= 0) {
        current_table_.columns[col_idx] = std::move(new_col);
    } else {
        current_table_.add_column(std::move(new_col));
    }
}

// VECTORIZED_TRANSFORM operator: Vectorized arithmetic on columns (FAST!)
// Pattern: total = price * quantity, discounted = price * 0.9
// Processes entire columns at once instead of row-by-row
void VM::execute_vectorized_transform(const PhysicalOp::VectorizedTransformOp& op) {
    // Get operand columns/scalars
    const Column* left_col = nullptr;
    const Column* right_col = nullptr;

    if (op.is_left_column) {
        left_col = current_table_.get_column(op.left_column_name);
        if (!left_col)
            throw RuntimeError("Column not found: " + op.left_column_name);
    }

    if (op.is_right_column) {
        right_col = current_table_.get_column(op.right_column_name);
        if (!right_col)
            throw RuntimeError("Column not found: " + op.right_column_name);
    }

    // Determine actual result type (may need promotion based on column types)
    ColumnType actual_result_type = op.result_type;
    if (left_col && left_col->type == ColumnType::DOUBLE)
        actual_result_type = ColumnType::DOUBLE;
    if (right_col && right_col->type == ColumnType::DOUBLE)
        actual_result_type = ColumnType::DOUBLE;

    // Dispatch to appropriate vectorized operation
    // Note: Columns must match expected type or we'd need type coercion
    Column result;
    if (actual_result_type == ColumnType::INT64) {
        // All operands must be INT64
        bool types_match = true;
        if (left_col && left_col->type != ColumnType::INT64)
            types_match = false;
        if (right_col && right_col->type != ColumnType::INT64)
            types_match = false;

        if (!types_match) {
            throw RuntimeError("Type mismatch in vectorized transform - this shouldn't happen");
        }

        if (op.is_left_column && op.is_right_column) {
            result = vec_arith_int64(op.op, *left_col, *right_col);
        } else if (op.is_left_column && !op.is_right_column) {
            int64_t scalar = std::get<int64_t>(op.right_scalar);
            result = vec_arith_int64_scalar(op.op, *left_col, scalar);
        } else if (!op.is_left_column && op.is_right_column) {
            int64_t scalar = std::get<int64_t>(op.left_scalar);
            result = vec_arith_scalar_int64(op.op, scalar, *right_col);
        }
    } else {
        // DOUBLE result type
        // All operands must be DOUBLE (we promote INT64->DOUBLE if needed)
        if (op.is_left_column && op.is_right_column) {
            result = vec_arith_double(op.op, *left_col, *right_col);
        } else if (op.is_left_column && !op.is_right_column) {
            // Column op scalar - column MUST be DOUBLE for vectorized path
            if (left_col->type != ColumnType::DOUBLE) {
                throw RuntimeError(
                    "Cannot vectorize: INT64 column with DOUBLE scalar (needs type coercion)");
            }
            double scalar = std::holds_alternative<double>(op.right_scalar)
                                ? std::get<double>(op.right_scalar)
                                : static_cast<double>(std::get<int64_t>(op.right_scalar));
            result = vec_arith_double_scalar(op.op, *left_col, scalar);
        } else if (!op.is_left_column && op.is_right_column) {
            if (right_col->type != ColumnType::DOUBLE) {
                throw RuntimeError(
                    "Cannot vectorize: DOUBLE scalar with INT64 column (needs type coercion)");
            }
            double scalar = std::holds_alternative<double>(op.left_scalar)
                                ? std::get<double>(op.left_scalar)
                                : static_cast<double>(std::get<int64_t>(op.left_scalar));
            result = vec_arith_scalar_double(op.op, scalar, *right_col);
        }
    }

    result.name = op.column_name;

    // Replace or add column
    int col_idx = current_table_.get_column_index(op.column_name);
    if (col_idx >= 0) {
        current_table_.columns[col_idx] = std::move(result);
    } else {
        current_table_.add_column(std::move(result));
    }
}

// VECTORIZED_TERNARY_TRANSFORM operator: Vectorized conditional (FAST!)
// Pattern: class = score > 90 ? "A" : "B"
// Uses vectorized condition evaluation + blend operation
void VM::execute_vectorized_ternary_transform(const PhysicalOp::VectorizedTernaryTransformOp& op) {
    // Step 1: Evaluate condition vectorially (reuse vectorized filter logic)
    const Column* cond_col = current_table_.get_column(op.condition.column_name);
    if (!cond_col)
        throw RuntimeError("Column not found: " + op.condition.column_name);

    SelectionVector selection;
    // Call appropriate vectorized comparison based on column type and operator
    if (cond_col->type == ColumnType::INT64) {
        int64_t value = std::get<int64_t>(op.condition.value);
        switch (op.condition.op) {
        case VectorOp::GT:
            selection = vec_gt_int64(*cond_col, value);
            break;
        case VectorOp::LT:
            selection = vec_lt_int64(*cond_col, value);
            break;
        case VectorOp::GTE:
            selection = vec_gte_int64(*cond_col, value);
            break;
        case VectorOp::LTE:
            selection = vec_lte_int64(*cond_col, value);
            break;
        case VectorOp::EQ:
            selection = vec_eq_int64(*cond_col, value);
            break;
        case VectorOp::NEQ:
            selection = vec_neq_int64(*cond_col, value);
            break;
        }
    } else if (cond_col->type == ColumnType::DOUBLE) {
        double value = std::get<double>(op.condition.value);
        switch (op.condition.op) {
        case VectorOp::GT:
            selection = vec_gt_double(*cond_col, value);
            break;
        case VectorOp::LT:
            selection = vec_lt_double(*cond_col, value);
            break;
        case VectorOp::GTE:
            selection = vec_gte_double(*cond_col, value);
            break;
        case VectorOp::LTE:
            selection = vec_lte_double(*cond_col, value);
            break;
        case VectorOp::EQ:
            selection = vec_eq_double(*cond_col, value);
            break;
        case VectorOp::NEQ:
            selection = vec_neq_double(*cond_col, value);
            break;
        }
    } else if (cond_col->type == ColumnType::STRING) {
        std::string value = std::get<std::string>(op.condition.value);
        switch (op.condition.op) {
        case VectorOp::GT:
            selection = vec_gt_string(*cond_col, value);
            break;
        case VectorOp::LT:
            selection = vec_lt_string(*cond_col, value);
            break;
        case VectorOp::GTE:
            selection = vec_gte_string(*cond_col, value);
            break;
        case VectorOp::LTE:
            selection = vec_lte_string(*cond_col, value);
            break;
        case VectorOp::EQ:
            selection = vec_eq_string(*cond_col, value);
            break;
        case VectorOp::NEQ:
            selection = vec_neq_string(*cond_col, value);
            break;
        }
    }

    // Step 2: Materialize true/false columns
    Column true_col, false_col;
    size_t num_rows = current_table_.num_rows;

    // Helper to create constant column or get existing column
    auto make_column = [&](bool is_column, const std::string& col_name,
                           const std::variant<int64_t, double, std::string>& scalar,
                           ColumnType type) -> Column {
        if (is_column) {
            const Column* col = current_table_.get_column(col_name);
            if (!col)
                throw RuntimeError("Column not found: " + col_name);
            return *col;  // Copy
        } else {
            // Create constant column
            Column col;
            col.type = type;
            if (type == ColumnType::INT64) {
                col.data = std::vector<std::optional<int64_t>>(
                    num_rows, std::get<int64_t>(scalar));
            } else if (type == ColumnType::DOUBLE) {
                col.data = std::vector<std::optional<double>>(
                    num_rows, std::get<double>(scalar));
            } else if (type == ColumnType::STRING) {
                col.data = std::vector<std::optional<std::string>>(
                    num_rows, std::get<std::string>(scalar));
            }
            return col;
        }
    };

    true_col = make_column(op.is_true_column, op.true_column_name, op.true_scalar, op.result_type);
    false_col =
        make_column(op.is_false_column, op.false_column_name, op.false_scalar, op.result_type);

    // Step 3: Blend based on selection vector
    Column result;
    if (op.result_type == ColumnType::INT64) {
        result = vec_select_int64(selection, true_col, false_col);
    } else if (op.result_type == ColumnType::DOUBLE) {
        result = vec_select_double(selection, true_col, false_col);
    } else if (op.result_type == ColumnType::STRING) {
        result = vec_select_string(selection, true_col, false_col);
    }

    result.name = op.column_name;

    // Replace or add column
    int col_idx = current_table_.get_column_index(op.column_name);
    if (col_idx >= 0) {
        current_table_.columns[col_idx] = std::move(result);
    } else {
        current_table_.add_column(std::move(result));
    }
}

// WRITE operator: Output current table to CSV file
// This is the data sink - typically last operator in pipeline
// Example: write "output.csv"
void VM::execute_write(const PhysicalOp::WriteOp& op) {
    write_csv(op.filepath, current_table_);
}

// ============================================================================
// Expression Evaluator - Stack-Based Bytecode Interpreter
// ============================================================================
// This is the HEART of the VM - the bytecode execution engine
// Implements a stack machine (like JVM, Python bytecode, WebAssembly)
//
// Stack machine model:
//   - Values are pushed onto a stack
//   - Operators pop operands from stack and push result
//   - Final result is the single value left on stack
//
// Example: age + 5
//   Bytecode: [LOAD_COLUMN "age", PUSH_INT 5, ADD]
//   Execution:
//     Stack: []
//     LOAD_COLUMN "age" → Stack: [42]         (if age column has value 42)
//     PUSH_INT 5        → Stack: [42, 5]
//     ADD               → Stack: [47]         (pop 42 and 5, push 42+5)
//     Result: 47

// Evaluate expression bytecode for a specific row
// Returns the computed value (could be int, double, string, or bool)
// This method is called once per row during FILTER execution
Value VM::eval_expr(const IRExpr& expr, size_t row_idx) {
    stack_.clear();  // Start with empty stack for this expression

    // Execute each instruction in sequence
    // This is a classic "fetch-decode-execute" loop
    for (const auto& instr : expr.instructions) {
        switch (instr.op) {
            // ================================================================
            // PUSH Instructions - Put values onto stack
            // ================================================================
            // These are "leaf" operations - they don't consume stack values

        case IRExpr::OpCode::PUSH_INT:
            stack_.push_back(Value::make_int(std::get<int64_t>(instr.operand)));
            break;

        case IRExpr::OpCode::PUSH_DOUBLE:
            stack_.push_back(Value::make_double(std::get<double>(instr.operand)));
            break;

        case IRExpr::OpCode::PUSH_STRING:
            stack_.push_back(Value::make_string(std::get<std::string>(instr.operand)));
            break;

        case IRExpr::OpCode::PUSH_BOOL:
            stack_.push_back(Value::make_bool(std::get<bool>(instr.operand)));
            break;

            // ================================================================
            // LOAD_COLUMN - Load value from current row (NULL support)
            // ================================================================
            // Operand is column name (string)
            // We look up the column, get the value at row_idx, push it
            // Example: LOAD_COLUMN "age" when row_idx=2 loads age[2]
            // SQL NULL semantics: if cell is NULL, push NULL value

        case IRExpr::OpCode::LOAD_COLUMN: {
            std::string col_name = std::get<std::string>(instr.operand);
            const Column* col = current_table_.get_column(col_name);
            if (!col) {
                throw RuntimeError("Column not found: " + col_name);
            }

            // Check if value is NULL first
            if (col->is_null(row_idx)) {
                stack_.push_back(Value::make_null());
                break;
            }

            // Load value from column at current row index
            // Type depends on column type
            switch (col->type) {
            case ColumnType::INT64:
                stack_.push_back(Value::make_int(col->get_int(row_idx)));
                break;
            case ColumnType::DOUBLE:
                stack_.push_back(Value::make_double(col->get_double(row_idx)));
                break;
            case ColumnType::STRING:
                stack_.push_back(Value::make_string(col->get_string(row_idx)));
                break;
            case ColumnType::BOOL:
                stack_.push_back(Value::make_bool(col->get_bool(row_idx)));
                break;
            }
            break;
        }

            // ================================================================
            // Arithmetic Instructions - Binary Operations
            // ================================================================
            // All follow same pattern:
            //   1. Pop right operand (top of stack)
            //   2. Pop left operand (second from top)
            //   3. Compute result
            //   4. Push result
            // Note: Order matters! a - b is different from b - a

        case IRExpr::OpCode::ADD: {
            Value b = stack_.back();
            stack_.pop_back();  // Right operand
            Value a = stack_.back();
            stack_.pop_back();  // Left operand

            // SQL NULL semantics: NULL + anything = NULL
            if (a.is_null() || b.is_null()) {
                stack_.push_back(Value::make_null());
            }
            // Type coercion: int + int = int, anything with double = double
            else if (a.is_int() && b.is_int()) {
                stack_.push_back(Value::make_int(a.as_int() + b.as_int()));
            } else if ((a.is_int() || a.is_double()) && (b.is_int() || b.is_double())) {
                // Promote to double if either operand is double
                double a_val = a.is_double() ? a.as_double() : static_cast<double>(a.as_int());
                double b_val = b.is_double() ? b.as_double() : static_cast<double>(b.as_int());
                stack_.push_back(Value::make_double(a_val + b_val));
            } else {
                throw RuntimeError("Cannot add non-numeric types");
            }
            break;
        }

        case IRExpr::OpCode::SUB: {
            Value b = stack_.back();
            stack_.pop_back();
            Value a = stack_.back();
            stack_.pop_back();

            // SQL NULL semantics: NULL - anything = NULL
            if (a.is_null() || b.is_null()) {
                stack_.push_back(Value::make_null());
            } else if (a.is_int() && b.is_int()) {
                stack_.push_back(Value::make_int(a.as_int() - b.as_int()));
            } else if ((a.is_int() || a.is_double()) && (b.is_int() || b.is_double())) {
                double a_val = a.is_double() ? a.as_double() : static_cast<double>(a.as_int());
                double b_val = b.is_double() ? b.as_double() : static_cast<double>(b.as_int());
                stack_.push_back(Value::make_double(a_val - b_val));
            } else {
                throw RuntimeError("Cannot subtract non-numeric types");
            }
            break;
        }

        case IRExpr::OpCode::MUL: {
            Value b = stack_.back();
            stack_.pop_back();
            Value a = stack_.back();
            stack_.pop_back();

            // SQL NULL semantics: NULL * anything = NULL
            if (a.is_null() || b.is_null()) {
                stack_.push_back(Value::make_null());
            } else if (a.is_int() && b.is_int()) {
                stack_.push_back(Value::make_int(a.as_int() * b.as_int()));
            } else if ((a.is_int() || a.is_double()) && (b.is_int() || b.is_double())) {
                double a_val = a.is_double() ? a.as_double() : static_cast<double>(a.as_int());
                double b_val = b.is_double() ? b.as_double() : static_cast<double>(b.as_int());
                stack_.push_back(Value::make_double(a_val * b_val));
            } else {
                throw RuntimeError("Cannot multiply non-numeric types");
            }
            break;
        }

        case IRExpr::OpCode::DIV: {
            Value b = stack_.back();
            stack_.pop_back();
            Value a = stack_.back();
            stack_.pop_back();

            // SQL NULL semantics: NULL / anything = NULL
            if (a.is_null() || b.is_null()) {
                stack_.push_back(Value::make_null());
            }
            // Check for division by zero
            else if (a.is_int() && b.is_int()) {
                if (b.as_int() == 0)
                    throw RuntimeError("Division by zero");
                stack_.push_back(Value::make_int(a.as_int() / b.as_int()));
            } else if ((a.is_int() || a.is_double()) && (b.is_int() || b.is_double())) {
                double a_val = a.is_double() ? a.as_double() : static_cast<double>(a.as_int());
                double b_val = b.is_double() ? b.as_double() : static_cast<double>(b.as_int());
                if (b_val == 0.0)
                    throw RuntimeError("Division by zero");
                stack_.push_back(Value::make_double(a_val / b_val));
            } else {
                throw RuntimeError("Cannot divide non-numeric types");
            }
            break;
        }

            // ================================================================
            // Unary Arithmetic - Negation
            // ================================================================
            // Pops one value, pushes negated result

        case IRExpr::OpCode::NEG: {
            Value a = stack_.back();
            stack_.pop_back();

            // SQL NULL semantics: -NULL = NULL
            if (a.is_null()) {
                stack_.push_back(Value::make_null());
            } else if (a.is_int()) {
                stack_.push_back(Value::make_int(-a.as_int()));
            } else if (a.is_double()) {
                stack_.push_back(Value::make_double(-a.as_double()));
            } else {
                throw RuntimeError("Cannot negate non-numeric value");
            }
            break;
        }

            // ================================================================
            // Comparison Instructions - Always return bool
            // ================================================================
            // Compare two values and push boolean result
            // Support comparing ints, doubles, strings, bools
            // Type coercion: int can compare with double
            // SQL NULL semantics: NULL compared to anything returns false

        case IRExpr::OpCode::EQ: {
            Value b = stack_.back();
            stack_.pop_back();
            Value a = stack_.back();
            stack_.pop_back();

            // SQL NULL semantics: NULL == anything is false (use IS NULL for NULL checks)
            if (a.is_null() || b.is_null()) {
                stack_.push_back(Value::make_bool(false));
                break;
            }

            bool result = false;
            // int == int
            if (a.is_int() && b.is_int()) {
                result = a.as_int() == b.as_int();
            }
            // int == double or double == int or double == double
            else if ((a.is_int() || a.is_double()) && (b.is_int() || b.is_double())) {
                double a_val = a.is_double() ? a.as_double() : static_cast<double>(a.as_int());
                double b_val = b.is_double() ? b.as_double() : static_cast<double>(b.as_int());
                result = a_val == b_val;
            }
            // string == string
            else if (a.is_string() && b.is_string()) {
                result = a.as_string() == b.as_string();
            }
            // bool == bool
            else if (a.is_bool() && b.is_bool()) {
                result = a.as_bool() == b.as_bool();
            }
            // Different types → error (strict type checking)
            else {
                throw RuntimeError("Cannot compare incompatible types");
            }

            stack_.push_back(Value::make_bool(result));
            break;
        }

        case IRExpr::OpCode::NEQ: {
            Value b = stack_.back();
            stack_.pop_back();
            Value a = stack_.back();
            stack_.pop_back();

            // SQL NULL semantics: NULL != anything is false
            if (a.is_null() || b.is_null()) {
                stack_.push_back(Value::make_bool(false));
                break;
            }

            bool result = false;
            if (a.is_int() && b.is_int()) {
                result = a.as_int() != b.as_int();
            } else if ((a.is_int() || a.is_double()) && (b.is_int() || b.is_double())) {
                double a_val = a.is_double() ? a.as_double() : static_cast<double>(a.as_int());
                double b_val = b.is_double() ? b.as_double() : static_cast<double>(b.as_int());
                result = a_val != b_val;
            } else if (a.is_string() && b.is_string()) {
                result = a.as_string() != b.as_string();
            } else if (a.is_bool() && b.is_bool()) {
                result = a.as_bool() != b.as_bool();
            } else {
                throw RuntimeError("Cannot compare incompatible types");
            }

            stack_.push_back(Value::make_bool(result));
            break;
        }

        case IRExpr::OpCode::LT: {
            Value b = stack_.back();
            stack_.pop_back();
            Value a = stack_.back();
            stack_.pop_back();

            // SQL NULL semantics: NULL < anything is false
            if (a.is_null() || b.is_null()) {
                stack_.push_back(Value::make_bool(false));
                break;
            }

            bool result = false;
            if (a.is_int() && b.is_int()) {
                result = a.as_int() < b.as_int();
            } else if ((a.is_int() || a.is_double()) && (b.is_int() || b.is_double())) {
                double a_val = a.is_double() ? a.as_double() : static_cast<double>(a.as_int());
                double b_val = b.is_double() ? b.as_double() : static_cast<double>(b.as_int());
                result = a_val < b_val;
            } else if (a.is_string() && b.is_string()) {
                // Lexicographic string comparison
                result = a.as_string() < b.as_string();
            } else {
                throw RuntimeError("Cannot compare incompatible types");
            }

            stack_.push_back(Value::make_bool(result));
            break;
        }

        case IRExpr::OpCode::GT: {
            Value b = stack_.back();
            stack_.pop_back();
            Value a = stack_.back();
            stack_.pop_back();

            // SQL NULL semantics: NULL > anything is false
            if (a.is_null() || b.is_null()) {
                stack_.push_back(Value::make_bool(false));
                break;
            }

            bool result = false;
            if (a.is_int() && b.is_int()) {
                result = a.as_int() > b.as_int();
            } else if ((a.is_int() || a.is_double()) && (b.is_int() || b.is_double())) {
                double a_val = a.is_double() ? a.as_double() : static_cast<double>(a.as_int());
                double b_val = b.is_double() ? b.as_double() : static_cast<double>(b.as_int());
                result = a_val > b_val;
            } else if (a.is_string() && b.is_string()) {
                result = a.as_string() > b.as_string();
            } else {
                throw RuntimeError("Cannot compare incompatible types");
            }

            stack_.push_back(Value::make_bool(result));
            break;
        }

        case IRExpr::OpCode::LTE: {
            Value b = stack_.back();
            stack_.pop_back();
            Value a = stack_.back();
            stack_.pop_back();

            // SQL NULL semantics: NULL <= anything is false
            if (a.is_null() || b.is_null()) {
                stack_.push_back(Value::make_bool(false));
                break;
            }

            bool result = false;
            if (a.is_int() && b.is_int()) {
                result = a.as_int() <= b.as_int();
            } else if ((a.is_int() || a.is_double()) && (b.is_int() || b.is_double())) {
                double a_val = a.is_double() ? a.as_double() : static_cast<double>(a.as_int());
                double b_val = b.is_double() ? b.as_double() : static_cast<double>(b.as_int());
                result = a_val <= b_val;
            } else if (a.is_string() && b.is_string()) {
                result = a.as_string() <= b.as_string();
            } else {
                throw RuntimeError("Cannot compare incompatible types");
            }

            stack_.push_back(Value::make_bool(result));
            break;
        }

        case IRExpr::OpCode::GTE: {
            Value b = stack_.back();
            stack_.pop_back();
            Value a = stack_.back();
            stack_.pop_back();

            // SQL NULL semantics: NULL >= anything is false
            if (a.is_null() || b.is_null()) {
                stack_.push_back(Value::make_bool(false));
                break;
            }

            bool result = false;
            if (a.is_int() && b.is_int()) {
                result = a.as_int() >= b.as_int();
            } else if ((a.is_int() || a.is_double()) && (b.is_int() || b.is_double())) {
                double a_val = a.is_double() ? a.as_double() : static_cast<double>(a.as_int());
                double b_val = b.is_double() ? b.as_double() : static_cast<double>(b.as_int());
                result = a_val >= b_val;
            } else if (a.is_string() && b.is_string()) {
                result = a.as_string() >= b.as_string();
            } else {
                throw RuntimeError("Cannot compare incompatible types");
            }

            stack_.push_back(Value::make_bool(result));
            break;
        }

            // ================================================================
            // Logical NOT - Boolean Negation
            // ================================================================
            // Pops bool (or int treated as bool), pushes negated result

        case IRExpr::OpCode::NOT: {
            Value a = stack_.back();
            stack_.pop_back();

            // SQL NULL semantics: NOT NULL = NULL (represented as false in boolean context)
            if (a.is_null()) {
                stack_.push_back(Value::make_bool(false));
            } else if (a.is_bool()) {
                stack_.push_back(Value::make_bool(!a.as_bool()));
            } else if (a.is_int()) {
                // Treat 0 as false, non-zero as true (C-style)
                stack_.push_back(Value::make_bool(a.as_int() == 0));
            } else {
                throw RuntimeError("Cannot apply NOT to non-boolean value");
            }
            break;
        }

        case IRExpr::OpCode::TERNARY: {
            // Stack has: [..., condition, true_val, false_val]
            // Pop in reverse order
            Value false_val = stack_.back();
            stack_.pop_back();
            Value true_val = stack_.back();
            stack_.pop_back();
            Value condition = stack_.back();
            stack_.pop_back();

            // Evaluate condition as boolean
            bool cond_result;
            if (condition.is_null()) {
                // NULL is falsy
                cond_result = false;
            } else if (condition.is_bool()) {
                cond_result = condition.as_bool();
            } else if (condition.is_int()) {
                // Non-zero is truthy (C-style)
                cond_result = (condition.as_int() != 0);
            } else {
                throw RuntimeError("Ternary condition must be boolean or numeric");
            }

            // Push selected value
            stack_.push_back(cond_result ? true_val : false_val);
            break;
        }
        }
    }

    // After executing all instructions, stack should contain exactly one value
    // This is the result of the expression
    if (stack_.size() != 1) {
        throw RuntimeError("Expression evaluation error: invalid stack state");
    }

    return stack_.back();
}

}  // namespace joy
