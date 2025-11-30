#include "vm.hpp"
#include <variant>
#include <iostream>

namespace joy {

// ============================================================================
// Value Implementation - Runtime Value Representation
// ============================================================================
// Value is a type-erased runtime value used during bytecode execution
// Uses std::variant to hold one of four possible types
// This is similar to Python's PyObject or JavaScript's JSValue
// The VM stack contains these Values during expression evaluation

// Type checking methods - safely check what type is currently held
// Uses std::holds_alternative to query the variant without extracting

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
        std::visit([this](const auto& op_data) {
            using T = std::decay_t<decltype(op_data)>;

            if constexpr (std::is_same_v<T, PhysicalOp::ScanOp>) {
                execute_scan(op_data);
            }
            else if constexpr (std::is_same_v<T, PhysicalOp::FilterOp>) {
                execute_filter(op_data);
            }
            else if constexpr (std::is_same_v<T, PhysicalOp::ProjectOp>) {
                execute_project(op_data);
            }
            else if constexpr (std::is_same_v<T, PhysicalOp::WriteOp>) {
                execute_write(op_data);
            }
        }, op.data);
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
    for (const auto& col : current_table_.columns) {
        Column new_col;
        new_col.name = col.name;
        new_col.type = col.type;

        // Initialize empty vector of the appropriate type
        // We don't know final size yet, so start empty
        switch (col.type) {
            case ColumnType::INT64:
                new_col.data = std::vector<int64_t>{};
                break;
            case ColumnType::DOUBLE:
                new_col.data = std::vector<double>{};
                break;
            case ColumnType::STRING:
                new_col.data = std::vector<std::string>{};
                break;
            case ColumnType::BOOL:
                new_col.data = std::vector<bool>{};
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
        // Allows both bool and int (0 = false, non-zero = true)
        bool keep_row = false;
        if (predicate_result.is_bool()) {
            keep_row = predicate_result.as_bool();
        } else if (predicate_result.is_int()) {
            keep_row = predicate_result.as_int() != 0;
        } else {
            throw RuntimeError("Filter predicate must return boolean");
        }

        // Step 2b: If predicate is true, copy this row to result
        if (keep_row) {
            // Copy each column's value for this row
            for (size_t col_idx = 0; col_idx < current_table_.columns.size(); ++col_idx) {
                const Column& src_col = current_table_.columns[col_idx];
                Column& dst_col = result.columns[col_idx];

                // Type-specific copy (no conversion)
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
            result.num_rows++;
        }
    }

    // Step 3: Replace current table with filtered result
    current_table_ = std::move(result);
}

// PROJECT operator: Select subset of columns
// Example: select name, age
// Delegates to table.project() which does the actual work
void VM::execute_project(const PhysicalOp::ProjectOp& op) {
    current_table_ = current_table_.project(op.columns);
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
            // LOAD_COLUMN - Load value from current row
            // ================================================================
            // Operand is column name (string)
            // We look up the column, get the value at row_idx, push it
            // Example: LOAD_COLUMN "age" when row_idx=2 loads age[2]

            case IRExpr::OpCode::LOAD_COLUMN: {
                std::string col_name = std::get<std::string>(instr.operand);
                const Column* col = current_table_.get_column(col_name);
                if (!col) {
                    throw RuntimeError("Column not found: " + col_name);
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
                Value b = stack_.back(); stack_.pop_back();  // Right operand
                Value a = stack_.back(); stack_.pop_back();  // Left operand

                // Type coercion: int + int = int, anything with double = double
                if (a.is_int() && b.is_int()) {
                    stack_.push_back(Value::make_int(a.as_int() + b.as_int()));
                } else {
                    // Promote to double if either operand is double
                    double a_val = a.is_double() ? a.as_double() : static_cast<double>(a.as_int());
                    double b_val = b.is_double() ? b.as_double() : static_cast<double>(b.as_int());
                    stack_.push_back(Value::make_double(a_val + b_val));
                }
                break;
            }

            case IRExpr::OpCode::SUB: {
                Value b = stack_.back(); stack_.pop_back();
                Value a = stack_.back(); stack_.pop_back();

                if (a.is_int() && b.is_int()) {
                    stack_.push_back(Value::make_int(a.as_int() - b.as_int()));
                } else {
                    double a_val = a.is_double() ? a.as_double() : static_cast<double>(a.as_int());
                    double b_val = b.is_double() ? b.as_double() : static_cast<double>(b.as_int());
                    stack_.push_back(Value::make_double(a_val - b_val));
                }
                break;
            }

            case IRExpr::OpCode::MUL: {
                Value b = stack_.back(); stack_.pop_back();
                Value a = stack_.back(); stack_.pop_back();

                if (a.is_int() && b.is_int()) {
                    stack_.push_back(Value::make_int(a.as_int() * b.as_int()));
                } else {
                    double a_val = a.is_double() ? a.as_double() : static_cast<double>(a.as_int());
                    double b_val = b.is_double() ? b.as_double() : static_cast<double>(b.as_int());
                    stack_.push_back(Value::make_double(a_val * b_val));
                }
                break;
            }

            case IRExpr::OpCode::DIV: {
                Value b = stack_.back(); stack_.pop_back();
                Value a = stack_.back(); stack_.pop_back();

                // Check for division by zero
                if (a.is_int() && b.is_int()) {
                    if (b.as_int() == 0) throw RuntimeError("Division by zero");
                    stack_.push_back(Value::make_int(a.as_int() / b.as_int()));
                } else {
                    double a_val = a.is_double() ? a.as_double() : static_cast<double>(a.as_int());
                    double b_val = b.is_double() ? b.as_double() : static_cast<double>(b.as_int());
                    if (b_val == 0.0) throw RuntimeError("Division by zero");
                    stack_.push_back(Value::make_double(a_val / b_val));
                }
                break;
            }

            // ================================================================
            // Unary Arithmetic - Negation
            // ================================================================
            // Pops one value, pushes negated result

            case IRExpr::OpCode::NEG: {
                Value a = stack_.back(); stack_.pop_back();

                if (a.is_int()) {
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

            case IRExpr::OpCode::EQ: {
                Value b = stack_.back(); stack_.pop_back();
                Value a = stack_.back(); stack_.pop_back();

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
                // Different types → false (no implicit conversion)

                stack_.push_back(Value::make_bool(result));
                break;
            }

            case IRExpr::OpCode::NEQ: {
                Value b = stack_.back(); stack_.pop_back();
                Value a = stack_.back(); stack_.pop_back();

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
                }

                stack_.push_back(Value::make_bool(result));
                break;
            }

            case IRExpr::OpCode::LT: {
                Value b = stack_.back(); stack_.pop_back();
                Value a = stack_.back(); stack_.pop_back();

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
                }

                stack_.push_back(Value::make_bool(result));
                break;
            }

            case IRExpr::OpCode::GT: {
                Value b = stack_.back(); stack_.pop_back();
                Value a = stack_.back(); stack_.pop_back();

                bool result = false;
                if (a.is_int() && b.is_int()) {
                    result = a.as_int() > b.as_int();
                } else if ((a.is_int() || a.is_double()) && (b.is_int() || b.is_double())) {
                    double a_val = a.is_double() ? a.as_double() : static_cast<double>(a.as_int());
                    double b_val = b.is_double() ? b.as_double() : static_cast<double>(b.as_int());
                    result = a_val > b_val;
                } else if (a.is_string() && b.is_string()) {
                    result = a.as_string() > b.as_string();
                }

                stack_.push_back(Value::make_bool(result));
                break;
            }

            case IRExpr::OpCode::LTE: {
                Value b = stack_.back(); stack_.pop_back();
                Value a = stack_.back(); stack_.pop_back();

                bool result = false;
                if (a.is_int() && b.is_int()) {
                    result = a.as_int() <= b.as_int();
                } else if ((a.is_int() || a.is_double()) && (b.is_int() || b.is_double())) {
                    double a_val = a.is_double() ? a.as_double() : static_cast<double>(a.as_int());
                    double b_val = b.is_double() ? b.as_double() : static_cast<double>(b.as_int());
                    result = a_val <= b_val;
                } else if (a.is_string() && b.is_string()) {
                    result = a.as_string() <= b.as_string();
                }

                stack_.push_back(Value::make_bool(result));
                break;
            }

            case IRExpr::OpCode::GTE: {
                Value b = stack_.back(); stack_.pop_back();
                Value a = stack_.back(); stack_.pop_back();

                bool result = false;
                if (a.is_int() && b.is_int()) {
                    result = a.as_int() >= b.as_int();
                } else if ((a.is_int() || a.is_double()) && (b.is_int() || b.is_double())) {
                    double a_val = a.is_double() ? a.as_double() : static_cast<double>(a.as_int());
                    double b_val = b.is_double() ? b.as_double() : static_cast<double>(b.as_int());
                    result = a_val >= b_val;
                } else if (a.is_string() && b.is_string()) {
                    result = a.as_string() >= b.as_string();
                }

                stack_.push_back(Value::make_bool(result));
                break;
            }

            // ================================================================
            // Logical NOT - Boolean Negation
            // ================================================================
            // Pops bool (or int treated as bool), pushes negated result

            case IRExpr::OpCode::NOT: {
                Value a = stack_.back(); stack_.pop_back();

                if (a.is_bool()) {
                    stack_.push_back(Value::make_bool(!a.as_bool()));
                } else if (a.is_int()) {
                    // Treat 0 as false, non-zero as true (C-style)
                    stack_.push_back(Value::make_bool(a.as_int() == 0));
                } else {
                    throw RuntimeError("Cannot apply NOT to non-boolean value");
                }
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

} // namespace joy
