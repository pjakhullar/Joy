#include "vm.hpp"
#include <variant>
#include <iostream>

namespace joy {

// ============================================================================
// Value Implementation
// ============================================================================

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

Value Value::make_int(int64_t val) {
    return Value{val};
}

Value Value::make_double(double val) {
    return Value{val};
}

Value Value::make_string(std::string val) {
    return Value{std::move(val)};
}

Value Value::make_bool(bool val) {
    return Value{val};
}

// ============================================================================
// VM Implementation
// ============================================================================

void VM::execute(const ExecutionPlan& plan) {
    for (const auto& op : plan.operators) {
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

void VM::execute_scan(const PhysicalOp::ScanOp& op) {
    current_table_ = read_csv(op.filepath);
}

void VM::execute_filter(const PhysicalOp::FilterOp& op) {
    Table result;

    // Copy column structure
    for (const auto& col : current_table_.columns) {
        Column new_col;
        new_col.name = col.name;
        new_col.type = col.type;

        // Initialize empty vector of the same type
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

    // Evaluate predicate for each row
    for (size_t row = 0; row < current_table_.num_rows; ++row) {
        Value predicate_result = eval_expr(op.predicate, row);

        bool keep_row = false;
        if (predicate_result.is_bool()) {
            keep_row = predicate_result.as_bool();
        } else if (predicate_result.is_int()) {
            keep_row = predicate_result.as_int() != 0;
        } else {
            throw RuntimeError("Filter predicate must return boolean");
        }

        if (keep_row) {
            // Copy row to result
            for (size_t col_idx = 0; col_idx < current_table_.columns.size(); ++col_idx) {
                const Column& src_col = current_table_.columns[col_idx];
                Column& dst_col = result.columns[col_idx];

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

    current_table_ = std::move(result);
}

void VM::execute_project(const PhysicalOp::ProjectOp& op) {
    current_table_ = current_table_.project(op.columns);
}

void VM::execute_write(const PhysicalOp::WriteOp& op) {
    write_csv(op.filepath, current_table_);
}

Value VM::eval_expr(const IRExpr& expr, size_t row_idx) {
    stack_.clear();

    for (const auto& instr : expr.instructions) {
        switch (instr.op) {
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

            case IRExpr::OpCode::LOAD_COLUMN: {
                std::string col_name = std::get<std::string>(instr.operand);
                const Column* col = current_table_.get_column(col_name);
                if (!col) {
                    throw RuntimeError("Column not found: " + col_name);
                }

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

            case IRExpr::OpCode::ADD: {
                Value b = stack_.back(); stack_.pop_back();
                Value a = stack_.back(); stack_.pop_back();

                if (a.is_int() && b.is_int()) {
                    stack_.push_back(Value::make_int(a.as_int() + b.as_int()));
                } else {
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

            case IRExpr::OpCode::EQ: {
                Value b = stack_.back(); stack_.pop_back();
                Value a = stack_.back(); stack_.pop_back();

                bool result = false;
                if (a.is_int() && b.is_int()) {
                    result = a.as_int() == b.as_int();
                } else if ((a.is_int() || a.is_double()) && (b.is_int() || b.is_double())) {
                    double a_val = a.is_double() ? a.as_double() : static_cast<double>(a.as_int());
                    double b_val = b.is_double() ? b.as_double() : static_cast<double>(b.as_int());
                    result = a_val == b_val;
                } else if (a.is_string() && b.is_string()) {
                    result = a.as_string() == b.as_string();
                } else if (a.is_bool() && b.is_bool()) {
                    result = a.as_bool() == b.as_bool();
                }

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

            case IRExpr::OpCode::NOT: {
                Value a = stack_.back(); stack_.pop_back();

                if (a.is_bool()) {
                    stack_.push_back(Value::make_bool(!a.as_bool()));
                } else if (a.is_int()) {
                    stack_.push_back(Value::make_bool(a.as_int() == 0));
                } else {
                    throw RuntimeError("Cannot apply NOT to non-boolean value");
                }
                break;
            }
        }
    }

    if (stack_.size() != 1) {
        throw RuntimeError("Expression evaluation error: invalid stack state");
    }

    return stack_.back();
}

} // namespace joy
