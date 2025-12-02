#pragma once

#include "ir.hpp"
#include "table.hpp"
#include <stdexcept>
#include <variant>
#include <vector>

namespace joy {

// ============================================================================
// Runtime Exception
// ============================================================================

class RuntimeError : public std::runtime_error {
public:
    explicit RuntimeError(const std::string& message)
        : std::runtime_error(message) {}
};

// ============================================================================
// Runtime Value (Stack Value During Expression Evaluation)
// ============================================================================

// NULL is represented using std::monostate (SQL-style NULL semantics)
struct Value {
    std::variant<std::monostate, int64_t, double, std::string, bool> data;

    // Type queries
    bool is_null() const;
    bool is_int() const;
    bool is_double() const;
    bool is_string() const;
    bool is_bool() const;

    // Getters
    int64_t as_int() const;
    double as_double() const;
    const std::string& as_string() const;
    bool as_bool() const;

    // Constructors
    static Value make_null();
    static Value make_int(int64_t val);
    static Value make_double(double val);
    static Value make_string(std::string val);
    static Value make_bool(bool val);
};

// ============================================================================
// Virtual Machine (Executes IR)
// ============================================================================

class VM {
public:
    VM() = default;

    // Execute entire plan
    void execute(const ExecutionPlan& plan);

private:
    Table current_table_;

    // Execute individual operators
    void execute_scan(const PhysicalOp::ScanOp& op);
    void execute_filter(const PhysicalOp::FilterOp& op);
    void execute_project(const PhysicalOp::ProjectOp& op);
    void execute_write(const PhysicalOp::WriteOp& op);

    // Evaluate expression bytecode for a single row
    Value eval_expr(const IRExpr& expr, size_t row_idx);

    // Stack-based expression evaluator
    std::vector<Value> stack_;
};

} // namespace joy
