#pragma once

#include <string>
#include <variant>
#include <vector>

namespace joy {

// ============================================================================
// IR Expression (Compiled Bytecode for Expression Evaluation)
// ============================================================================

struct IRExpr {
    enum class OpCode {
        // Stack operations
        PUSH_INT,
        PUSH_DOUBLE,
        PUSH_STRING,
        PUSH_BOOL,
        LOAD_COLUMN,  // Load column value at current row

        // Arithmetic
        ADD,
        SUB,
        MUL,
        DIV,
        NEG,

        // Comparison
        EQ,
        NEQ,
        LT,
        GT,
        LTE,
        GTE,

        // Logical
        NOT
    };

    struct Instruction {
        OpCode op;
        // Operand: int for column indices, or literal values
        std::variant<int64_t, double, std::string, bool, int> operand;
    };

    std::vector<Instruction> instructions;
};

// ============================================================================
// Physical Operators (Pipeline Operations on Tables)
// ============================================================================

enum class OpType {
    SCAN,      // Read CSV into table
    FILTER,    // Filter rows by predicate
    PROJECT,   // Select specific columns
    WRITE      // Write table to CSV
};

struct PhysicalOp {
    OpType type;

    // Operator-specific data
    struct ScanOp {
        std::string filepath;
    };

    struct FilterOp {
        IRExpr predicate;
    };

    struct ProjectOp {
        std::vector<std::string> columns;
    };

    struct WriteOp {
        std::string filepath;
    };

    std::variant<ScanOp, FilterOp, ProjectOp, WriteOp> data;
};

// ============================================================================
// Complete Execution Plan
// ============================================================================

struct ExecutionPlan {
    std::vector<PhysicalOp> operators;
};

} // namespace joy
