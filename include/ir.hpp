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
    SCAN,               // Read CSV into table
    FILTER,             // Filter rows by predicate (scalar, row-at-a-time)
    VECTORIZED_FILTER,  // Filter rows using vectorized operations (FAST!)
    PROJECT,            // Select specific columns
    WRITE               // Write table to CSV
};

// ============================================================================
// Vectorized Filter Operations (for blazingly fast column operations)
// ============================================================================

enum class VectorOp {
    // Comparison operators for vectorized filters
    GT,   // Greater than
    LT,   // Less than
    GTE,  // Greater than or equal
    LTE,  // Less than or equal
    EQ,   // Equal
    NEQ   // Not equal
};

struct PhysicalOp {
    OpType type;

    // Operator-specific data
    struct ScanOp {
        std::string filepath;
    };

    struct FilterOp {
        IRExpr predicate;  // Scalar expression (fallback for complex cases)
    };

    // Vectorized filter - processes entire column at once
    // Example: "age > 30" → VectorizedFilterOp{"age", GT, int(30)}
    struct VectorizedFilterOp {
        std::string column_name;
        VectorOp op;
        // Scalar value to compare against (type depends on column)
        std::variant<int64_t, double, std::string> value;
    };

    struct ProjectOp {
        std::vector<std::string> columns;
    };

    struct WriteOp {
        std::string filepath;
    };

    std::variant<ScanOp, FilterOp, VectorizedFilterOp, ProjectOp, WriteOp> data;
};

// ============================================================================
// Complete Execution Plan
// ============================================================================

struct ExecutionPlan {
    std::vector<PhysicalOp> operators;
};

}  // namespace joy
