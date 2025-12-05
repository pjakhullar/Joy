#pragma once

#include <string>
#include <variant>
#include <vector>

#include "vectorized_ops.hpp"  // For VectorArithOp

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
        NOT,

        // Ternary conditional
        TERNARY  // Pop 3: condition, true_val, false_val; push result
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
    SCAN,                          // Read CSV into table
    FILTER,                        // Filter rows by predicate (scalar, row-at-a-time)
    VECTORIZED_FILTER,             // Filter rows using vectorized operations (FAST!)
    PROJECT,                       // Select specific columns
    TRANSFORM,                     // Add/update column with expression (scalar)
    VECTORIZED_TRANSFORM,          // Add/update column with vectorized arithmetic (FAST!)
    VECTORIZED_TERNARY_TRANSFORM,  // Add/update column with vectorized ternary (FAST!)
    WRITE                          // Write table to CSV
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

    struct TransformOp {
        std::string column_name;
        IRExpr expression;
    };

    // Vectorized transform - processes entire columns at once (FAST!)
    // Only handles simple patterns: column op column, column op scalar
    // Example: "total = price * quantity" → VectorizedTransformOp{...}
    struct VectorizedTransformOp {
        std::string column_name;
        VectorArithOp op;  // ADD, SUB, MUL, DIV

        // Operands can be either column names or scalar values
        // If is_*_column is true, use *_column_name; otherwise use *_scalar
        bool is_left_column;
        std::string left_column_name;
        std::variant<int64_t, double> left_scalar;

        bool is_right_column;
        std::string right_column_name;
        std::variant<int64_t, double> right_scalar;

        // Result type (inferred from operand types)
        ColumnType result_type;
    };

    // Vectorized ternary transform - vectorized conditional
    // Pattern: condition ? true_val : false_val (all vectorized)
    // Example: "class = score > 90 ? \"A\" : \"B\"" → VectorizedTernaryTransformOp{...}
    struct VectorizedTernaryTransformOp {
        std::string column_name;

        // Condition (must be a comparison that can be vectorized)
        VectorizedFilterOp condition;  // Reuse vectorized filter for condition

        // True/false values can be columns or scalars
        bool is_true_column;
        std::string true_column_name;
        std::variant<int64_t, double, std::string> true_scalar;

        bool is_false_column;
        std::string false_column_name;
        std::variant<int64_t, double, std::string> false_scalar;

        // Result type
        ColumnType result_type;
    };

    struct WriteOp {
        std::string filepath;
    };

    std::variant<ScanOp, FilterOp, VectorizedFilterOp, ProjectOp, TransformOp,
                 VectorizedTransformOp, VectorizedTernaryTransformOp, WriteOp>
        data;
};

// ============================================================================
// Complete Execution Plan
// ============================================================================

struct ExecutionPlan {
    std::vector<PhysicalOp> operators;
};

}  // namespace joy
