#pragma once

#include <optional>
#include <stdexcept>

#include "ast.hpp"
#include "ir.hpp"
#include "table.hpp"

namespace joy {

// ============================================================================
// Compiler Exception
// ============================================================================

class CompileError : public std::runtime_error {
public:
    explicit CompileError(const std::string& message) : std::runtime_error(message) {}
};

// ============================================================================
// Compiler (AST → IR)
// ============================================================================

class Compiler {
public:
    // Compile entire program into execution plan
    ExecutionPlan compile(const Program& program);

private:
    // Compile individual expression into bytecode
    IRExpr compile_expr(const Expr& expr);

    // Compile statement into physical operator
    PhysicalOp compile_stmt(const Stmt& stmt);

    // Expression compilation helpers
    void compile_literal(const LiteralExpr& node, IRExpr& result);
    void compile_column_ref(const ColumnRef& node, IRExpr& result);
    void compile_binary(const BinaryExpr& node, IRExpr& result);
    void compile_unary(const UnaryExpr& node, IRExpr& result);
    void compile_ternary(const TernaryExpr& node, IRExpr& result);

    // Vectorization pattern detection
    // Try to convert filter expression to vectorized operation
    // Returns nullopt if expression is too complex
    std::optional<PhysicalOp::VectorizedFilterOp> try_vectorize_filter(const Expr& expr);

    // Try to convert transform expressions to vectorized operations
    std::optional<PhysicalOp::VectorizedTransformOp> try_vectorize_arith_transform(
        const std::string& column_name, const Expr& expr);
    std::optional<PhysicalOp::VectorizedTernaryTransformOp> try_vectorize_ternary_transform(
        const std::string& column_name, const Expr& expr, int max_depth);
};

}  // namespace joy
