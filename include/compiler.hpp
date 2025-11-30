#pragma once

#include "ast.hpp"
#include "ir.hpp"
#include "table.hpp"
#include <stdexcept>

namespace joy {

// ============================================================================
// Compiler Exception
// ============================================================================

class CompileError : public std::runtime_error {
public:
    explicit CompileError(const std::string& message)
        : std::runtime_error(message) {}
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

    // Column name to index mapping (set during SCAN compilation)
    // For MVP, we don't know schema at compile time, so LOAD_COLUMN
    // uses column names; VM resolves at runtime
    // (Post-MVP: add schema inference pass)
};

} // namespace joy
