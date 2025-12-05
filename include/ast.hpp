#pragma once

#include <memory>
#include <string>
#include <variant>
#include <vector>

namespace joy {

// Forward declarations
struct Expr;
struct Stmt;

// ============================================================================
// Expression Types
// ============================================================================

enum class BinaryOp {
    Add,  // +
    Sub,  // -
    Mul,  // *
    Div,  // /
    Eq,   // ==
    Neq,  // !=
    Lt,   // <
    Gt,   // >
    Lte,  // <=
    Gte   // >=
};

enum class UnaryOp {
    Neg,  // -x
    Not   // not x
};

enum class ValueType { Int, Double, String, Bool };

// Literal value
struct Literal {
    std::variant<int64_t, double, std::string, bool> value;
    ValueType type;
};

// AST node types for expressions
struct LiteralExpr {
    Literal value;
};

struct ColumnRef {
    std::string name;
};

struct BinaryExpr {
    BinaryOp op;
    std::unique_ptr<Expr> left;
    std::unique_ptr<Expr> right;
};

struct UnaryExpr {
    UnaryOp op;
    std::unique_ptr<Expr> operand;
};

// Expression wrapper
struct Expr {
    std::variant<LiteralExpr, ColumnRef, BinaryExpr, UnaryExpr> node;
};

// ============================================================================
// Statement Types
// ============================================================================

struct FromStmt {
    std::string filepath;
};

struct FilterStmt {
    std::unique_ptr<Expr> condition;
};

struct SelectStmt {
    std::vector<std::string> columns;
};

struct WriteStmt {
    std::string filepath;
};

// Statement wrapper
struct Stmt {
    std::variant<FromStmt, FilterStmt, SelectStmt, WriteStmt> node;
};

// ============================================================================
// Top-level Program
// ============================================================================

struct Program {
    std::vector<Stmt> statements;
};

// ============================================================================
// Helper Functions for Creating AST Nodes
// ============================================================================

inline std::unique_ptr<Expr> make_literal(int64_t val) {
    auto expr = std::make_unique<Expr>();
    expr->node = LiteralExpr{Literal{val, ValueType::Int}};
    return expr;
}

inline std::unique_ptr<Expr> make_literal(double val) {
    auto expr = std::make_unique<Expr>();
    expr->node = LiteralExpr{Literal{val, ValueType::Double}};
    return expr;
}

inline std::unique_ptr<Expr> make_literal(std::string val) {
    auto expr = std::make_unique<Expr>();
    expr->node = LiteralExpr{Literal{std::move(val), ValueType::String}};
    return expr;
}

inline std::unique_ptr<Expr> make_literal(bool val) {
    auto expr = std::make_unique<Expr>();
    expr->node = LiteralExpr{Literal{val, ValueType::Bool}};
    return expr;
}

inline std::unique_ptr<Expr> make_column_ref(std::string name) {
    auto expr = std::make_unique<Expr>();
    expr->node = ColumnRef{std::move(name)};
    return expr;
}

inline std::unique_ptr<Expr> make_binary(BinaryOp op, std::unique_ptr<Expr> left,
                                         std::unique_ptr<Expr> right) {
    auto expr = std::make_unique<Expr>();
    expr->node = BinaryExpr{op, std::move(left), std::move(right)};
    return expr;
}

inline std::unique_ptr<Expr> make_unary(UnaryOp op, std::unique_ptr<Expr> operand) {
    auto expr = std::make_unique<Expr>();
    expr->node = UnaryExpr{op, std::move(operand)};
    return expr;
}

}  // namespace joy
