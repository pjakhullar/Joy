#include "compiler.hpp"
#include <variant>

namespace joy {

// ============================================================================
// Compiler Implementation
// ============================================================================

ExecutionPlan Compiler::compile(const Program& program) {
    ExecutionPlan plan;

    for (const auto& stmt : program.statements) {
        plan.operators.push_back(compile_stmt(stmt));
    }

    return plan;
}

PhysicalOp Compiler::compile_stmt(const Stmt& stmt) {
    PhysicalOp op;

    std::visit([&](const auto& node) {
        using T = std::decay_t<decltype(node)>;

        if constexpr (std::is_same_v<T, FromStmt>) {
            op.type = OpType::SCAN;
            op.data = PhysicalOp::ScanOp{node.filepath};
        }
        else if constexpr (std::is_same_v<T, FilterStmt>) {
            op.type = OpType::FILTER;
            IRExpr predicate = compile_expr(*node.condition);
            op.data = PhysicalOp::FilterOp{std::move(predicate)};
        }
        else if constexpr (std::is_same_v<T, SelectStmt>) {
            op.type = OpType::PROJECT;
            op.data = PhysicalOp::ProjectOp{node.columns};
        }
        else if constexpr (std::is_same_v<T, WriteStmt>) {
            op.type = OpType::WRITE;
            op.data = PhysicalOp::WriteOp{node.filepath};
        }
    }, stmt.node);

    return op;
}

IRExpr Compiler::compile_expr(const Expr& expr) {
    IRExpr result;

    std::visit([&](const auto& node) {
        using T = std::decay_t<decltype(node)>;

        if constexpr (std::is_same_v<T, LiteralExpr>) {
            compile_literal(node, result);
        }
        else if constexpr (std::is_same_v<T, ColumnRef>) {
            compile_column_ref(node, result);
        }
        else if constexpr (std::is_same_v<T, BinaryExpr>) {
            compile_binary(node, result);
        }
        else if constexpr (std::is_same_v<T, UnaryExpr>) {
            compile_unary(node, result);
        }
    }, expr.node);

    return result;
}

void Compiler::compile_literal(const LiteralExpr& node, IRExpr& result) {
    const auto& lit = node.value;

    std::visit([&](const auto& val) {
        using T = std::decay_t<decltype(val)>;

        if constexpr (std::is_same_v<T, int64_t>) {
            result.instructions.push_back({IRExpr::OpCode::PUSH_INT, val});
        }
        else if constexpr (std::is_same_v<T, double>) {
            result.instructions.push_back({IRExpr::OpCode::PUSH_DOUBLE, val});
        }
        else if constexpr (std::is_same_v<T, std::string>) {
            result.instructions.push_back({IRExpr::OpCode::PUSH_STRING, val});
        }
        else if constexpr (std::is_same_v<T, bool>) {
            result.instructions.push_back({IRExpr::OpCode::PUSH_BOOL, val});
        }
    }, lit.value);
}

void Compiler::compile_column_ref(const ColumnRef& node, IRExpr& result) {
    // Store column name as string operand
    // VM will resolve to column index at runtime
    result.instructions.push_back({IRExpr::OpCode::LOAD_COLUMN, node.name});
}

void Compiler::compile_binary(const BinaryExpr& node, IRExpr& result) {
    // Compile left operand
    IRExpr left = compile_expr(*node.left);
    result.instructions.insert(result.instructions.end(),
                               left.instructions.begin(),
                               left.instructions.end());

    // Compile right operand
    IRExpr right = compile_expr(*node.right);
    result.instructions.insert(result.instructions.end(),
                               right.instructions.begin(),
                               right.instructions.end());

    // Add operator instruction
    IRExpr::OpCode op_code;
    switch (node.op) {
        case BinaryOp::Add: op_code = IRExpr::OpCode::ADD; break;
        case BinaryOp::Sub: op_code = IRExpr::OpCode::SUB; break;
        case BinaryOp::Mul: op_code = IRExpr::OpCode::MUL; break;
        case BinaryOp::Div: op_code = IRExpr::OpCode::DIV; break;
        case BinaryOp::Eq:  op_code = IRExpr::OpCode::EQ; break;
        case BinaryOp::Neq: op_code = IRExpr::OpCode::NEQ; break;
        case BinaryOp::Lt:  op_code = IRExpr::OpCode::LT; break;
        case BinaryOp::Gt:  op_code = IRExpr::OpCode::GT; break;
        case BinaryOp::Lte: op_code = IRExpr::OpCode::LTE; break;
        case BinaryOp::Gte: op_code = IRExpr::OpCode::GTE; break;
    }

    result.instructions.push_back({op_code, 0});
}

void Compiler::compile_unary(const UnaryExpr& node, IRExpr& result) {
    // Compile operand
    IRExpr operand = compile_expr(*node.operand);
    result.instructions.insert(result.instructions.end(),
                               operand.instructions.begin(),
                               operand.instructions.end());

    // Add operator instruction
    IRExpr::OpCode op_code;
    switch (node.op) {
        case UnaryOp::Neg: op_code = IRExpr::OpCode::NEG; break;
        case UnaryOp::Not: op_code = IRExpr::OpCode::NOT; break;
    }

    result.instructions.push_back({op_code, 0});
}

} // namespace joy
