#include "compiler.hpp"

#include <variant>

namespace joy {

// ============================================================================
// Compiler Implementation (AST → IR Translation)
// ============================================================================
// The compiler translates high-level AST nodes into low-level IR (bytecode)
// This is the bridge between parsing and execution
//
// Translation strategy:
//   - Statements → Physical Operators (SCAN, FILTER, PROJECT, WRITE)
//   - Expressions → Stack Bytecode (PUSH, LOAD, ADD, GT, etc.)
//
// Example:
//   AST: filter age > 30
//   IR:  FilterOp { bytecode: [LOAD_COLUMN "age", PUSH_INT 30, GT] }

// Main entry point: Compile entire program
// Converts Program (list of AST statements) into ExecutionPlan (list of IR operators)
ExecutionPlan Compiler::compile(const Program& program) {
    ExecutionPlan plan;

    // Translate each statement into a physical operator
    // Order is preserved: operators execute sequentially in a pipeline
    for (const auto& stmt : program.statements) {
        plan.operators.push_back(compile_stmt(stmt));
    }

    return plan;
}

// Compile a single statement into a physical operator
// Uses std::visit to pattern-match on the variant type
// This is C++17's type-safe alternative to virtual dispatch
PhysicalOp Compiler::compile_stmt(const Stmt& stmt) {
    PhysicalOp op;

    std::visit(
        [&](const auto& node) {
            using T = std::decay_t<decltype(node)>;  // Get actual type without references

            // FROM "file.csv" → SCAN operator (load data from file)
            if constexpr (std::is_same_v<T, FromStmt>) {
                op.type = OpType::SCAN;
                op.data = PhysicalOp::ScanOp{node.filepath};
            }
            // FILTER expr → Try vectorized path first, fall back to scalar
            else if constexpr (std::is_same_v<T, FilterStmt>) {
                // Try to detect simple vectorizable pattern: column op scalar
                auto vec_pattern = try_vectorize_filter(*node.condition);
                if (vec_pattern.has_value()) {
                    // Use fast vectorized path
                    op.type = OpType::VECTORIZED_FILTER;
                    op.data = vec_pattern.value();
                } else {
                    // Fall back to scalar row-at-a-time execution
                    op.type = OpType::FILTER;
                    IRExpr predicate = compile_expr(*node.condition);
                    op.data = PhysicalOp::FilterOp{std::move(predicate)};
                }
            }
            // SELECT col1, col2 → PROJECT operator (column selection)
            else if constexpr (std::is_same_v<T, SelectStmt>) {
                op.type = OpType::PROJECT;
                op.data = PhysicalOp::ProjectOp{node.columns};
            }
            // TRANSFORM column = expr → Try vectorized first, fall back to scalar
            else if constexpr (std::is_same_v<T, TransformStmt>) {
                // Try vectorized ternary first (depth limit = 1)
                auto vec_ternary = try_vectorize_ternary_transform(node.column_name, *node.expression, 1);
                if (vec_ternary.has_value()) {
                    op.type = OpType::VECTORIZED_TERNARY_TRANSFORM;
                    op.data = vec_ternary.value();
                } else {
                    // Try vectorized arithmetic
                    auto vec_arith = try_vectorize_arith_transform(node.column_name, *node.expression);
                    if (vec_arith.has_value()) {
                        op.type = OpType::VECTORIZED_TRANSFORM;
                        op.data = vec_arith.value();
                    } else {
                        // Fall back to scalar evaluation
                        op.type = OpType::TRANSFORM;
                        IRExpr expression = compile_expr(*node.expression);
                        op.data = PhysicalOp::TransformOp{node.column_name, std::move(expression)};
                    }
                }
            }
            // WRITE "file.csv" → WRITE operator (save data to file)
            else if constexpr (std::is_same_v<T, WriteStmt>) {
                op.type = OpType::WRITE;
                op.data = PhysicalOp::WriteOp{node.filepath};
            }
        },
        stmt.node);

    return op;
}

// ============================================================================
// Expression Compilation (AST → Bytecode)
// ============================================================================
// Compiles expression AST into stack-based bytecode
// This is where the real "compilation" happens
//
// Example:
//   AST:      BinaryExpr(GT, ColumnRef("age"), Literal(30))
//   Bytecode: [LOAD_COLUMN "age", PUSH_INT 30, GT]
//
// Execution model: Stack machine (like JVM, Python bytecode, WebAssembly)
//   1. Operands are pushed onto stack
//   2. Operators pop operands, push result

// Compile expression into IR bytecode
// Recursively walks the expression tree and emits bytecode
IRExpr Compiler::compile_expr(const Expr& expr) {
    IRExpr result;

    // Pattern match on expression node type
    std::visit(
        [&](const auto& node) {
            using T = std::decay_t<decltype(node)>;

            if constexpr (std::is_same_v<T, LiteralExpr>) {
                compile_literal(node, result);
            } else if constexpr (std::is_same_v<T, ColumnRef>) {
                compile_column_ref(node, result);
            } else if constexpr (std::is_same_v<T, BinaryExpr>) {
                compile_binary(node, result);
            } else if constexpr (std::is_same_v<T, UnaryExpr>) {
                compile_unary(node, result);
            } else if constexpr (std::is_same_v<T, TernaryExpr>) {
                compile_ternary(node, result);
            }
        },
        expr.node);

    return result;
}

// Compile literal value into PUSH instruction
// Example: Literal(42) → PUSH_INT 42
void Compiler::compile_literal(const LiteralExpr& node, IRExpr& result) {
    const auto& lit = node.value;

    // Emit appropriate PUSH instruction based on literal type
    std::visit(
        [&](const auto& val) {
            using T = std::decay_t<decltype(val)>;

            if constexpr (std::is_same_v<T, int64_t>) {
                result.instructions.push_back({IRExpr::OpCode::PUSH_INT, val});
            } else if constexpr (std::is_same_v<T, double>) {
                result.instructions.push_back({IRExpr::OpCode::PUSH_DOUBLE, val});
            } else if constexpr (std::is_same_v<T, std::string>) {
                result.instructions.push_back({IRExpr::OpCode::PUSH_STRING, val});
            } else if constexpr (std::is_same_v<T, bool>) {
                result.instructions.push_back({IRExpr::OpCode::PUSH_BOOL, val});
            }
        },
        lit.value);
}

// Compile column reference into LOAD_COLUMN instruction
// Example: ColumnRef("age") → LOAD_COLUMN "age"
// Note: We store column NAME, not index
// The VM resolves names to indices at runtime (could be optimized)
void Compiler::compile_column_ref(const ColumnRef& node, IRExpr& result) {
    // Store column name as string operand
    // VM will look up this name in the current table and load the value
    result.instructions.push_back({IRExpr::OpCode::LOAD_COLUMN, node.name});
}

// Compile binary expression into bytecode
// Strategy: Emit code for left, then right, then operator (postfix notation)
//
// Example: age + 5
//   AST:      BinaryExpr(ADD, ColumnRef("age"), Literal(5))
//   Bytecode: [LOAD_COLUMN "age", PUSH_INT 5, ADD]
//   Stack:    [] → [age_value] → [age_value, 5] → [age_value + 5]
void Compiler::compile_binary(const BinaryExpr& node, IRExpr& result) {
    // Compile left operand (emits bytecode into result)
    IRExpr left = compile_expr(*node.left);
    result.instructions.insert(result.instructions.end(), left.instructions.begin(),
                               left.instructions.end());

    // Compile right operand (appends bytecode to result)
    IRExpr right = compile_expr(*node.right);
    result.instructions.insert(result.instructions.end(), right.instructions.begin(),
                               right.instructions.end());

    // Emit operator instruction (pops two values, pushes result)
    // Map AST operator enum to IR opcode
    IRExpr::OpCode op_code;
    switch (node.op) {
    case BinaryOp::Add:
        op_code = IRExpr::OpCode::ADD;
        break;
    case BinaryOp::Sub:
        op_code = IRExpr::OpCode::SUB;
        break;
    case BinaryOp::Mul:
        op_code = IRExpr::OpCode::MUL;
        break;
    case BinaryOp::Div:
        op_code = IRExpr::OpCode::DIV;
        break;
    case BinaryOp::Eq:
        op_code = IRExpr::OpCode::EQ;
        break;
    case BinaryOp::Neq:
        op_code = IRExpr::OpCode::NEQ;
        break;
    case BinaryOp::Lt:
        op_code = IRExpr::OpCode::LT;
        break;
    case BinaryOp::Gt:
        op_code = IRExpr::OpCode::GT;
        break;
    case BinaryOp::Lte:
        op_code = IRExpr::OpCode::LTE;
        break;
    case BinaryOp::Gte:
        op_code = IRExpr::OpCode::GTE;
        break;
    }

    // Operator instructions have no operand (they operate on stack)
    result.instructions.push_back({op_code, 0});
}

// Compile unary expression into bytecode
// Example: -age
//   AST:      UnaryExpr(NEG, ColumnRef("age"))
//   Bytecode: [LOAD_COLUMN "age", NEG]
//   Stack:    [] → [age_value] → [-age_value]
void Compiler::compile_unary(const UnaryExpr& node, IRExpr& result) {
    // Compile operand first
    IRExpr operand = compile_expr(*node.operand);
    result.instructions.insert(result.instructions.end(), operand.instructions.begin(),
                               operand.instructions.end());

    // Emit unary operator instruction (pops one value, pushes result)
    IRExpr::OpCode op_code;
    switch (node.op) {
    case UnaryOp::Neg:
        op_code = IRExpr::OpCode::NEG;
        break;  // Numeric negation
    case UnaryOp::Not:
        op_code = IRExpr::OpCode::NOT;
        break;  // Boolean negation
    }

    result.instructions.push_back({op_code, 0});
}

// Compile ternary conditional expression into bytecode
// Example: age > 30 ? "senior" : "junior"
//   AST:      TernaryExpr(BinaryExpr(GT, ...), Literal("senior"), Literal("junior"))
//   Bytecode: [LOAD_COLUMN "age", PUSH_INT 30, GT,
//              PUSH_STRING "senior", PUSH_STRING "junior", TERNARY]
//   Stack:    [] → [age_val] → [age_val, 30] → [bool]
//             → [bool, "senior"] → [bool, "senior", "junior"] → [result]
void Compiler::compile_ternary(const TernaryExpr& node, IRExpr& result) {
    // Compile condition expression (should evaluate to bool)
    IRExpr condition = compile_expr(*node.condition);
    result.instructions.insert(result.instructions.end(), condition.instructions.begin(),
                               condition.instructions.end());

    // Compile true branch (value if condition is true)
    IRExpr true_branch = compile_expr(*node.true_branch);
    result.instructions.insert(result.instructions.end(), true_branch.instructions.begin(),
                               true_branch.instructions.end());

    // Compile false branch (value if condition is false)
    IRExpr false_branch = compile_expr(*node.false_branch);
    result.instructions.insert(result.instructions.end(), false_branch.instructions.begin(),
                               false_branch.instructions.end());

    // Emit TERNARY instruction (pops 3 values: condition, true_val, false_val; pushes result)
    result.instructions.push_back({IRExpr::OpCode::TERNARY, 0});
}

// ============================================================================
// Vectorization Pattern Detection
// ============================================================================
// Detects simple filter patterns that can be vectorized
// Pattern: column comparison_op literal
// Examples: age > 30, name == "Alice", salary <= 50000

std::optional<PhysicalOp::VectorizedFilterOp> Compiler::try_vectorize_filter(const Expr& expr) {
    // Only handle binary expressions
    const auto* binary_node = std::get_if<BinaryExpr>(&expr.node);
    if (!binary_node) {
        return std::nullopt;
    }

    // Check if operator is a comparison
    BinaryOp op = binary_node->op;
    bool is_comparison = (op == BinaryOp::Gt || op == BinaryOp::Lt || op == BinaryOp::Gte ||
                          op == BinaryOp::Lte || op == BinaryOp::Eq || op == BinaryOp::Neq);
    if (!is_comparison) {
        return std::nullopt;
    }

    // Pattern 1: column op literal
    const auto* left_col = std::get_if<ColumnRef>(&binary_node->left->node);
    const auto* right_lit = std::get_if<LiteralExpr>(&binary_node->right->node);

    if (left_col && right_lit) {
        // Map AST operator to VectorOp
        VectorOp vec_op;
        switch (op) {
        case BinaryOp::Gt:
            vec_op = VectorOp::GT;
            break;
        case BinaryOp::Lt:
            vec_op = VectorOp::LT;
            break;
        case BinaryOp::Gte:
            vec_op = VectorOp::GTE;
            break;
        case BinaryOp::Lte:
            vec_op = VectorOp::LTE;
            break;
        case BinaryOp::Eq:
            vec_op = VectorOp::EQ;
            break;
        case BinaryOp::Neq:
            vec_op = VectorOp::NEQ;
            break;
        default:
            return std::nullopt;
        }

        // Extract literal value
        PhysicalOp::VectorizedFilterOp result;
        result.column_name = left_col->name;
        result.op = vec_op;

        // Store appropriate type of literal value
        const auto& lit_value = right_lit->value.value;
        if (std::holds_alternative<int64_t>(lit_value)) {
            result.value = std::get<int64_t>(lit_value);
        } else if (std::holds_alternative<double>(lit_value)) {
            result.value = std::get<double>(lit_value);
        } else if (std::holds_alternative<std::string>(lit_value)) {
            result.value = std::get<std::string>(lit_value);
        } else {
            return std::nullopt;  // Bool not supported for vectorized filters yet
        }

        return result;
    }

    // Pattern 2: literal op column (reverse operands)
    const auto* left_lit = std::get_if<LiteralExpr>(&binary_node->left->node);
    const auto* right_col = std::get_if<ColumnRef>(&binary_node->right->node);

    if (left_lit && right_col) {
        // Reverse the operator: 30 < age → age > 30
        VectorOp vec_op;
        switch (op) {
        case BinaryOp::Gt:
            vec_op = VectorOp::LT;
            break;  // 30 > age → age < 30
        case BinaryOp::Lt:
            vec_op = VectorOp::GT;
            break;  // 30 < age → age > 30
        case BinaryOp::Gte:
            vec_op = VectorOp::LTE;
            break;  // 30 >= age → age <= 30
        case BinaryOp::Lte:
            vec_op = VectorOp::GTE;
            break;  // 30 <= age → age >= 30
        case BinaryOp::Eq:
            vec_op = VectorOp::EQ;
            break;  // 30 == age → age == 30
        case BinaryOp::Neq:
            vec_op = VectorOp::NEQ;
            break;  // 30 != age → age != 30
        default:
            return std::nullopt;
        }

        PhysicalOp::VectorizedFilterOp result;
        result.column_name = right_col->name;
        result.op = vec_op;

        const auto& lit_value = left_lit->value.value;
        if (std::holds_alternative<int64_t>(lit_value)) {
            result.value = std::get<int64_t>(lit_value);
        } else if (std::holds_alternative<double>(lit_value)) {
            result.value = std::get<double>(lit_value);
        } else if (std::holds_alternative<std::string>(lit_value)) {
            result.value = std::get<std::string>(lit_value);
        } else {
            return std::nullopt;
        }

        return result;
    }

    // Complex expression - cannot vectorize
    return std::nullopt;
}

// ============================================================================
// Transform Vectorization Pattern Detection
// ============================================================================

// Try to vectorize simple arithmetic patterns
// Pattern: column arith_op column/literal
// Examples: price * quantity, value * 2, base + offset
std::optional<PhysicalOp::VectorizedTransformOp> Compiler::try_vectorize_arith_transform(
    const std::string& column_name, const Expr& expr) {
    // Only handle binary expressions
    const auto* binary_node = std::get_if<BinaryExpr>(&expr.node);
    if (!binary_node) {
        return std::nullopt;
    }

    // Check if operator is arithmetic
    BinaryOp op = binary_node->op;
    bool is_arithmetic =
        (op == BinaryOp::Add || op == BinaryOp::Sub || op == BinaryOp::Mul || op == BinaryOp::Div);
    if (!is_arithmetic) {
        return std::nullopt;
    }

    // Map to VectorArithOp
    VectorArithOp vec_op;
    switch (op) {
    case BinaryOp::Add:
        vec_op = VectorArithOp::ADD;
        break;
    case BinaryOp::Sub:
        vec_op = VectorArithOp::SUB;
        break;
    case BinaryOp::Mul:
        vec_op = VectorArithOp::MUL;
        break;
    case BinaryOp::Div:
        vec_op = VectorArithOp::DIV;
        break;
    default:
        return std::nullopt;
    }

    // BOTH operands must be simple (ColumnRef OR LiteralExpr)
    const auto* left_col = std::get_if<ColumnRef>(&binary_node->left->node);
    const auto* left_lit = std::get_if<LiteralExpr>(&binary_node->left->node);
    const auto* right_col = std::get_if<ColumnRef>(&binary_node->right->node);
    const auto* right_lit = std::get_if<LiteralExpr>(&binary_node->right->node);

    if (!left_col && !left_lit)
        return std::nullopt;  // Nested - bail out
    if (!right_col && !right_lit)
        return std::nullopt;  // Nested - bail out

    PhysicalOp::VectorizedTransformOp result;
    result.column_name = column_name;
    result.op = vec_op;

    // Process left operand
    if (left_col) {
        result.is_left_column = true;
        result.left_column_name = left_col->name;
    } else {
        result.is_left_column = false;
        const auto& lit_value = left_lit->value.value;
        if (std::holds_alternative<int64_t>(lit_value)) {
            result.left_scalar = std::get<int64_t>(lit_value);
        } else if (std::holds_alternative<double>(lit_value)) {
            result.left_scalar = std::get<double>(lit_value);
        } else {
            return std::nullopt;  // Only numeric
        }
    }

    // Process right operand
    if (right_col) {
        result.is_right_column = true;
        result.right_column_name = right_col->name;
    } else {
        result.is_right_column = false;
        const auto& lit_value = right_lit->value.value;
        if (std::holds_alternative<int64_t>(lit_value)) {
            result.right_scalar = std::get<int64_t>(lit_value);
        } else if (std::holds_alternative<double>(lit_value)) {
            result.right_scalar = std::get<double>(lit_value);
        } else {
            return std::nullopt;
        }
    }

    // Infer result type (promote to double if any operand is double)
    bool has_double_literal = false;
    if (!result.is_left_column && std::holds_alternative<double>(result.left_scalar))
        has_double_literal = true;
    if (!result.is_right_column && std::holds_alternative<double>(result.right_scalar))
        has_double_literal = true;

    // IMPORTANT: Reject mixed-type operations (would need type coercion)
    // If we have a double literal but we're referencing columns, we'd need to know
    // the column types at compile time, which we don't. So conservatively reject.
    // This falls back to scalar evaluation which handles type coercion properly.
    if (has_double_literal && (result.is_left_column || result.is_right_column)) {
        // Mixed types - would need runtime type coercion
        // Example: INT64_column * 0.9 (DOUBLE scalar)
        // Fall back to scalar path which handles this
        return std::nullopt;
    }

    result.result_type = has_double_literal ? ColumnType::DOUBLE : ColumnType::INT64;

    return result;
}

// Try to vectorize ternary patterns with depth limit
// Pattern: condition ? true_val : false_val
// Depth limit prevents exponential explosion
std::optional<PhysicalOp::VectorizedTernaryTransformOp> Compiler::try_vectorize_ternary_transform(
    const std::string& column_name, const Expr& expr, int max_depth) {
    if (max_depth <= 0) {
        return std::nullopt;  // Hit depth limit
    }

    // Only handle ternary expressions
    const auto* ternary_node = std::get_if<TernaryExpr>(&expr.node);
    if (!ternary_node) {
        return std::nullopt;
    }

    // Try to vectorize the condition (must be a simple comparison)
    auto condition_vec = try_vectorize_filter(*ternary_node->condition);
    if (!condition_vec.has_value()) {
        return std::nullopt;  // Condition too complex
    }

    PhysicalOp::VectorizedTernaryTransformOp result;
    result.column_name = column_name;
    result.condition = condition_vec.value();

    // Process true branch (must be simple: ColumnRef or LiteralExpr)
    const auto* true_col = std::get_if<ColumnRef>(&ternary_node->true_branch->node);
    const auto* true_lit = std::get_if<LiteralExpr>(&ternary_node->true_branch->node);

    if (true_col) {
        result.is_true_column = true;
        result.true_column_name = true_col->name;
    } else if (true_lit) {
        result.is_true_column = false;
        const auto& lit_value = true_lit->value.value;
        if (std::holds_alternative<int64_t>(lit_value)) {
            result.true_scalar = std::get<int64_t>(lit_value);
        } else if (std::holds_alternative<double>(lit_value)) {
            result.true_scalar = std::get<double>(lit_value);
        } else if (std::holds_alternative<std::string>(lit_value)) {
            result.true_scalar = std::get<std::string>(lit_value);
        } else {
            return std::nullopt;
        }
    } else {
        return std::nullopt;  // True branch too complex
    }

    // Process false branch (must be simple: ColumnRef or LiteralExpr)
    const auto* false_col = std::get_if<ColumnRef>(&ternary_node->false_branch->node);
    const auto* false_lit = std::get_if<LiteralExpr>(&ternary_node->false_branch->node);

    if (false_col) {
        result.is_false_column = true;
        result.false_column_name = false_col->name;
    } else if (false_lit) {
        result.is_false_column = false;
        const auto& lit_value = false_lit->value.value;
        if (std::holds_alternative<int64_t>(lit_value)) {
            result.false_scalar = std::get<int64_t>(lit_value);
        } else if (std::holds_alternative<double>(lit_value)) {
            result.false_scalar = std::get<double>(lit_value);
        } else if (std::holds_alternative<std::string>(lit_value)) {
            result.false_scalar = std::get<std::string>(lit_value);
        } else {
            return std::nullopt;
        }
    } else {
        return std::nullopt;  // False branch too complex
    }

    // Infer result type from branches
    // If either is string, result is string
    // If either is double, result is double
    // Otherwise, int
    ColumnType result_type = ColumnType::INT64;
    if (!result.is_true_column && std::holds_alternative<std::string>(result.true_scalar))
        result_type = ColumnType::STRING;
    if (!result.is_false_column && std::holds_alternative<std::string>(result.false_scalar))
        result_type = ColumnType::STRING;
    if (result_type != ColumnType::STRING) {
        if (!result.is_true_column && std::holds_alternative<double>(result.true_scalar))
            result_type = ColumnType::DOUBLE;
        if (!result.is_false_column && std::holds_alternative<double>(result.false_scalar))
            result_type = ColumnType::DOUBLE;
    }
    result.result_type = result_type;

    return result;
}

}  // namespace joy
