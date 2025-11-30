#include "parser.hpp"

namespace joy {

// ============================================================================
// Parser Implementation
// ============================================================================

Parser::Parser(std::vector<Token> tokens) : tokens_(std::move(tokens)) {}

Program Parser::parse() {
    return parse_program();
}

Program Parser::parse_program() {
    auto stmts = parse_pipeline();
    consume(TokenType::END_OF_FILE, "Expected end of file");
    return Program{std::move(stmts)};
}

std::vector<Stmt> Parser::parse_pipeline() {
    std::vector<Stmt> statements;

    // First statement must be FROM
    statements.push_back(parse_from_stmt());

    // Parse remaining operations
    while (!is_at_end()) {
        if (check(TokenType::FILTER)) {
            statements.push_back(parse_filter_stmt());
        } else if (check(TokenType::SELECT)) {
            statements.push_back(parse_select_stmt());
        } else if (check(TokenType::WRITE)) {
            statements.push_back(parse_write_stmt());
        } else {
            break;
        }
    }

    return statements;
}

Stmt Parser::parse_from_stmt() {
    consume(TokenType::FROM, "Expected 'from'");
    Token filepath = consume(TokenType::STRING, "Expected string literal for file path");

    Stmt stmt;
    stmt.node = FromStmt{filepath.lexeme};
    return stmt;
}

Stmt Parser::parse_filter_stmt() {
    consume(TokenType::FILTER, "Expected 'filter'");
    auto condition = parse_expr();

    Stmt stmt;
    stmt.node = FilterStmt{std::move(condition)};
    return stmt;
}

Stmt Parser::parse_select_stmt() {
    consume(TokenType::SELECT, "Expected 'select'");
    auto columns = parse_column_list();

    Stmt stmt;
    stmt.node = SelectStmt{std::move(columns)};
    return stmt;
}

Stmt Parser::parse_write_stmt() {
    consume(TokenType::WRITE, "Expected 'write'");
    Token filepath = consume(TokenType::STRING, "Expected string literal for file path");

    Stmt stmt;
    stmt.node = WriteStmt{filepath.lexeme};
    return stmt;
}

std::unique_ptr<Expr> Parser::parse_expr() {
    return parse_equality();
}

std::unique_ptr<Expr> Parser::parse_equality() {
    auto expr = parse_comparison();

    while (match(TokenType::EQUAL_EQUAL) || match(TokenType::BANG_EQUAL)) {
        Token op = previous();
        auto right = parse_comparison();

        BinaryOp bin_op = (op.type == TokenType::EQUAL_EQUAL) ? BinaryOp::Eq : BinaryOp::Neq;
        expr = make_binary(bin_op, std::move(expr), std::move(right));
    }

    return expr;
}

std::unique_ptr<Expr> Parser::parse_comparison() {
    auto expr = parse_term();

    while (match(TokenType::LESS) || match(TokenType::GREATER) ||
           match(TokenType::LESS_EQUAL) || match(TokenType::GREATER_EQUAL)) {
        Token op = previous();
        auto right = parse_term();

        BinaryOp bin_op;
        switch (op.type) {
            case TokenType::LESS: bin_op = BinaryOp::Lt; break;
            case TokenType::GREATER: bin_op = BinaryOp::Gt; break;
            case TokenType::LESS_EQUAL: bin_op = BinaryOp::Lte; break;
            case TokenType::GREATER_EQUAL: bin_op = BinaryOp::Gte; break;
            default: error("Invalid comparison operator");
        }

        expr = make_binary(bin_op, std::move(expr), std::move(right));
    }

    return expr;
}

std::unique_ptr<Expr> Parser::parse_term() {
    auto expr = parse_factor();

    while (match(TokenType::PLUS) || match(TokenType::MINUS)) {
        Token op = previous();
        auto right = parse_factor();

        BinaryOp bin_op = (op.type == TokenType::PLUS) ? BinaryOp::Add : BinaryOp::Sub;
        expr = make_binary(bin_op, std::move(expr), std::move(right));
    }

    return expr;
}

std::unique_ptr<Expr> Parser::parse_factor() {
    auto expr = parse_unary();

    while (match(TokenType::STAR) || match(TokenType::SLASH)) {
        Token op = previous();
        auto right = parse_unary();

        BinaryOp bin_op = (op.type == TokenType::STAR) ? BinaryOp::Mul : BinaryOp::Div;
        expr = make_binary(bin_op, std::move(expr), std::move(right));
    }

    return expr;
}

std::unique_ptr<Expr> Parser::parse_unary() {
    if (match(TokenType::MINUS)) {
        auto operand = parse_unary();
        return make_unary(UnaryOp::Neg, std::move(operand));
    }

    if (match(TokenType::NOT)) {
        auto operand = parse_unary();
        return make_unary(UnaryOp::Not, std::move(operand));
    }

    return parse_primary();
}

std::unique_ptr<Expr> Parser::parse_primary() {
    // Number literal
    if (match(TokenType::NUMBER)) {
        Token token = previous();
        if (token.is_double) {
            return make_literal(token.double_value);
        } else {
            return make_literal(token.int_value);
        }
    }

    // String literal
    if (match(TokenType::STRING)) {
        Token token = previous();
        return make_literal(token.lexeme);
    }

    // Identifier (column reference)
    if (match(TokenType::IDENT)) {
        Token token = previous();
        return make_column_ref(token.lexeme);
    }

    // Parenthesized expression
    if (match(TokenType::LPAREN)) {
        auto expr = parse_expr();
        consume(TokenType::RPAREN, "Expected ')' after expression");
        return expr;
    }

    error("Expected expression");
}

std::vector<std::string> Parser::parse_column_list() {
    std::vector<std::string> columns;

    Token col = consume(TokenType::IDENT, "Expected column name");
    columns.push_back(col.lexeme);

    while (match(TokenType::COMMA)) {
        col = consume(TokenType::IDENT, "Expected column name after ','");
        columns.push_back(col.lexeme);
    }

    return columns;
}

// ============================================================================
// Token Utilities
// ============================================================================

Token Parser::peek() const {
    return tokens_[current_];
}

Token Parser::previous() const {
    return tokens_[current_ - 1];
}

bool Parser::is_at_end() const {
    return peek().type == TokenType::END_OF_FILE;
}

bool Parser::check(TokenType type) const {
    return peek().type == type;
}

Token Parser::advance() {
    if (!is_at_end()) current_++;
    return previous();
}

bool Parser::match(TokenType type) {
    if (check(type)) {
        advance();
        return true;
    }
    return false;
}

Token Parser::consume(TokenType type, const std::string& message) {
    if (check(type)) return advance();
    error(message);
}

void Parser::error(const std::string& message) {
    Token token = peek();
    throw ParseError(message, token.line, token.column);
}

} // namespace joy
