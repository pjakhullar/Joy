#pragma once

#include <stdexcept>
#include <vector>

#include "ast.hpp"
#include "lexer.hpp"

namespace joy {

// ============================================================================
// Parser Exception
// ============================================================================

class ParseError : public std::runtime_error {
public:
    ParseError(const std::string& message, int line, int column)
        : std::runtime_error(message), line_(line), column_(column) {}

    int line() const {
        return line_;
    }
    int column() const {
        return column_;
    }

private:
    int line_;
    int column_;
};

// ============================================================================
// Recursive Descent Parser
// ============================================================================

class Parser {
public:
    explicit Parser(std::vector<Token> tokens);

    // Parse entire program
    Program parse();

private:
    std::vector<Token> tokens_;
    size_t current_ = 0;

    // Recursive descent methods (following grammar)
    Program parse_program();
    std::vector<Stmt> parse_pipeline();
    Stmt parse_from_stmt();
    Stmt parse_filter_stmt();
    Stmt parse_select_stmt();
    Stmt parse_write_stmt();

    // Expression parsing (precedence climbing)
    std::unique_ptr<Expr> parse_expr();
    std::unique_ptr<Expr> parse_equality();
    std::unique_ptr<Expr> parse_comparison();
    std::unique_ptr<Expr> parse_term();
    std::unique_ptr<Expr> parse_factor();
    std::unique_ptr<Expr> parse_unary();
    std::unique_ptr<Expr> parse_primary();

    // Column list parsing
    std::vector<std::string> parse_column_list();

    // Token utilities
    Token peek() const;
    Token previous() const;
    bool is_at_end() const;
    bool check(TokenType type) const;
    Token advance();
    bool match(TokenType type);
    Token consume(TokenType type, const std::string& message);

    // Error handling
    [[noreturn]] void error(const std::string& message);
};

}  // namespace joy
