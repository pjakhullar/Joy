#include "parser.hpp"

namespace joy {

// ============================================================================
// Parser Implementation - Recursive Descent
// ============================================================================
// This is a recursive descent parser that directly follows the grammar
// Each non-terminal in the grammar becomes a parse_*() method
// The parser consumes tokens from left to right, building an AST

// Constructor: Takes ownership of token vector
Parser::Parser(std::vector<Token> tokens) : tokens_(std::move(tokens)) {}

// Main entry point: Parse entire program
Program Parser::parse() {
    return parse_program();
}

// Parse the top-level program structure
// Grammar: program ::= pipeline EOF
Program Parser::parse_program() {
    auto stmts = parse_pipeline();
    consume(TokenType::END_OF_FILE, "Expected end of file");
    return Program{std::move(stmts)};
}

// Parse a pipeline of operations
// Grammar: pipeline ::= from_stmt operation*
// where operation ::= filter_stmt | select_stmt | write_stmt
//
// Example:
//   from "data.csv"
//   filter age > 30
//   select name
//   write "out.csv"
std::vector<Stmt> Parser::parse_pipeline() {
    std::vector<Stmt> statements;

    // First statement MUST be FROM (data source)
    statements.push_back(parse_from_stmt());

    // Parse remaining operations until we hit EOF or unrecognized token
    while (!is_at_end()) {
        if (check(TokenType::FILTER)) {
            statements.push_back(parse_filter_stmt());
        } else if (check(TokenType::SELECT)) {
            statements.push_back(parse_select_stmt());
        } else if (check(TokenType::WRITE)) {
            statements.push_back(parse_write_stmt());
        } else {
            // Not a recognized statement, stop parsing pipeline
            break;
        }
    }

    return statements;
}

// ============================================================================
// Statement Parsers
// ============================================================================
// Each statement type has its own parser method
// These methods consume tokens and build AST nodes

// Parse: from "filepath.csv"
// Grammar: from_stmt ::= FROM STRING
Stmt Parser::parse_from_stmt() {
    consume(TokenType::FROM, "Expected 'from'");
    Token filepath = consume(TokenType::STRING, "Expected string literal for file path");

    Stmt stmt;
    stmt.node = FromStmt{filepath.lexeme};
    return stmt;
}

// Parse: filter <expression>
// Grammar: filter_stmt ::= FILTER expr
// Example: filter age > 30
Stmt Parser::parse_filter_stmt() {
    consume(TokenType::FILTER, "Expected 'filter'");
    auto condition = parse_expr();  // Parse boolean expression

    Stmt stmt;
    stmt.node = FilterStmt{std::move(condition)};
    return stmt;
}

// Parse: select col1, col2, ...
// Grammar: select_stmt ::= SELECT column_list
// Example: select name, age, salary
Stmt Parser::parse_select_stmt() {
    consume(TokenType::SELECT, "Expected 'select'");
    auto columns = parse_column_list();

    Stmt stmt;
    stmt.node = SelectStmt{std::move(columns)};
    return stmt;
}

// Parse: write "filepath.csv"
// Grammar: write_stmt ::= WRITE STRING
Stmt Parser::parse_write_stmt() {
    consume(TokenType::WRITE, "Expected 'write'");
    Token filepath = consume(TokenType::STRING, "Expected string literal for file path");

    Stmt stmt;
    stmt.node = WriteStmt{filepath.lexeme};
    return stmt;
}

// ============================================================================
// Expression Parsers - Precedence Climbing
// ============================================================================
// Expressions are parsed using precedence climbing (recursive descent variant)
// Each precedence level gets its own method
//
// Precedence hierarchy (lowest to highest):
//   equality:    ==, !=
//   comparison:  <, >, <=, >=
//   term:        +, -
//   factor:      *, /
//   unary:       -, not
//   primary:     literals, identifiers, parentheses

// Entry point for expression parsing
// Grammar: expr ::= equality
std::unique_ptr<Expr> Parser::parse_expr() {
    return parse_equality();
}

// Parse equality operators: == !=
// Grammar: equality ::= comparison ( ("==" | "!=") comparison )*
// Example: age == 30, name != "Alice"
// Left-associative: a == b == c parses as (a == b) == c
std::unique_ptr<Expr> Parser::parse_equality() {
    auto expr = parse_comparison();  // Parse left operand

    // Handle chain of equality operators (left-associative)
    while (match(TokenType::EQUAL_EQUAL) || match(TokenType::BANG_EQUAL)) {
        Token op = previous();
        auto right = parse_comparison();  // Parse right operand

        BinaryOp bin_op = (op.type == TokenType::EQUAL_EQUAL) ? BinaryOp::Eq : BinaryOp::Neq;
        expr = make_binary(bin_op, std::move(expr), std::move(right));
    }

    return expr;
}

// Parse comparison operators: < > <= >=
// Grammar: comparison ::= term ( ("<" | ">" | "<=" | ">=") term )*
// Example: age > 30, salary <= 50000
std::unique_ptr<Expr> Parser::parse_comparison() {
    auto expr = parse_term();

    while (match(TokenType::LESS) || match(TokenType::GREATER) || match(TokenType::LESS_EQUAL) ||
           match(TokenType::GREATER_EQUAL)) {
        Token op = previous();
        auto right = parse_term();

        // Map token type to AST operator
        BinaryOp bin_op;
        switch (op.type) {
        case TokenType::LESS:
            bin_op = BinaryOp::Lt;
            break;
        case TokenType::GREATER:
            bin_op = BinaryOp::Gt;
            break;
        case TokenType::LESS_EQUAL:
            bin_op = BinaryOp::Lte;
            break;
        case TokenType::GREATER_EQUAL:
            bin_op = BinaryOp::Gte;
            break;
        default:
            error("Invalid comparison operator");
        }

        expr = make_binary(bin_op, std::move(expr), std::move(right));
    }

    return expr;
}

// Parse additive operators: + -
// Grammar: term ::= factor ( ("+" | "-") factor )*
// Example: age + 5, salary - 1000
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

// Parse multiplicative operators: * /
// Grammar: factor ::= unary ( ("*" | "/") unary )*
// Example: salary * 1.1, hours / 40
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

// Parse unary operators: - not
// Grammar: unary ::= ("-" | "not") unary | primary
// Example: -age, not active
// Recursive to handle chains: --x, not not active
std::unique_ptr<Expr> Parser::parse_unary() {
    if (match(TokenType::MINUS)) {
        auto operand = parse_unary();  // Recursive for chaining
        return make_unary(UnaryOp::Neg, std::move(operand));
    }

    if (match(TokenType::NOT)) {
        auto operand = parse_unary();
        return make_unary(UnaryOp::Not, std::move(operand));
    }

    return parse_primary();
}

// Parse primary expressions (atoms)
// Grammar: primary ::= NUMBER | STRING | IDENT | "(" expr ")"
// These are the "leaf nodes" of the expression tree
std::unique_ptr<Expr> Parser::parse_primary() {
    // Number literal: 42 or 3.14
    if (match(TokenType::NUMBER)) {
        Token token = previous();
        if (token.is_double) {
            return make_literal(token.double_value);
        } else {
            return make_literal(token.int_value);
        }
    }

    // String literal: "hello"
    if (match(TokenType::STRING)) {
        Token token = previous();
        return make_literal(token.lexeme);
    }

    // Identifier (column reference): age, name, etc.
    if (match(TokenType::IDENT)) {
        Token token = previous();
        return make_column_ref(token.lexeme);
    }

    // Parenthesized expression: (age + 5)
    // Allows overriding precedence: (a + b) * c
    if (match(TokenType::LPAREN)) {
        auto expr = parse_expr();  // Parse inner expression
        consume(TokenType::RPAREN, "Expected ')' after expression");
        return expr;
    }

    // If we get here, we found an unexpected token
    error("Expected expression");
}

// ============================================================================
// Helper: Parse Column List
// ============================================================================

// Parse comma-separated list of column names
// Grammar: column_list ::= IDENT ("," IDENT)*
// Example: name, age, salary
std::vector<std::string> Parser::parse_column_list() {
    std::vector<std::string> columns;

    // First column (required)
    Token col = consume(TokenType::IDENT, "Expected column name");
    columns.push_back(col.lexeme);

    // Additional columns (optional, comma-separated)
    while (match(TokenType::COMMA)) {
        col = consume(TokenType::IDENT, "Expected column name after ','");
        columns.push_back(col.lexeme);
    }

    return columns;
}

// ============================================================================
// Token Navigation Utilities
// ============================================================================
// These methods manage the current position in the token stream
// Similar to lexer's character navigation, but for tokens

// Look at current token without consuming it
Token Parser::peek() const {
    return tokens_[current_];
}

// Look at previously consumed token
// Used after match() to get the matched token
Token Parser::previous() const {
    return tokens_[current_ - 1];
}

// Check if we've reached end of token stream
bool Parser::is_at_end() const {
    return peek().type == TokenType::END_OF_FILE;
}

// Check if current token matches given type (without consuming)
// Returns false if at EOF (even if checking for EOF)
bool Parser::check(TokenType type) const {
    return peek().type == type;
}

// Consume and return current token
// Advances current position unless at EOF
Token Parser::advance() {
    if (!is_at_end())
        current_++;
    return previous();
}

// Conditional consume: advance if current token matches type
// Returns true if matched and consumed, false otherwise
// This is the primary way to consume expected tokens
bool Parser::match(TokenType type) {
    if (check(type)) {
        advance();
        return true;
    }
    return false;
}

// Consume token of expected type or throw error
// This is used when a token is required (not optional)
// Example: consume(LPAREN, "Expected '('") ensures we have a left paren
Token Parser::consume(TokenType type, const std::string& message) {
    if (check(type))
        return advance();
    error(message);
}

// ============================================================================
// Error Handling
// ============================================================================

// Throw parse error with current token's location
// [[noreturn]] tells compiler this function never returns normally
void Parser::error(const std::string& message) {
    Token token = peek();
    throw ParseError(message, token.line, token.column);
}

}  // namespace joy
