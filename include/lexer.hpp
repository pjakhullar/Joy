#pragma once

#include <string>
#include <vector>

namespace joy {

// ============================================================================
// Token Types
// ============================================================================

enum class TokenType {
    // Keywords
    FROM,
    FILTER,
    SELECT,
    WRITE,
    NOT,

    // Literals
    IDENT,
    NUMBER,
    STRING,

    // Operators
    PLUS,       // +
    MINUS,      // -
    STAR,       // *
    SLASH,      // /
    EQUAL_EQUAL,// ==
    BANG_EQUAL, // !=
    LESS,       // <
    GREATER,    // >
    LESS_EQUAL, // <=
    GREATER_EQUAL, // >=

    // Punctuation
    COMMA,      // ,
    LPAREN,     // (
    RPAREN,     // )

    // Special
    END_OF_FILE,
    ERROR
};

struct Token {
    TokenType type;
    std::string lexeme;
    int line;
    int column;

    // Parsed value (for numbers)
    union {
        int64_t int_value;
        double double_value;
    };
    bool is_double = false;
};

// ============================================================================
// Lexer
// ============================================================================

class Lexer {
public:
    explicit Lexer(std::string source);

    // Tokenize entire source
    std::vector<Token> tokenize();

private:
    std::string source_;
    size_t start_ = 0;    // Start of current token
    size_t current_ = 0;  // Current position
    int line_ = 1;
    int column_ = 1;

    // Single token scanning
    Token scan_token();

    // Character utilities
    bool is_at_end() const;
    char advance();
    char peek() const;
    char peek_next() const;
    bool match(char expected);

    // Token creation
    Token make_token(TokenType type);
    Token make_error(const std::string& message);

    // Scanning specific token types
    Token scan_string();
    Token scan_number();
    Token scan_identifier();

    // Keyword checking
    TokenType identifier_type() const;

    void skip_whitespace();
};

// ============================================================================
// Utility Functions
// ============================================================================

const char* token_type_to_string(TokenType type);

} // namespace joy
