#include "lexer.hpp"

#include <cctype>
#include <unordered_map>

namespace joy {

// ============================================================================
// Keyword Map
// ============================================================================
// Maps string literals to their corresponding keyword token types.
// This allows O(1) lookup to distinguish keywords from identifiers.
// Example: "from" -> TokenType::FROM, but "age" -> TokenType::IDENT
static const std::unordered_map<std::string, TokenType> keywords = {
    {"from", TokenType::FROM},   {"filter", TokenType::FILTER}, {"select", TokenType::SELECT},
    {"write", TokenType::WRITE}, {"not", TokenType::NOT},
};

// ============================================================================
// Lexer Implementation
// ============================================================================

// Constructor: Takes ownership of source string via move semantics
// Initializes position tracking (line=1, column=1, current=0)
Lexer::Lexer(std::string source) : source_(std::move(source)) {}

// Main entry point: Converts entire source string into a vector of tokens
// This is a single-pass lexer that processes left-to-right
std::vector<Token> Lexer::tokenize() {
    std::vector<Token> tokens;

    // Scan tokens until we reach end of source
    while (!is_at_end()) {
        start_ = current_;  // Mark start of next token
        Token token = scan_token();

        // Skip error tokens (could collect them for error reporting instead)
        if (token.type != TokenType::ERROR) {
            tokens.push_back(token);
        }
    }

    // Always end with exactly one EOF token
    // This guard prevents duplicate EOF if scan_token() already returned one
    // (can happen when whitespace at end causes skip_whitespace to reach EOF)
    if (tokens.empty() || tokens.back().type != TokenType::END_OF_FILE) {
        tokens.push_back(Token{TokenType::END_OF_FILE, "", line_, column_});
    }
    return tokens;
}

// Scans a single token from current position
// Returns the token and advances current_ past it
Token Lexer::scan_token() {
    skip_whitespace();  // Ignore whitespace and comments first

    start_ = current_;  // Mark where this token begins

    if (is_at_end()) {
        return make_token(TokenType::END_OF_FILE);
    }

    char c = advance();  // Consume first character

    // Identifiers and keywords: [a-zA-Z_][a-zA-Z0-9_]*
    if (std::isalpha(c) || c == '_') {
        return scan_identifier();
    }

    // Numbers: [0-9]+ or [0-9]+.[0-9]+
    if (std::isdigit(c)) {
        return scan_number();
    }

    // String literals: "..."
    if (c == '"') {
        return scan_string();
    }

    // Single-character and two-character operators
    switch (c) {
    // Single-character operators (unambiguous)
    case '+':
        return make_token(TokenType::PLUS);
    case '-':
        return make_token(TokenType::MINUS);
    case '*':
        return make_token(TokenType::STAR);
    case '/':
        return make_token(TokenType::SLASH);
    case ',':
        return make_token(TokenType::COMMA);
    case '(':
        return make_token(TokenType::LPAREN);
    case ')':
        return make_token(TokenType::RPAREN);

    // Two-character operators (need lookahead)
    case '<':
        // Could be '<' or '<='
        if (match('='))
            return make_token(TokenType::LESS_EQUAL);
        return make_token(TokenType::LESS);
    case '>':
        // Could be '>' or '>='
        if (match('='))
            return make_token(TokenType::GREATER_EQUAL);
        return make_token(TokenType::GREATER);
    case '=':
        // Must be '==' (single '=' is not allowed in our language)
        if (match('='))
            return make_token(TokenType::EQUAL_EQUAL);
        return make_error("Unexpected character '='");
    case '!':
        // Must be '!=' (single '!' is not allowed, use 'not')
        if (match('='))
            return make_token(TokenType::BANG_EQUAL);
        return make_error("Unexpected character '!'");
    }

    // If we get here, it's an unrecognized character
    return make_error("Unexpected character");
}

// ============================================================================
// Character Navigation Utilities
// ============================================================================

// Check if we've consumed all input
bool Lexer::is_at_end() const {
    return current_ >= source_.size();
}

// Consume current character and return it
// Also increments column counter for error reporting
char Lexer::advance() {
    column_++;
    return source_[current_++];
}

// Look at current character without consuming it
// Returns '\0' if at end (sentinel value)
char Lexer::peek() const {
    if (is_at_end())
        return '\0';
    return source_[current_];
}

// Look ahead two characters (for number parsing: "1.5")
// Used to distinguish "1." from "1.5" (only latter is a double)
char Lexer::peek_next() const {
    if (current_ + 1 >= source_.size())
        return '\0';
    return source_[current_ + 1];
}

// Conditional advance: consume current char only if it matches expected
// Used for two-character operators like "<=" and "=="
// Returns true if matched and consumed, false otherwise
bool Lexer::match(char expected) {
    if (is_at_end() || source_[current_] != expected) {
        return false;
    }
    current_++;
    column_++;
    return true;
}

// ============================================================================
// Token Creation Helpers
// ============================================================================

// Creates a token from the current lexeme (start_ to current_)
// Calculates column position by subtracting lexeme length from current column
Token Lexer::make_token(TokenType type) {
    std::string lexeme = source_.substr(start_, current_ - start_);
    return Token{type, lexeme, line_, column_ - static_cast<int>(lexeme.size())};
}

// Creates an error token with a message
// Used for lexical errors (unterminated strings, unexpected characters, etc.)
Token Lexer::make_error(const std::string& message) {
    return Token{TokenType::ERROR, message, line_, column_};
}

// ============================================================================
// Specialized Token Scanners
// ============================================================================

// Scans a string literal: "hello world"
// Supports multi-line strings (tracks line numbers)
// Returns error if string is not terminated before EOF
Token Lexer::scan_string() {
    // Consume characters until closing quote or EOF
    while (peek() != '"' && !is_at_end()) {
        // Track newlines inside strings for error reporting
        if (peek() == '\n') {
            line_++;
            column_ = 0;
        }
        advance();
    }

    // Error: reached end of file without closing quote
    if (is_at_end()) {
        return make_error("Unterminated string");
    }

    advance();  // Consume closing quote

    // Extract string content (excluding quotes)
    // start_ points to opening ", current_ is after closing "
    std::string value = source_.substr(start_ + 1, current_ - start_ - 2);
    Token token = make_token(TokenType::STRING);
    token.lexeme = value;  // Store actual string value, not including quotes
    return token;
}

// Scans a number: integer (42) or double (3.14)
// Grammar: [0-9]+ ("." [0-9]+)?
Token Lexer::scan_number() {
    // Consume all digits in integer part
    while (std::isdigit(peek())) {
        advance();
    }

    // Check for decimal point followed by digits
    // peek_next() ensures we don't treat "1." as a double
    bool is_double = false;
    if (peek() == '.' && std::isdigit(peek_next())) {
        is_double = true;
        advance();  // Consume '.'

        // Consume all digits in fractional part
        while (std::isdigit(peek())) {
            advance();
        }
    }

    Token token = make_token(TokenType::NUMBER);
    token.is_double = is_double;

    // Parse the numeric value into the appropriate union field
    if (is_double) {
        token.double_value = std::stod(token.lexeme);
    } else {
        token.int_value = std::stoll(token.lexeme);
    }

    return token;
}

// Scans an identifier or keyword
// Grammar: [a-zA-Z_][a-zA-Z0-9_]*
// First character already consumed by scan_token()
Token Lexer::scan_identifier() {
    // Consume alphanumeric characters and underscores
    while (std::isalnum(peek()) || peek() == '_') {
        advance();
    }

    // Check if this identifier is actually a keyword
    TokenType type = identifier_type();
    return make_token(type);
}

// Determines if current lexeme is a keyword or identifier
// Uses the keyword map for O(1) lookup
TokenType Lexer::identifier_type() const {
    std::string text = source_.substr(start_, current_ - start_);
    auto it = keywords.find(text);
    if (it != keywords.end()) {
        return it->second;  // It's a keyword
    }
    return TokenType::IDENT;  // It's a regular identifier
}

// ============================================================================
// Whitespace and Comment Handling
// ============================================================================

// Skips whitespace and comments until a significant character is found
// Tracks line/column for error reporting
// Comments: # to end of line (like Python)
void Lexer::skip_whitespace() {
    while (!is_at_end()) {
        char c = peek();
        switch (c) {
        case ' ':
        case '\r':
        case '\t':
            advance();  // Skip whitespace
            break;
        case '\n':
            line_++;      // Track line numbers
            column_ = 0;  // Reset column at start of line
            advance();
            break;
        case '#':
            // Comment: skip to end of line
            while (peek() != '\n' && !is_at_end()) {
                advance();
            }
            break;
        default:
            return;  // Found a non-whitespace character
        }
    }
}

// ============================================================================
// Debugging Utility
// ============================================================================

// Converts TokenType enum to string for debugging/error messages
// Used by test utilities and error reporting
const char* token_type_to_string(TokenType type) {
    switch (type) {
    case TokenType::FROM:
        return "FROM";
    case TokenType::FILTER:
        return "FILTER";
    case TokenType::SELECT:
        return "SELECT";
    case TokenType::WRITE:
        return "WRITE";
    case TokenType::NOT:
        return "NOT";
    case TokenType::IDENT:
        return "IDENT";
    case TokenType::NUMBER:
        return "NUMBER";
    case TokenType::STRING:
        return "STRING";
    case TokenType::PLUS:
        return "PLUS";
    case TokenType::MINUS:
        return "MINUS";
    case TokenType::STAR:
        return "STAR";
    case TokenType::SLASH:
        return "SLASH";
    case TokenType::EQUAL_EQUAL:
        return "EQUAL_EQUAL";
    case TokenType::BANG_EQUAL:
        return "BANG_EQUAL";
    case TokenType::LESS:
        return "LESS";
    case TokenType::GREATER:
        return "GREATER";
    case TokenType::LESS_EQUAL:
        return "LESS_EQUAL";
    case TokenType::GREATER_EQUAL:
        return "GREATER_EQUAL";
    case TokenType::COMMA:
        return "COMMA";
    case TokenType::LPAREN:
        return "LPAREN";
    case TokenType::RPAREN:
        return "RPAREN";
    case TokenType::END_OF_FILE:
        return "EOF";
    case TokenType::ERROR:
        return "ERROR";
    default:
        return "UNKNOWN";
    }
}

}  // namespace joy
