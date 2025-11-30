#include "lexer.hpp"
#include <cctype>
#include <unordered_map>

namespace joy {

// ============================================================================
// Keyword Map
// ============================================================================

static const std::unordered_map<std::string, TokenType> keywords = {
    {"from", TokenType::FROM},
    {"filter", TokenType::FILTER},
    {"select", TokenType::SELECT},
    {"write", TokenType::WRITE},
    {"not", TokenType::NOT},
};

// ============================================================================
// Lexer Implementation
// ============================================================================

Lexer::Lexer(std::string source) : source_(std::move(source)) {}

std::vector<Token> Lexer::tokenize() {
    std::vector<Token> tokens;

    while (!is_at_end()) {
        start_ = current_;
        Token token = scan_token();
        if (token.type != TokenType::ERROR) {
            tokens.push_back(token);
        }
    }

    // Add EOF only if not already added
    if (tokens.empty() || tokens.back().type != TokenType::END_OF_FILE) {
        tokens.push_back(Token{TokenType::END_OF_FILE, "", line_, column_});
    }
    return tokens;
}

Token Lexer::scan_token() {
    skip_whitespace();

    start_ = current_;

    if (is_at_end()) {
        return make_token(TokenType::END_OF_FILE);
    }

    char c = advance();

    if (std::isalpha(c) || c == '_') {
        return scan_identifier();
    }

    if (std::isdigit(c)) {
        return scan_number();
    }

    if (c == '"') {
        return scan_string();
    }

    switch (c) {
        case '+': return make_token(TokenType::PLUS);
        case '-': return make_token(TokenType::MINUS);
        case '*': return make_token(TokenType::STAR);
        case '/': return make_token(TokenType::SLASH);
        case ',': return make_token(TokenType::COMMA);
        case '(': return make_token(TokenType::LPAREN);
        case ')': return make_token(TokenType::RPAREN);
        case '<':
            if (match('=')) return make_token(TokenType::LESS_EQUAL);
            return make_token(TokenType::LESS);
        case '>':
            if (match('=')) return make_token(TokenType::GREATER_EQUAL);
            return make_token(TokenType::GREATER);
        case '=':
            if (match('=')) return make_token(TokenType::EQUAL_EQUAL);
            return make_error("Unexpected character '='");
        case '!':
            if (match('=')) return make_token(TokenType::BANG_EQUAL);
            return make_error("Unexpected character '!'");
    }

    return make_error("Unexpected character");
}

bool Lexer::is_at_end() const {
    return current_ >= source_.size();
}

char Lexer::advance() {
    column_++;
    return source_[current_++];
}

char Lexer::peek() const {
    if (is_at_end()) return '\0';
    return source_[current_];
}

char Lexer::peek_next() const {
    if (current_ + 1 >= source_.size()) return '\0';
    return source_[current_ + 1];
}

bool Lexer::match(char expected) {
    if (is_at_end() || source_[current_] != expected) {
        return false;
    }
    current_++;
    column_++;
    return true;
}

Token Lexer::make_token(TokenType type) {
    std::string lexeme = source_.substr(start_, current_ - start_);
    return Token{type, lexeme, line_, column_ - static_cast<int>(lexeme.size())};
}

Token Lexer::make_error(const std::string& message) {
    return Token{TokenType::ERROR, message, line_, column_};
}

Token Lexer::scan_string() {
    while (peek() != '"' && !is_at_end()) {
        if (peek() == '\n') {
            line_++;
            column_ = 0;
        }
        advance();
    }

    if (is_at_end()) {
        return make_error("Unterminated string");
    }

    advance(); // Closing quote

    std::string value = source_.substr(start_ + 1, current_ - start_ - 2);
    Token token = make_token(TokenType::STRING);
    token.lexeme = value;
    return token;
}

Token Lexer::scan_number() {
    while (std::isdigit(peek())) {
        advance();
    }

    bool is_double = false;
    if (peek() == '.' && std::isdigit(peek_next())) {
        is_double = true;
        advance();
        while (std::isdigit(peek())) {
            advance();
        }
    }

    Token token = make_token(TokenType::NUMBER);
    token.is_double = is_double;

    if (is_double) {
        token.double_value = std::stod(token.lexeme);
    } else {
        token.int_value = std::stoll(token.lexeme);
    }

    return token;
}

Token Lexer::scan_identifier() {
    while (std::isalnum(peek()) || peek() == '_') {
        advance();
    }

    TokenType type = identifier_type();
    return make_token(type);
}

TokenType Lexer::identifier_type() const {
    std::string text = source_.substr(start_, current_ - start_);
    auto it = keywords.find(text);
    if (it != keywords.end()) {
        return it->second;
    }
    return TokenType::IDENT;
}

void Lexer::skip_whitespace() {
    while (!is_at_end()) {
        char c = peek();
        switch (c) {
            case ' ':
            case '\r':
            case '\t':
                advance();
                break;
            case '\n':
                line_++;
                column_ = 0;
                advance();
                break;
            case '#':
                while (peek() != '\n' && !is_at_end()) {
                    advance();
                }
                break;
            default:
                return;
        }
    }
}

const char* token_type_to_string(TokenType type) {
    switch (type) {
        case TokenType::FROM: return "FROM";
        case TokenType::FILTER: return "FILTER";
        case TokenType::SELECT: return "SELECT";
        case TokenType::WRITE: return "WRITE";
        case TokenType::NOT: return "NOT";
        case TokenType::IDENT: return "IDENT";
        case TokenType::NUMBER: return "NUMBER";
        case TokenType::STRING: return "STRING";
        case TokenType::PLUS: return "PLUS";
        case TokenType::MINUS: return "MINUS";
        case TokenType::STAR: return "STAR";
        case TokenType::SLASH: return "SLASH";
        case TokenType::EQUAL_EQUAL: return "EQUAL_EQUAL";
        case TokenType::BANG_EQUAL: return "BANG_EQUAL";
        case TokenType::LESS: return "LESS";
        case TokenType::GREATER: return "GREATER";
        case TokenType::LESS_EQUAL: return "LESS_EQUAL";
        case TokenType::GREATER_EQUAL: return "GREATER_EQUAL";
        case TokenType::COMMA: return "COMMA";
        case TokenType::LPAREN: return "LPAREN";
        case TokenType::RPAREN: return "RPAREN";
        case TokenType::END_OF_FILE: return "EOF";
        case TokenType::ERROR: return "ERROR";
        default: return "UNKNOWN";
    }
}

} // namespace joy
