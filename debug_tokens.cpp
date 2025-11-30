#include "lexer.hpp"
#include <iostream>
#include <fstream>
#include <sstream>

using namespace joy;

std::string read_file(const std::string& filepath) {
    std::ifstream file(filepath);
    std::stringstream buffer;
    buffer << file.rdbuf();
    return buffer.str();
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <file.jy>\n";
        return 1;
    }

    std::string source = read_file(argv[1]);
    Lexer lexer(source);
    auto tokens = lexer.tokenize();

    for (const auto& tok : tokens) {
        std::cout << "Line " << tok.line << ", Col " << tok.column
                  << ": " << token_type_to_string(tok.type)
                  << " [" << tok.lexeme << "]\n";
    }

    return 0;
}
