#include "compiler.hpp"
#include "lexer.hpp"
#include "parser.hpp"
#include "vm.hpp"
#include <fstream>
#include <iostream>
#include <sstream>

using namespace joy;

// Read entire file into string
std::string read_file(const std::string& filepath) {
    std::ifstream file(filepath);
    if (!file) {
        std::cerr << "Error: Could not open file '" << filepath << "'\n";
        std::exit(1);
    }

    std::stringstream buffer;
    buffer << file.rdbuf();
    return buffer.str();
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: joy <source_file.jy>\n";
        std::cerr << "Example: joy process.jy\n";
        return 1;
    }

    std::string source_file = argv[1];

    try {
        // 1. Read source code
        std::string source = read_file(source_file);

        // 2. Lex
        Lexer lexer(source);
        auto tokens = lexer.tokenize();

        // 3. Parse
        Parser parser(std::move(tokens));
        Program program = parser.parse();

        // 4. Compile
        Compiler compiler;
        ExecutionPlan plan = compiler.compile(program);

        // 5. Execute
        VM vm;
        vm.execute(plan);

        std::cout << "Execution completed successfully.\n";
        return 0;

    } catch (const ParseError& e) {
        std::cerr << "Parse error at line " << e.line() << ", column " << e.column()
                  << ": " << e.what() << "\n";
        return 1;
    } catch (const CompileError& e) {
        std::cerr << "Compile error: " << e.what() << "\n";
        return 1;
    } catch (const RuntimeError& e) {
        std::cerr << "Runtime error: " << e.what() << "\n";
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
}
