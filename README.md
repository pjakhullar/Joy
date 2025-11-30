# Joy Language

A minimal compiled domain-specific language for fast tabular data transformations.

## Features

- **Compiled pipeline execution**: Lexer → Parser → IR → Bytecode VM
- **Columnar data representation**: Efficient memory layout for future vectorization
- **CSV I/O**: Read and write CSV files with automatic type inference
- **Filter operations**: Row filtering with boolean predicates
- **Select operations**: Column projection
- **Expression evaluation**: Arithmetic, comparison, and logical operators

## Building

```bash
make
```

## Usage

```bash
./joy <program.jy>
```

## Example

**employees.csv:**
```csv
name,age,department,salary
Alice,35,Engineering,95000
Bob,28,Sales,72000
Charlie,42,Engineering,110000
```

**process.jy:**
```
from "employees.csv"
filter age > 30
filter department == "Engineering"
select name, salary
write "output.csv"
```

**Run:**
```bash
./joy process.jy
```

**Output (output.csv):**
```csv
name,salary
Alice,95000
Charlie,110000
```

## Language Grammar

```
program    := pipeline EOF
pipeline   := from_stmt operation*
operation  := filter_stmt | select_stmt | write_stmt

from_stmt   := FROM string
filter_stmt := FILTER expr
select_stmt := SELECT column_list
write_stmt  := WRITE string

expr := equality
     | comparison  (>, <, >=, <=)
     | term        (+, -)
     | factor      (*, /)
     | unary       (-, not)
     | primary     (number, string, column, parentheses)
```

## Types

- `int64` - 64-bit integers
- `double` - floating point numbers
- `string` - UTF-8 strings
- `bool` - boolean values

## Architecture

- **lexer.cpp** - Tokenization
- **parser.cpp** - Recursive descent parser
- **ast.hpp** - Abstract syntax tree definitions
- **compiler.cpp** - AST → IR compilation
- **ir.hpp** - Intermediate representation (bytecode)
- **vm.cpp** - Stack-based virtual machine
- **table.cpp** - Columnar table operations and CSV I/O

## Bytecode Instructions (14 total)

Stack operations: PUSH_INT, PUSH_DOUBLE, PUSH_STRING, PUSH_BOOL, LOAD_COLUMN
Arithmetic: ADD, SUB, MUL, DIV, NEG
Comparison: EQ, NEQ, LT, GT, LTE, GTE
Logical: NOT

## Status

MVP complete! All core components implemented and tested.

## Next Steps

See the design document for post-MVP roadmap including:
- Type checking pass
- Optimization passes (constant folding, predicate pushdown)
- Vectorized execution
- GROUP BY and aggregations
- JOIN operations
- Apache Arrow integration
