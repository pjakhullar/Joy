#include "table.hpp"
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <algorithm>

namespace joy {

// ============================================================================
// Column Implementation with NULL support
// ============================================================================
// Column stores data in a columnar (column-major) layout for cache efficiency
// Uses std::variant with std::optional vectors for NULL support (SQL-style)
// std::nullopt represents NULL values (empty CSV cells, etc.)
// This design enables future vectorization (SIMD) optimizations

// Get number of values in this column
// Uses std::visit to polymorphically call size() on whichever vector type is active
size_t Column::size() const {
    return std::visit([](const auto& vec) { return vec.size(); }, data);
}

// Pre-allocate space for n values (optimization for bulk loading)
// Reduces allocations when we know the final size in advance
void Column::reserve(size_t n) {
    std::visit([n](auto& vec) { vec.reserve(n); }, data);
}

// Check if value at index is NULL
// Returns true if the value is std::nullopt (SQL NULL)
bool Column::is_null(size_t idx) const {
    return std::visit([idx](const auto& vec) {
        return !vec[idx].has_value();  // true if nullopt
    }, data);
}

// Type-specific getters: Extract value at index
// These throw std::bad_variant_access if called on wrong type
// These throw std::bad_optional_access if value is NULL
// Example: calling get_int() on a string column or NULL value will throw

int64_t Column::get_int(size_t idx) const {
    return std::get<std::vector<std::optional<int64_t>>>(data)[idx].value();
}

double Column::get_double(size_t idx) const {
    return std::get<std::vector<std::optional<double>>>(data)[idx].value();
}

const std::string& Column::get_string(size_t idx) const {
    return std::get<std::vector<std::optional<std::string>>>(data)[idx].value();
}

bool Column::get_bool(size_t idx) const {
    return std::get<std::vector<std::optional<bool>>>(data)[idx].value();
}

// Type-specific appenders: Add value to end of column
// Must match the column's type or std::bad_variant_access will be thrown
// Accepts std::optional - std::nullopt represents NULL

void Column::append_int(std::optional<int64_t> val) {
    std::get<std::vector<std::optional<int64_t>>>(data).push_back(val);
}

void Column::append_double(std::optional<double> val) {
    std::get<std::vector<std::optional<double>>>(data).push_back(val);
}

void Column::append_string(std::optional<std::string> val) {
    // Use move semantics to avoid copying the string
    std::get<std::vector<std::optional<std::string>>>(data).push_back(std::move(val));
}

void Column::append_bool(std::optional<bool> val) {
    std::get<std::vector<std::optional<bool>>>(data).push_back(val);
}

// ============================================================================
// Table Implementation
// ============================================================================
// Table is a collection of columns with shared row count
// This is a columnar data structure (like Apache Arrow, pandas internals)
// Benefits: Better cache locality, enables vectorization, natural for analytics

// Find column by name (mutable version)
// Returns pointer to allow nullptr check for "not found"
// Linear search is fine for MVP (could use hash map for 100s of columns)
Column* Table::get_column(const std::string& name) {
    for (auto& col : columns) {
        if (col.name == name) {
            return &col;
        }
    }
    return nullptr;  // Column not found
}

// Find column by name (const version)
// Needed to support const Table references
const Column* Table::get_column(const std::string& name) const {
    for (const auto& col : columns) {
        if (col.name == name) {
            return &col;
        }
    }
    return nullptr;
}

// Get column index by name
// Returns -1 if not found (could use optional<size_t> instead)
// Used by VM to convert column references to indices
int Table::get_column_index(const std::string& name) const {
    for (size_t i = 0; i < columns.size(); ++i) {
        if (columns[i].name == name) {
            return static_cast<int>(i);
        }
    }
    return -1;  // Not found
}

// Add a new column to the table
// Takes ownership via move semantics
void Table::add_column(Column col) {
    columns.push_back(std::move(col));
}

// Create new table with subset of columns (SELECT operation)
// Example: table.project({"name", "age"}) returns table with only those columns
// Copies column data (could be optimized with shared pointers)
Table Table::project(const std::vector<std::string>& cols) const {
    Table result;
    result.num_rows = num_rows;  // Same number of rows

    // Copy each requested column
    for (const auto& col_name : cols) {
        const Column* col = get_column(col_name);
        if (!col) {
            throw std::runtime_error("Column not found: " + col_name);
        }
        result.columns.push_back(*col);  // Copy column data
    }

    return result;
}

// ============================================================================
// CSV I/O Implementation
// ============================================================================
// Simple CSV parser with automatic type inference
// Limitations: No quoted fields with commas, no escape sequences
// Good enough for MVP, could use a proper CSV library (like csv-parser) later

// Helper: Split string by delimiter (handles trailing empty fields)
// Example: split("a,b,c", ',') -> ["a", "b", "c"]
// Example: split("a,b,", ',') -> ["a", "b", ""]  (important for NULL handling)
static std::vector<std::string> split(const std::string& s, char delim) {
    std::vector<std::string> result;
    size_t start = 0;
    size_t end = s.find(delim);

    while (end != std::string::npos) {
        result.push_back(s.substr(start, end - start));
        start = end + 1;
        end = s.find(delim, start);
    }

    // Add the last field (or empty string if line ends with delimiter)
    result.push_back(s.substr(start));

    return result;
}

// Helper: Remove leading and trailing whitespace
// Example: trim("  hello  ") -> "hello"
static std::string trim(const std::string& s) {
    auto start = s.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return "";  // All whitespace
    auto end = s.find_last_not_of(" \t\r\n");
    return s.substr(start, end - start + 1);
}

// Helper: Infer column type from first NON-NULL data value
// Strategy: Try int64 -> double -> string (most specific to least)
// This is heuristic-based and can be wrong (e.g., "123" might be a product code)
// Empty values (NULL) don't influence type inference
// Future: Could sample multiple rows or use schema file
static ColumnType infer_type(const std::vector<std::string>& values) {
    // Find first non-empty value for type inference
    for (const auto& val : values) {
        std::string v = trim(val);
        if (v.empty()) continue;  // Skip NULL values

        // Try to parse as int64
        try {
            size_t pos;
            std::stoll(v, &pos);
            if (pos == v.size()) {  // Entire string was parsed
                return ColumnType::INT64;
            }
        } catch (...) {}  // Not an integer

        // Try to parse as double
        try {
            size_t pos;
            std::stod(v, &pos);
            if (pos == v.size()) {  // Entire string was parsed
                return ColumnType::DOUBLE;
            }
        } catch (...) {}  // Not a double

        // If first non-empty value isn't numeric, it's string
        return ColumnType::STRING;
    }

    // All values are empty (NULL) - default to string
    return ColumnType::STRING;
}

// Helper: Parse string value and append to column (SQL NULL support)
// Handles type coercion (string -> typed value)
// Empty cells become NULL (std::nullopt) - SQL semantics
// Throws if value cannot be parsed according to column type
static void append_value(Column& col, const std::string& value) {
    std::string v = trim(value);

    // SQL NULL semantics: empty cells are NULL
    if (v.empty()) {
        switch (col.type) {
            case ColumnType::INT64:
                col.append_int(std::nullopt);
                break;
            case ColumnType::DOUBLE:
                col.append_double(std::nullopt);
                break;
            case ColumnType::STRING:
                col.append_string(std::nullopt);
                break;
            case ColumnType::BOOL:
                col.append_bool(std::nullopt);
                break;
        }
        return;
    }

    // Non-empty values: parse according to column type
    try {
        switch (col.type) {
            case ColumnType::INT64:
                col.append_int(std::stoll(v));
                break;
            case ColumnType::DOUBLE:
                col.append_double(std::stod(v));
                break;
            case ColumnType::STRING:
                col.append_string(v);
                break;
            case ColumnType::BOOL:
                // "true" or "1" -> true, anything else -> false
                col.append_bool(v == "true" || v == "1");
                break;
        }
    } catch (const std::exception& e) {
        // Type coercion failed (e.g., "abc" in INT64 column)
        throw std::runtime_error("Failed to parse value '" + value + "' for column " + col.name);
    }
}

// Read CSV file into a Table
// Process:
//   1. Read header row -> column names
//   2. Read first data row -> infer types
//   3. Create columns with appropriate types
//   4. Parse all rows and append to columns
Table read_csv(const std::string& filepath) {
    std::ifstream file(filepath);
    if (!file) {
        throw std::runtime_error("Cannot open file: " + filepath);
    }

    Table table;
    std::string line;

    // Read header row (first line contains column names)
    if (!std::getline(file, line)) {
        throw std::runtime_error("Empty CSV file");
    }

    std::vector<std::string> headers = split(line, ',');
    for (auto& header : headers) {
        header = trim(header);  // Clean up column names
    }

    // Collect all data rows first to infer types from non-NULL values
    std::vector<std::vector<std::string>> all_rows;
    while (std::getline(file, line)) {
        if (line.empty()) continue;  // Skip blank lines
        all_rows.push_back(split(line, ','));
    }

    if (all_rows.empty()) {
        // File has header but no data rows
        // Create empty string columns with NULL support
        for (const auto& header : headers) {
            Column col;
            col.name = header;
            col.type = ColumnType::STRING;
            col.data = std::vector<std::optional<std::string>>{};
            table.add_column(std::move(col));
        }
        return table;
    }

    // Verify all rows have same column count
    for (size_t row = 0; row < all_rows.size(); ++row) {
        if (all_rows[row].size() != headers.size()) {
            throw std::runtime_error("Column count mismatch in CSV at row " + std::to_string(row + 2));
        }
    }

    // Infer types from all values in each column (skip NULLs)
    for (size_t col_idx = 0; col_idx < headers.size(); ++col_idx) {
        Column col;
        col.name = headers[col_idx];

        // Collect all values for this column
        std::vector<std::string> column_values;
        for (const auto& row : all_rows) {
            column_values.push_back(row[col_idx]);
        }

        col.type = infer_type(column_values);  // Infer from non-NULL values

        // Initialize variant with the appropriate std::optional vector type
        // This is necessary because std::variant needs an active alternative
        switch (col.type) {
            case ColumnType::INT64:
                col.data = std::vector<std::optional<int64_t>>{};
                break;
            case ColumnType::DOUBLE:
                col.data = std::vector<std::optional<double>>{};
                break;
            case ColumnType::STRING:
                col.data = std::vector<std::optional<std::string>>{};
                break;
            case ColumnType::BOOL:
                col.data = std::vector<std::optional<bool>>{};
                break;
        }

        table.add_column(std::move(col));
    }

    // Parse all rows (already collected for type inference)
    for (const auto& row : all_rows) {
        for (size_t col_idx = 0; col_idx < row.size(); ++col_idx) {
            append_value(table.columns[col_idx], row[col_idx]);
        }
        table.num_rows++;
    }

    return table;
}

// Write Table to CSV file (SQL NULL support)
// Format: header row, then data rows
// NULL values are written as empty cells
// No quoting (assumes values don't contain commas)
void write_csv(const std::string& filepath, const Table& table) {
    std::ofstream file(filepath);
    if (!file) {
        throw std::runtime_error("Cannot create file: " + filepath);
    }

    // Write header row (column names)
    for (size_t i = 0; i < table.columns.size(); ++i) {
        if (i > 0) file << ",";  // Comma separator between columns
        file << table.columns[i].name;
    }
    file << "\n";

    // Write data rows
    // Outer loop: rows (traditional row-wise iteration)
    // Inner loop: columns
    // Even though data is stored column-wise, we write row-wise for CSV format
    for (size_t row = 0; row < table.num_rows; ++row) {
        for (size_t col_idx = 0; col_idx < table.columns.size(); ++col_idx) {
            if (col_idx > 0) file << ",";

            const Column& col = table.columns[col_idx];

            // SQL NULL semantics: NULL values write as empty cells
            if (col.is_null(row)) {
                // Write nothing (empty cell)
                continue;
            }

            // Format value based on column type
            switch (col.type) {
                case ColumnType::INT64:
                    file << col.get_int(row);
                    break;
                case ColumnType::DOUBLE:
                    file << col.get_double(row);
                    break;
                case ColumnType::STRING:
                    file << col.get_string(row);
                    // TODO: Quote strings containing commas
                    break;
                case ColumnType::BOOL:
                    file << (col.get_bool(row) ? "true" : "false");
                    break;
            }
        }
        file << "\n";
    }
}

} // namespace joy
