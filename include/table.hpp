#pragma once

#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace joy {

// ============================================================================
// Column-major Table Representation with NULL support
// ============================================================================

enum class ColumnType { INT64, DOUBLE, STRING, BOOL };

struct Column {
    std::string name;
    ColumnType type;

    // Type-erased storage using std::optional for NULL support
    // std::nullopt represents NULL values (SQL-style NULL semantics)
    std::variant<std::vector<std::optional<int64_t>>, std::vector<std::optional<double>>,
                 std::vector<std::optional<std::string>>, std::vector<std::optional<bool>>>
        data;

    size_t size() const;
    void reserve(size_t n);

    // Check if value at index is NULL
    bool is_null(size_t idx) const;

    // Get typed value at index (throws if NULL or wrong type)
    int64_t get_int(size_t idx) const;
    double get_double(size_t idx) const;
    const std::string& get_string(size_t idx) const;
    bool get_bool(size_t idx) const;

    // Append typed value
    void append_int(std::optional<int64_t> val);
    void append_double(std::optional<double> val);
    void append_string(std::optional<std::string> val);
    void append_bool(std::optional<bool> val);
};

struct Table {
    std::vector<Column> columns;
    size_t num_rows = 0;

    // Get column by name (returns nullptr if not found)
    Column* get_column(const std::string& name);
    const Column* get_column(const std::string& name) const;

    // Get column index by name (returns -1 if not found)
    int get_column_index(const std::string& name) const;

    // Add column
    void add_column(Column col);

    // Create new table with subset of columns
    Table project(const std::vector<std::string>& cols) const;
};

// ============================================================================
// CSV I/O
// ============================================================================

// Read CSV file into table (infers types from data)
Table read_csv(const std::string& filepath);

// Write table to CSV file
void write_csv(const std::string& filepath, const Table& table);

}  // namespace joy
