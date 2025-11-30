#pragma once

#include <memory>
#include <string>
#include <variant>
#include <vector>

namespace joy {

// ============================================================================
// Column-major Table Representation
// ============================================================================

enum class ColumnType {
    INT64,
    DOUBLE,
    STRING,
    BOOL
};

struct Column {
    std::string name;
    ColumnType type;

    // Type-erased storage (one variant active based on type)
    std::variant<
        std::vector<int64_t>,
        std::vector<double>,
        std::vector<std::string>,
        std::vector<bool>
    > data;

    size_t size() const;
    void reserve(size_t n);

    // Get typed value at index
    int64_t get_int(size_t idx) const;
    double get_double(size_t idx) const;
    const std::string& get_string(size_t idx) const;
    bool get_bool(size_t idx) const;

    // Append typed value
    void append_int(int64_t val);
    void append_double(double val);
    void append_string(std::string val);
    void append_bool(bool val);
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

} // namespace joy
