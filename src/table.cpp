#include "table.hpp"
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <algorithm>

namespace joy {

// ============================================================================
// Column Implementation
// ============================================================================

size_t Column::size() const {
    return std::visit([](const auto& vec) { return vec.size(); }, data);
}

void Column::reserve(size_t n) {
    std::visit([n](auto& vec) { vec.reserve(n); }, data);
}

int64_t Column::get_int(size_t idx) const {
    return std::get<std::vector<int64_t>>(data)[idx];
}

double Column::get_double(size_t idx) const {
    return std::get<std::vector<double>>(data)[idx];
}

const std::string& Column::get_string(size_t idx) const {
    return std::get<std::vector<std::string>>(data)[idx];
}

bool Column::get_bool(size_t idx) const {
    return std::get<std::vector<bool>>(data)[idx];
}

void Column::append_int(int64_t val) {
    std::get<std::vector<int64_t>>(data).push_back(val);
}

void Column::append_double(double val) {
    std::get<std::vector<double>>(data).push_back(val);
}

void Column::append_string(std::string val) {
    std::get<std::vector<std::string>>(data).push_back(std::move(val));
}

void Column::append_bool(bool val) {
    std::get<std::vector<bool>>(data).push_back(val);
}

// ============================================================================
// Table Implementation
// ============================================================================

Column* Table::get_column(const std::string& name) {
    for (auto& col : columns) {
        if (col.name == name) {
            return &col;
        }
    }
    return nullptr;
}

const Column* Table::get_column(const std::string& name) const {
    for (const auto& col : columns) {
        if (col.name == name) {
            return &col;
        }
    }
    return nullptr;
}

int Table::get_column_index(const std::string& name) const {
    for (size_t i = 0; i < columns.size(); ++i) {
        if (columns[i].name == name) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

void Table::add_column(Column col) {
    columns.push_back(std::move(col));
}

Table Table::project(const std::vector<std::string>& cols) const {
    Table result;
    result.num_rows = num_rows;

    for (const auto& col_name : cols) {
        const Column* col = get_column(col_name);
        if (!col) {
            throw std::runtime_error("Column not found: " + col_name);
        }
        result.columns.push_back(*col);
    }

    return result;
}

// ============================================================================
// CSV I/O Implementation
// ============================================================================

// Helper: Split string by delimiter
static std::vector<std::string> split(const std::string& s, char delim) {
    std::vector<std::string> result;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        result.push_back(item);
    }
    return result;
}

// Helper: Trim whitespace
static std::string trim(const std::string& s) {
    auto start = s.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return "";
    auto end = s.find_last_not_of(" \t\r\n");
    return s.substr(start, end - start + 1);
}

// Helper: Infer column type from string value
static ColumnType infer_type(const std::string& value) {
    std::string v = trim(value);

    if (v.empty()) {
        return ColumnType::STRING;
    }

    // Try to parse as int64
    try {
        size_t pos;
        std::stoll(v, &pos);
        if (pos == v.size()) {
            return ColumnType::INT64;
        }
    } catch (...) {}

    // Try to parse as double
    try {
        size_t pos;
        std::stod(v, &pos);
        if (pos == v.size()) {
            return ColumnType::DOUBLE;
        }
    } catch (...) {}

    // Default to string
    return ColumnType::STRING;
}

// Helper: Parse value into column based on type
static void append_value(Column& col, const std::string& value) {
    std::string v = trim(value);

    try {
        switch (col.type) {
            case ColumnType::INT64:
                col.append_int(v.empty() ? 0 : std::stoll(v));
                break;
            case ColumnType::DOUBLE:
                col.append_double(v.empty() ? 0.0 : std::stod(v));
                break;
            case ColumnType::STRING:
                col.append_string(v);
                break;
            case ColumnType::BOOL:
                col.append_bool(v == "true" || v == "1");
                break;
        }
    } catch (const std::exception& e) {
        // Type coercion failed, convert column to string if needed
        throw std::runtime_error("Failed to parse value '" + value + "' for column " + col.name);
    }
}

Table read_csv(const std::string& filepath) {
    std::ifstream file(filepath);
    if (!file) {
        throw std::runtime_error("Cannot open file: " + filepath);
    }

    Table table;
    std::string line;

    // Read header
    if (!std::getline(file, line)) {
        throw std::runtime_error("Empty CSV file");
    }

    std::vector<std::string> headers = split(line, ',');
    for (auto& header : headers) {
        header = trim(header);
    }

    // Read first data row to infer types
    if (!std::getline(file, line)) {
        // Empty table (header only)
        for (const auto& header : headers) {
            Column col;
            col.name = header;
            col.type = ColumnType::STRING;
            col.data = std::vector<std::string>{};
            table.add_column(std::move(col));
        }
        return table;
    }

    std::vector<std::string> first_row = split(line, ',');
    if (first_row.size() != headers.size()) {
        throw std::runtime_error("Column count mismatch in CSV");
    }

    // Infer types and create columns
    for (size_t i = 0; i < headers.size(); ++i) {
        Column col;
        col.name = headers[i];
        col.type = infer_type(first_row[i]);

        // Initialize variant with appropriate vector type
        switch (col.type) {
            case ColumnType::INT64:
                col.data = std::vector<int64_t>{};
                break;
            case ColumnType::DOUBLE:
                col.data = std::vector<double>{};
                break;
            case ColumnType::STRING:
                col.data = std::vector<std::string>{};
                break;
            case ColumnType::BOOL:
                col.data = std::vector<bool>{};
                break;
        }

        table.add_column(std::move(col));
    }

    // Parse first row
    for (size_t i = 0; i < first_row.size(); ++i) {
        append_value(table.columns[i], first_row[i]);
    }
    table.num_rows = 1;

    // Parse remaining rows
    while (std::getline(file, line)) {
        if (line.empty()) continue;

        std::vector<std::string> values = split(line, ',');
        if (values.size() != headers.size()) {
            throw std::runtime_error("Column count mismatch in CSV at row " + std::to_string(table.num_rows + 1));
        }

        for (size_t i = 0; i < values.size(); ++i) {
            append_value(table.columns[i], values[i]);
        }
        table.num_rows++;
    }

    return table;
}

void write_csv(const std::string& filepath, const Table& table) {
    std::ofstream file(filepath);
    if (!file) {
        throw std::runtime_error("Cannot create file: " + filepath);
    }

    // Write header
    for (size_t i = 0; i < table.columns.size(); ++i) {
        if (i > 0) file << ",";
        file << table.columns[i].name;
    }
    file << "\n";

    // Write data rows
    for (size_t row = 0; row < table.num_rows; ++row) {
        for (size_t col_idx = 0; col_idx < table.columns.size(); ++col_idx) {
            if (col_idx > 0) file << ",";

            const Column& col = table.columns[col_idx];
            switch (col.type) {
                case ColumnType::INT64:
                    file << col.get_int(row);
                    break;
                case ColumnType::DOUBLE:
                    file << col.get_double(row);
                    break;
                case ColumnType::STRING:
                    file << col.get_string(row);
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
