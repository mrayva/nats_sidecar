#include "dialect.hpp"
#include <array>
#include <cctype>
#include <stdexcept>

namespace sidecar {

namespace {

bool is_ident_start(char c) {
    return std::isalpha(static_cast<unsigned char>(c)) || c == '_';
}

bool is_ident_char(char c) {
    return std::isalnum(static_cast<unsigned char>(c)) || c == '_' || c == '-';
}

} // namespace

std::string translate_to_betree_dialect(std::string_view expr) {
    // Rolling window of the last three identifier-shaped tokens seen
    // outside quotes, to catch "is not empty" wherever it appears.
    std::array<std::string, 3> window;

    std::size_t i = 0;
    while (i < expr.size()) {
        char c = expr[i];

        // Quoted string literal - skip over untouched, respecting
        // backslash escapes, so keyword-shaped text inside literals never
        // triggers the check (e.g. symbol = "is not empty handed").
        if (c == '"' || c == '\'') {
            char quote = c;
            ++i;
            while (i < expr.size()) {
                char qc = expr[i];
                ++i;
                if (qc == '\\' && i < expr.size()) {
                    ++i;
                    continue;
                }
                if (qc == quote) break;
            }
            continue;
        }

        if (is_ident_start(c)) {
            std::size_t start = i;
            ++i;
            while (i < expr.size() && is_ident_char(expr[i])) ++i;

            window[0] = window[1];
            window[1] = window[2];
            window[2] = expr.substr(start, i - start);

            if (window[0] == "is" && window[1] == "not" && window[2] == "empty") {
                throw std::runtime_error(
                    "is not empty is not supported when engine=betree");
            }
            continue;
        }

        ++i;
    }

    return std::string(expr);
}

} // namespace sidecar
