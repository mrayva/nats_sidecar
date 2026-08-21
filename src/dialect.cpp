#include "dialect.hpp"
#include <array>
#include <cctype>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

namespace sidecar {

namespace {

bool is_ident_start(char c) {
    return std::isalpha(static_cast<unsigned char>(c)) || c == '_';
}

bool is_ident_char(char c) {
    return std::isalnum(static_cast<unsigned char>(c)) || c == '_' || c == '-';
}

std::string trim(std::string_view s) {
    std::size_t begin = 0;
    while (begin < s.size() && std::isspace(static_cast<unsigned char>(s[begin]))) ++begin;
    std::size_t end = s.size();
    while (end > begin && std::isspace(static_cast<unsigned char>(s[end - 1]))) --end;
    return std::string(s.substr(begin, end - begin));
}

// Parses "( e1 , e2 , ... , en )" starting at expr[open_paren_pos] == '(',
// returning each element's trimmed raw text (quotes kept verbatim - these
// are re-emitted as-is, never reinterpreted) plus the offset just past the
// closing ')'. Returns nullopt on anything malformed (empty list, empty
// element, unterminated) - the caller leaves such input untouched so the
// real engine parser reports its own syntax error.
std::optional<std::pair<std::vector<std::string>, std::size_t>>
parse_paren_list(std::string_view expr, std::size_t open_paren_pos) {
    std::vector<std::string> elems;
    std::size_t i = open_paren_pos + 1;
    std::size_t elem_start = i;

    while (i < expr.size()) {
        char c = expr[i];
        if (c == '"' || c == '\'') {
            char quote = c;
            ++i;
            while (i < expr.size()) {
                char qc = expr[i];
                ++i;
                if (qc == '\\' && i < expr.size()) { ++i; continue; }
                if (qc == quote) break;
            }
            continue;
        }
        if (c == ',') {
            std::string trimmed = trim(expr.substr(elem_start, i - elem_start));
            if (trimmed.empty()) return std::nullopt;
            elems.push_back(std::move(trimmed));
            ++i;
            elem_start = i;
            continue;
        }
        if (c == ')') {
            std::string trimmed = trim(expr.substr(elem_start, i - elem_start));
            if (trimmed.empty()) return std::nullopt;
            elems.push_back(std::move(trimmed));
            return std::make_pair(std::move(elems), i + 1);
        }
        ++i;
    }
    return std::nullopt;
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

std::string translate_to_atree_dialect(std::string_view expr) {
    struct IdentTok {
        std::string text;
        std::size_t start = std::string::npos;
    };

    std::string out;
    out.reserve(expr.size());
    std::size_t copied_up_to = 0; // [0, copied_up_to) of expr already in `out`

    // Sliding window of the last two identifier-shaped tokens seen outside
    // quotes. `prev2` is the "<ident>" candidate for "<ident> all of (...)"
    // once the current token is "of" and the window reads (prev2="X",
    // prev1="all").
    IdentTok prev2, prev1;

    std::size_t i = 0;
    while (i < expr.size()) {
        char c = expr[i];

        if (c == '"' || c == '\'') {
            char quote = c;
            ++i;
            while (i < expr.size()) {
                char qc = expr[i];
                ++i;
                if (qc == '\\' && i < expr.size()) { ++i; continue; }
                if (qc == quote) break;
            }
            prev2 = IdentTok{};
            prev1 = IdentTok{};
            continue;
        }

        if (is_ident_start(c)) {
            std::size_t start = i;
            ++i;
            while (i < expr.size() && is_ident_char(expr[i])) ++i;
            std::string tok(expr.substr(start, i - start));

            if (tok == "of" && prev1.text == "all" && prev2.start != std::string::npos) {
                std::size_t j = i;
                while (j < expr.size() && std::isspace(static_cast<unsigned char>(expr[j]))) ++j;
                if (j < expr.size() && expr[j] == '(') {
                    auto parsed = parse_paren_list(expr, j);
                    if (parsed) {
                        auto& [elems, after] = *parsed;
                        out.append(expr.substr(copied_up_to, prev2.start - copied_up_to));
                        out += "(";
                        for (std::size_t k = 0; k < elems.size(); ++k) {
                            if (k) out += " and ";
                            out += prev2.text;
                            out += " one of (";
                            out += elems[k];
                            out += ")";
                        }
                        out += ")";
                        copied_up_to = after;
                        i = after;
                        prev2 = IdentTok{};
                        prev1 = IdentTok{};
                        continue;
                    }
                }
            }

            prev2 = prev1;
            prev1 = IdentTok{std::move(tok), start};
            continue;
        }

        // Anything else (whitespace, operators, punctuation, digits) is not
        // itself an identifier token, but must NOT reset the window: valid
        // "<ident> all of (...)" syntax always has whitespace (and nothing
        // else) between its three tokens, so the window has to survive it.
        // Only quoted content (handled above) resets it - see that branch's
        // comment for why even that reset isn't load-bearing for valid
        // input, just defensive.
        ++i;
    }

    out.append(expr.substr(copied_up_to));
    return out;
}

} // namespace sidecar
