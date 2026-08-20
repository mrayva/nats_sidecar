#pragma once

#include <string>
#include <string_view>

namespace sidecar {

// a-tree and be-tree agree on keyword spelling for every multi-word
// operator except one. Confirmed against both engines' real lexers
// (a-tree: src/lexer.rs's own test fixtures - "not in", "one of",
// "none of", "all of", "is null", "is not null", "is empty" are all
// space-separated, not underscore-joined; be-tree: src/lexer.l defines
// the identical space-separated spellings). The underscore-joined forms
// seen in a-tree's parser error messages (e.g. "not_in") are just the
// display name of the token *type*, not accepted surface syntax - a-tree
// rejects the underscore form as a plain identifier.
//
// The one real gap: be-tree's grammar has no "is not empty" rule at all
// (a-tree has IsNotEmpty; be-tree's lexer.l defines is null/is not
// null/is empty but not is not empty).
//
// Validates an expression is safe to hand to be-tree's parser as-is.
// Quoted string literals (single or double quoted, backslash-escaped) are
// never inspected - only bare keyword tokens outside quotes are checked.
// Throws std::runtime_error if the expression uses "is not empty", since
// be-tree has no way to express it. Otherwise returns the expression
// unchanged - no translation is actually needed.
std::string translate_to_betree_dialect(std::string_view expr);

} // namespace sidecar
