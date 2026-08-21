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

// a-tree's own "all of" list operator means the opposite of what its name
// (and be-tree's "all of") suggests: confirmed against both engines' real
// matcher source, a-tree's checks the *event's* value list is a subset of
// the literal list ("all of the event's values are among these"), while
// be-tree's checks the event's value list is a superset of (contains) the
// literal list ("the event has all of these values") - the reading that
// actually matches the operator's name. be-tree's semantic is treated as
// canonical; this rewrites every "<ident> all of (v1, v2, ..., vn)" found
// outside quoted string literals into an equivalent conjunction of
// singleton "one of" checks - "(<ident> one of (v1) and ... and <ident>
// one of (vn))" - which both engines already agree on (a symmetric
// non-empty-intersection test), sidestepping a-tree's native "all of"
// token entirely rather than trying to reinterpret it.
//
// A malformed "all of" clause (an empty or unterminated list, which
// neither engine's grammar accepts to begin with) is left untouched, so
// the underlying parser reports its own ordinary syntax error instead of
// this rewrite doing something unexpected. Everything else - including
// occurrences of the words "all"/"of" inside quoted strings, or as
// unrelated identifiers not followed by "of ("/"(" - passes through
// byte-for-byte.
std::string translate_to_atree_dialect(std::string_view expr);

} // namespace sidecar
