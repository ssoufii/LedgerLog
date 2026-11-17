/**
 * The snippet the start dialog writes, from the one template both sides share.
 *
 * `snippet.json` holds the template; this renders it, and `tests/test_snippet.py`
 * renders the same template and executes the result for every fsync choice. One
 * template is the point: a snippet the page composed by itself would be code
 * nobody ever ran, and "getting started takes one copy" is only true if the
 * thing copied works.
 */

import data from "./snippet.json";

export interface SnippetTemplate {
  default_directory: string;
  policies: string[];
  template: string;
}

export const SNIPPET: SnippetTemplate = data;

/**
 * Escape a value for a double quoted Python string literal.
 *
 * The directory is a field a visitor types into, so it can hold a backslash or
 * a quote, and a snippet that pasted either straight through would be code
 * that does not parse. Backslashes first, or the escapes added after would
 * themselves be escaped.
 */
function pythonString(value: string): string {
  return value.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

export function renderSnippet(directory: string, policy: string): string {
  return SNIPPET.template
    .replace("__DIRECTORY__", pythonString(directory))
    .replace("__POLICY__", pythonString(policy));
}
