/**
 * The start dialog's snippet, filled in from the one shared template.
 *
 * Why a template in a JSON file rather than a string in the component: the
 * page promises that the code it hands over runs, and the only way to keep
 * that promise is to run it. `tests/test_snippets.py` reads the same file,
 * fills it in the same way and executes the result against the real engine
 * for every fsync policy, so a renamed parameter fails pytest instead of
 * shipping a snippet that raises on first use.
 *
 * Values go into the template as JSON string literals. JSON's string syntax
 * is a subset of Python's, so `JSON.stringify` here and `json.dumps` in the
 * test produce a literal Python reads back as the same text, quotes and
 * backslashes in a directory name included.
 */
import data from "./snippets.json";

export type SnippetPolicy = "never" | "interval" | "always";

export const SNIPPET_POLICIES = data.policies as SnippetPolicy[];
export const DEFAULT_DIRECTORY = data.default_directory;
export const DEFAULT_POLICY = data.default_policy as SnippetPolicy;

export function renderSnippet(directory: string, policy: SnippetPolicy): string {
  return data.template
    .replace("{{directory}}", JSON.stringify(directory))
    .replace("{{fsync_policy}}", JSON.stringify(policy));
}
