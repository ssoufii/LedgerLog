/** Where the page points when it points off itself. */
export const REPO_URL = "https://github.com/ssoufii/LedgerLog";

/** A file in the repository, on the default branch. */
export function repoFile(path: string): string {
  return `${REPO_URL}/blob/main/${path}`;
}
