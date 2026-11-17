import { REPO_URL, repoFile } from "../links";
import { Wordmark } from "./Wordmark";

/**
 * Section 6's footer: the mark built large, then the links and the credit.
 */
const FOOTER_LINKS: { label: string; href: string }[] = [
  { label: "github", href: REPO_URL },
  { label: "architecture", href: repoFile("docs/ARCHITECTURE.md") },
  { label: "roadmap", href: repoFile("docs/ROADMAP.md") },
  { label: "licence", href: repoFile("LICENSE") },
];

export function SiteFooter() {
  return (
    <footer className="footer">
      <Wordmark />
      <div className="footer__meta">
        <p className="footer__copy">2026 Salim Y. Soufi, MIT licensed.</p>
        <ul className="footer__links">
          {FOOTER_LINKS.map((link) => (
            <li key={link.href}>
              <a className="nav-link" href={link.href}>
                {link.label}
              </a>
            </li>
          ))}
        </ul>
      </div>
    </footer>
  );
}
