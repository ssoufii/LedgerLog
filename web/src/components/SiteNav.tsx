import { REPO_URL } from "../links";

/**
 * Section 5's navigation: a plain row 28px from the top, wordmark left and four
 * text links right. No bar, no blur, no pill, and it does not stick.
 *
 * Three of the four links point at sections that later Milestone 11 stories
 * add (how it works, durability, crash proof). They are in place now because
 * the alternative is rebuilding the navigation once per story, and a fragment
 * link with no target simply does nothing until the target lands.
 */
const NAV_LINKS: { label: string; href: string }[] = [
  { label: "how it works", href: "#how-it-works" },
  { label: "durability", href: "#durability" },
  { label: "crash proof", href: "#crash-proof" },
  { label: "github", href: REPO_URL },
];

export function SiteNav() {
  return (
    <header className="site-nav">
      <span className="site-nav__wordmark">ledgerlog</span>
      <nav aria-label="sections">
        <ul className="site-nav__links">
          {NAV_LINKS.map((link) => (
            <li key={link.href}>
              <a className="nav-link" href={link.href}>
                {link.label}
              </a>
            </li>
          ))}
        </ul>
      </nav>
    </header>
  );
}
