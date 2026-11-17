import { Hero } from "./components/Hero";
import { SiteFooter } from "./components/SiteFooter";
import { SiteNav } from "./components/SiteNav";

/**
 * The page, in the order section 2 argues it.
 *
 * Only the hero and the footer exist in this story. The sections between them
 * (how it works, durability, crash proof, numbers, start) each prove one claim
 * and arrive with the story that can prove it, because section 2's rule is that
 * a section without a claim does not go on the page.
 */
export function App() {
  return (
    <>
      <SiteNav />
      <main>
        <Hero />
      </main>
      <SiteFooter />
    </>
  );
}
