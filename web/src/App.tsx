import { useEffect } from "react";

import { DEMO_KEY, refresh } from "./engine/state";
import { useEngineStatus } from "./engine/useEngine";
import { CrashTiles } from "./components/CrashTiles";
import { DurabilityDial } from "./components/DurabilityDial";
import { Hero } from "./components/Hero";
import { NumbersSlider } from "./components/NumbersSlider";
import { StartDialog } from "./components/StartDialog";
import { SiteFooter } from "./components/SiteFooter";
import { SiteNav } from "./components/SiteNav";
import { WriteStrip } from "./components/WriteStrip";

/**
 * The page, in the order section 2 argues it.
 *
 * The sections still missing (durability, crash proof, numbers, start) each
 * prove one claim and arrive with the story that can prove it, because
 * section 2's rule is that a section without a claim does not go on the page.
 *
 * The one effect here is the first read of the engine. The engine is the
 * external system the page renders, and until it reports itself ready there
 * is nothing to read; once it is, the page takes one snapshot so that what is
 * on screen is the engine's own state rather than an empty placeholder. Every
 * later read is driven by a control someone pressed.
 */
export function App() {
  const status = useEngineStatus();

  useEffect(() => {
    if (status.phase !== "ready") {
      return;
    }
    void refresh(DEMO_KEY);
  }, [status.phase]);

  return (
    <>
      <SiteNav />
      <main>
        <Hero />
        <WriteStrip />
        <DurabilityDial />
        <CrashTiles />
        <NumbersSlider />
        <StartDialog />
      </main>
      <SiteFooter />
    </>
  );
}
