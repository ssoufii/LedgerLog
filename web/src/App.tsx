import { useEffect } from "react";

import { DEMO_KEY, refresh } from "./engine/state";
import { useEngineStatus } from "./engine/useEngine";
import { CrashTiles } from "./components/CrashTiles";
import { DurabilityDial } from "./components/DurabilityDial";
import { Hero } from "./components/Hero";
import { NumbersSlider } from "./components/NumbersSlider";
import { SiteFooter } from "./components/SiteFooter";
import { SiteNav } from "./components/SiteNav";
import { StartSection } from "./components/StartSection";
import { WriteStrip } from "./components/WriteStrip";

/**
 * The page, in the order section 2 argues it.
 *
 * Every section proves one claim, in the order the table in section 2 gives
 * them: drive it, follow a write, the durability dial, crash proof, the
 * numbers, and the start. A section without a claim does not go on the page.
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
        <StartSection />
      </main>
      <SiteFooter />
    </>
  );
}
