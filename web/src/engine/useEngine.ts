import { useSyncExternalStore } from "react";

import { createBridge, type BridgeStatus, type EngineBridge } from "./bridge";

/**
 * One bridge per page, created on first use.
 *
 * A module-level instance rather than one owned by a component, because the
 * thing behind it is a Web Worker that loads a 9 MB interpreter: a component
 * that created one in an effect would boot a second interpreter whenever React
 * mounted it twice (which StrictMode does in development, on purpose), and a
 * component that tore one down on unmount would throw away a database the
 * visitor had been typing into.
 *
 * Never disposed. The worker lives as long as the page, which is the lifetime
 * the demo wants; `dispose` exists on the bridge for tests and for a caller
 * that embeds it somewhere shorter-lived.
 */
let shared: EngineBridge | null = null;

export function engineBridge(): EngineBridge {
  shared ??= createBridge();
  return shared;
}

export function useEngineStatus(): BridgeStatus {
  const bridge = engineBridge();
  return useSyncExternalStore(
    (onChange) => bridge.subscribe(() => onChange()),
    () => bridge.status,
  );
}
