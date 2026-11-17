import { CONSOLE_KEY_INPUT_ID, Console } from "./Console";

/**
 * Section 6's hero: the claim, one sentence, one button, and the shaped stage
 * with the live console on it.
 *
 * The button does what section 6 says it does, which is focus the console's key
 * input rather than scroll or open anything. It is the one primary button in
 * the hero, and it is the only control on the page that moves focus, so it
 * moves it to an element that announces itself (a labelled text field) rather
 * than to a container.
 */
export function Hero() {
  const focusConsole = () => {
    document.getElementById(CONSOLE_KEY_INPUT_ID)?.focus();
  };

  return (
    <section className="hero" aria-labelledby="hero-title">
      <div className="hero__copy">
        <h1 className="hero__title" id="hero-title">
          <span className="hero__line">a storage engine</span>
          <span className="hero__line">you can drive.</span>
        </h1>
        <p className="hero__lead">
          LedgerLog is a log-structured key-value store written from scratch in Python. This is
          the real engine, running in your browser.
        </p>
        <div className="hero__action">
          <button type="button" onClick={focusConsole}>
            Write a key
          </button>
        </div>
      </div>
      <div className="stage">
        <Console />
      </div>
    </section>
  );
}
