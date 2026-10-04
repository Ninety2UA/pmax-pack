# README hero source

A 10 second seamless loop of one night. The Cloud Run Job sits above the pipeline and starts each step
with a pulse: Performance Max rows stream out of Google Ads and stack into a new typed-history layer in
BigQuery, history feeds the validated marts, packets pass through the validation gate (scan, then pass),
one transaction swaps all eight reporting tables to the new generation at once, Looker Studio redraws,
and the job writes the run report. As the night completes the reporting window moves forward one day and
the camera pulls back, which is exactly where the loop starts again.

- `index.html`: the HyperFrames composition (1600x640, 30 fps, one paused GSAP timeline). Variables:
  `theme` (`light` or `dark`) and `still` (true holds the wide camera, used for the reduced-motion PNG).
- `assets/`: five objects generated with GPT Image through the Codex CLI on transparent backgrounds
  (Google Ads panel, job, history slab, marts slab, blank monitor), trimmed to their content.
- `render.sh`: lints, renders four PNG sequences (light, dark, and the two still-camera variants), then
  runs `encode.py`.
- `encode.py`: writes `readme-hero.webp`, `readme-hero-dark.webp` and the two static PNGs (frame 259 of
  the still-camera renders) into `docs/diagrams/`.

## Re-render

Needs Node (for `npx hyperframes@0.8.125`) and Python 3 with Pillow built with WebP support.

```bash
bash docs/diagrams/hero-src/render.sh
```

Frames go to a temporary directory (set `WORK=<dir>` to keep them elsewhere). The first render fetches the
Inter font from Google Fonts and caches it. On macOS, `export PRODUCER_BROWSER_GPU_MODE=hardware` speeds up
capture.
