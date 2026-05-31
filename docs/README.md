# Databricks-Native MDM — Architecture Blueprint (website)

This folder is a **self-contained static website** (plain HTML/CSS, no build step)
that publishes the deep architecture blueprint for a *truly Databricks-native*
Master Data Management product.

## Viewing locally

```bash
cd docs
python3 -m http.server 8000
# open http://localhost:8000
```

## Hosting on GitHub Pages

GitHub Pages is configured to serve from the **`main` branch `/docs` folder**.
Once this branch is merged to `main`, the site goes live at:

```
https://aviral-bhardwaj.github.io/databricksmdm/
```

`.nojekyll` is present so the files are served verbatim (no Jekyll processing).

## Structure (navigation follows deliverable order A–H)

| File | Section |
|------|---------|
| `index.html`     | A · Executive summary |
| `market.html`    | B · Market landscape + comparison matrix |
| `native.html`    | C · What "Databricks-native" really requires (+ anti-patterns) |
| `solid.html`     | D · SOLID-based product architecture + contracts |
| `domain.html`    | E · Domain model & bounded contexts |
| `blueprint.html` | F · Reference implementation blueprint (functional + non-functional) |
| `risks.html`     | G · Risks, trade-offs & differentiators |
| `roadmap.html`   | H · Phased roadmap (MVP → v1 → enterprise) |
| `assets/style.css`, `assets/nav.js` | Shared styling + minimal nav JS |
