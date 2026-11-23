// The pinned reference's HTML render constants, which Rust's html_preview and html_render use as
// they are: the markup injected into each page's head (theme stylesheet and bootstrap script),
// the theme fragments its headless browser loads pages with, the widths it measures, and the
// height and title limits.
//
//   node scripts/reference_html_render.mjs <reference checkout>  > html_render.json
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

const root = resolve(process.argv[2]);
const load = (path) => import(pathToFileURL(resolve(root, path)).href);
const html = await load("packages/shared/src/htmlRender.ts");
const palettes = await load("packages/shared/src/themePalettes.ts");

// The injected markup, read back from a page that already has both meta tags, so it holds only
// the theme stylesheet and the bootstrap script. Rust adds the meta tags as the reference does.
const probe = '<head><meta charset="utf-8"><meta name="viewport" content="x"></head>';
const injected = html.injectHtmlRenderBootstrap(probe);
const markup = injected.slice("<head>".length, injected.indexOf('<meta charset="utf-8">'));
if (!markup.startsWith('<style id="t3-theme">') || !markup.endsWith("</script>")) {
  throw new Error("injectHtmlRenderBootstrap no longer puts its markup right after <head>");
}
const fragment = (appearance) =>
  html.htmlRenderThemeFragment(
    html.htmlRenderTheme(
      appearance === "light" ? palettes.T3_CODE_LIGHT_THEME_COLORS : palettes.T3_CODE_DARK_THEME_COLORS,
      appearance,
      html.HTML_RENDER_MEASURE_FONTS,
    ),
  );

const constants = {
  bootstrapMarkup: markup,
  charsetMeta: '<meta charset="utf-8">',
  viewportMeta: '<meta name="viewport" content="width=device-width, initial-scale=1">',
  fragments: { dark: fragment("dark"), light: fragment("light") },
  measureWidths: [...html.HTML_RENDER_MEASURE_WIDTHS],
  columnWidth: html.HTML_RENDER_COLUMN_WIDTH,
  minHeight: html.HTML_RENDER_MIN_HEIGHT,
  maxHeight: html.HTML_RENDER_MAX_HEIGHT,
  maxTitleLength: html.HTML_RENDER_MAX_TITLE_LENGTH,
};
process.stdout.write(`${JSON.stringify(constants, null, 1)}\n`);
