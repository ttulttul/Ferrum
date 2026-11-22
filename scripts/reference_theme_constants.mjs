// The pinned reference's environment theme constants, which Rust's theme service and the
// t3_theme_* tools use as they are: the built-in ids, the ids nobody may publish, the stock
// theme's id, the CSS limit and the restyling guide agents read.
//
//   node scripts/reference_theme_constants.mjs <reference checkout>  > themes.json
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

const root = resolve(process.argv[2]);
const load = (path) => import(pathToFileURL(resolve(root, path)).href);
const palettes = await load("packages/shared/src/themePalettes.ts");
const { THEME_GUIDE } = await load("packages/shared/src/themeCss.ts");
const { THEME_CSS_MAX_LENGTH } = await load("packages/contracts/src/server.ts");

const constants = {
  builtIn: [...palettes.MOBILE_THEME_IDS],
  defaultThemeId: palettes.MOBILE_DEFAULT_THEME_ID,
  unpublishable: [...palettes.UNPUBLISHABLE_THEME_IDS].sort(),
  cssMaxLength: THEME_CSS_MAX_LENGTH,
  guide: THEME_GUIDE,
};
process.stdout.write(`${JSON.stringify(constants, null, 1)}\n`);
