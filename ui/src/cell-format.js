// Presentation carried in the result itself.
//
// A result column named `_sqlnow_format_<col>` holds the style for the column
// named `<col>`; `_sqlnow_cell_<col>` holds a JSON object saying what kind of
// cell it is; `_sqlnow_column_<col>` sizes it; `_sqlnow_row_height` sets row
// heights. All are hidden from the grid, and the server strips them from every
// export, so this lives in the SQL and needs nothing stored.
//
// The split between the two per-cell directives is the one glide itself makes:
// a string is cheap and styles a cell, JSON says what the cell *is*. They
// compose, so a bar can still take a background from the format column.
//
// The one absolute rule: a bad directive must never break a frame. Unknown
// tokens, unknown kinds, malformed JSON, unparseable numbers and unresolvable
// colours all mean "this contributed nothing".

import { GridCellKind } from '@glideapps/glide-data-grid';
import { gridTheme } from './theme';

export const FORMAT_PREFIX = '_sqlnow_format_';
export const CELL_PREFIX = '_sqlnow_cell_';
export const COLUMN_PREFIX = '_sqlnow_column_';
export const ROW_HEIGHT = '_sqlnow_row_height';
export const DIRECTIVE_PREFIX = '_sqlnow_';

export const DEFAULT_ROW_HEIGHT = 26;
const MIN_ROW_HEIGHT = 16;
const MAX_ROW_HEIGHT = 400;

// --- palettes ---------------------------------------------------------------

// Sequential: one hue, light to dark. The full range is right for magnitude —
// the lightest step means "near zero" and is allowed to recede into the surface.
const SEQ_LIGHT = ['#cde2fb', '#9ec5f4', '#6da7ec', '#3987e5', '#256abf', '#184f95', '#0d366b'];
// Dark is a selected ramp, not a flip of the above: low values recede toward
// the dark cell background, and the hot end stops short of the near-white step
// that would glare.
const SEQ_DARK = ['#0d366b', '#184f95', '#256abf', '#3987e5', '#5598e7', '#6da7ec', '#86b6ef'];

// Diverging: two poles that read as opposite, with a neutral gray midpoint. A
// hue at the midpoint would read as a value rather than as zero.
const NEG_LIGHT = ['#fbdcd9', '#f6bdb7', '#ef9a92', '#e34948', '#c23a39', '#9c2c2b', '#75201f'];
const NEG_DARK = ['#5c2321', '#7d2f2b', '#a33d38', '#e66767', '#ee8681', '#f4a29e', '#f8bcb9'];
const MID_LIGHT = '#f0efec';
const MID_DARK = '#383835';

const TOKENS_LIGHT = {
  ok: '#e3f3e8',
  added: '#e3f3e8',
  error: '#fbe4e4',
  removed: '#fbe4e4',
  changed: '#fdf1dc',
  warn: '#fdeccd',
  muted: '#f0efec',
};
const TOKENS_DARK = {
  ok: '#16301f',
  added: '#16301f',
  error: '#3a1b1b',
  removed: '#3a1b1b',
  changed: '#3a2f16',
  warn: '#3d3115',
  muted: '#2a2a28',
};

// Ink for the auto-contrast fallback, when neither theme's own text colour is
// legible on the background the query asked for.
const INK_ON_LIGHT = '#0f1115';
const INK_ON_DARK = '#f7f8fa';

// --- colour validation ------------------------------------------------------

// Our own palette literals, which are always #rrggbb.
function hexRgb(hex) {
  return [
    parseInt(hex.slice(1, 3), 16),
    parseInt(hex.slice(3, 5), 16),
    parseInt(hex.slice(5, 7), 16),
    1,
  ];
}

// glide's own colour parser returns black rather than throwing on garbage, so a
// typo would silently paint a cell black. Validate here instead, on a 1x1
// canvas: assigning an invalid fillStyle leaves the previous value in place, so
// two different sentinels expose the failure.
//
// Then read the pixel rather than the fillStyle string. Chrome hands back
// `oklch(...)`, `lab(...)` and `color(...)` verbatim instead of converting them,
// and glide cannot parse those — it would paint the cell black. A pixel is exact
// for every syntax the browser supports, alpha included, and anything it does
// not support fails validation and means "no formatting".
let probeCtx;
const colorCache = new Map();

export function resolveColor(css) {
  const key = String(css).trim().toLowerCase();
  if (key === '') return null;
  const hit = colorCache.get(key);
  if (hit !== undefined) return hit;

  let out = null;
  if (probeCtx === undefined) {
    const canvas = document.createElement('canvas');
    canvas.width = 1;
    canvas.height = 1;
    probeCtx = canvas.getContext('2d', { willReadFrequently: true });
  }
  if (probeCtx) {
    probeCtx.fillStyle = '#000000';
    probeCtx.fillStyle = key;
    const black = probeCtx.fillStyle;
    probeCtx.fillStyle = '#ffffff';
    probeCtx.fillStyle = key;
    if (black === probeCtx.fillStyle) {
      probeCtx.clearRect(0, 0, 1, 1);
      probeCtx.fillStyle = key;
      probeCtx.fillRect(0, 0, 1, 1);
      const d = probeCtx.getImageData(0, 0, 1, 1).data;
      const a = d[3] / 255;
      const rgba = [d[0], d[1], d[2], a];
      // handed to glide as rgba(), the one form it is certain to parse
      out = { css: `rgba(${d[0]}, ${d[1]}, ${d[2]}, ${a})`, rgba };
    }
  }

  if (colorCache.size > 2048) colorCache.clear();
  colorCache.set(key, out);
  return out;
}

// --- contrast ---------------------------------------------------------------

function lin(c) {
  const s = c / 255;
  return s <= 0.04045 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4;
}

function luminance(rgb) {
  return 0.2126 * lin(rgb[0]) + 0.7152 * lin(rgb[1]) + 0.0722 * lin(rgb[2]);
}

function ratio(a, b) {
  return (Math.max(a, b) + 0.05) / (Math.min(a, b) + 0.05);
}

// A translucent background is the theme-portable authoring form, and its
// apparent luminance depends on what it sits on, so composite first.
function over(fg, bg) {
  const a = fg[3];
  if (a >= 1) return fg;
  return [
    fg[0] * a + bg[0] * (1 - a),
    fg[1] * a + bg[1] * (1 - a),
    fg[2] * a + bg[2] * (1 - a),
    1,
  ];
}

// undefined means "the theme's own text colour is fine" — preferred, because it
// keeps the theme's character on subtle tints. Only a background dark or light
// enough to break legibility forces our ink.
function autoFg(bgRgba, pal) {
  const l = luminance(over(bgRgba, pal.baseRgb));
  if (ratio(l, pal.textL) >= 4.5) return undefined;
  return ratio(l, pal.inkDarkL) >= ratio(l, pal.inkLightL) ? INK_ON_LIGHT : INK_ON_DARK;
}

// --- ramps ------------------------------------------------------------------

function srgb(l) {
  const s = l <= 0.0031308 ? l * 12.92 : 1.055 * l ** (1 / 2.4) - 0.055;
  return Math.max(0, Math.min(255, Math.round(s * 255)));
}

// Interpolate in linear light: a naive sRGB lerp between two adjacent blue
// steps visibly darkens the midpoint.
function mix(aHex, bHex, f) {
  const a = hexRgb(aHex);
  const b = hexRgb(bHex);
  let out = '#';
  for (let k = 0; k < 3; k++) {
    out += srgb(lin(a[k]) * (1 - f) + lin(b[k]) * f)
      .toString(16)
      .padStart(2, '0');
  }
  return out;
}

function ramp(stops, t) {
  if (!(t > 0)) return stops[0]; // also catches NaN
  if (t >= 1) return stops[stops.length - 1];
  const x = t * (stops.length - 1);
  const i = Math.min(Math.floor(x), stops.length - 2);
  return mix(stops[i], stops[i + 1], x - i);
}

function diverging(t, pal) {
  if (t < 0) return ramp([pal.mid, ...pal.neg], Math.min(1, -t));
  return ramp([pal.mid, ...pal.pos], Math.min(1, t || 0));
}

// --- theme-derived palette --------------------------------------------------

// Derived from gridTheme so the surface and ink can never drift from theme.js.
const palettes = new Map();

function palette(theme) {
  let pal = palettes.get(theme);
  if (pal !== undefined) return pal;
  const grid = gridTheme(theme);
  const dark = theme === 'dark';
  // through the same resolver as user colours, so the surface and ink can never
  // drift from theme.js whatever notation it uses
  const base = resolveColor(grid.bgCell);
  const text = resolveColor(grid.textDark);
  pal = {
    baseRgb: base ? base.rgba : hexRgb('#ffffff'),
    textL: luminance(text ? text.rgba : hexRgb('#000000')),
    inkDarkL: luminance(hexRgb(INK_ON_LIGHT)),
    inkLightL: luminance(hexRgb(INK_ON_DARK)),
    fontSize: grid.baseFontStyle,
    tokens: dark ? TOKENS_DARK : TOKENS_LIGHT,
    pos: dark ? SEQ_DARK : SEQ_LIGHT,
    neg: dark ? NEG_DARK : NEG_LIGHT,
    mid: dark ? MID_DARK : MID_LIGHT,
  };
  palettes.set(theme, pal);
  return pal;
}

// --- parsing ----------------------------------------------------------------

const NUMBER = /^[+-]?(?:\d+(?:\.\d+)?|\.\d+)$/;

function clamp01(n) {
  return Math.max(0, Math.min(1, n));
}

// An atom: a token name, a bare number meaning a position on the sequential
// ramp, or a colour. A bare number is always sequential — reading a negative as
// diverging would guess between two ramps from one sign, and guess wrong
// silently. Diverging has to be spelled `div:`.
function atom(text, pal, into) {
  const token = pal.tokens[text];
  if (token !== undefined) {
    into.bg = token;
    return;
  }
  if (NUMBER.test(text)) {
    into.bg = ramp(pal.pos, clamp01(Number(text)));
    return;
  }
  if (resolveColor(text) !== null) into.bg = text;
  // anything else contributed nothing
}

function declaration(name, value, pal, into) {
  switch (name) {
    case 'bg':
    case 'background':
      // a token name is a colour here too: `bg:warn` has to mean what `warn`
      // means, or the vocabulary only half works
      into.bg = pal.tokens[value] ?? value;
      break;
    case 'fg':
    case 'color':
    case 'text':
      into.fg = pal.tokens[value] ?? value;
      break;
    case 'heat':
      if (NUMBER.test(value)) into.bg = ramp(pal.pos, clamp01(Number(value)));
      break;
    case 'div':
      if (NUMBER.test(value)) into.bg = diverging(Math.max(-1, Math.min(1, Number(value))), pal);
      break;
    case 'token':
      if (pal.tokens[value] !== undefined) into.bg = pal.tokens[value];
      break;
    case 'bold':
      into.bold = value !== '0' && value !== 'false';
      break;
    case 'italic':
      into.italic = value !== '0' && value !== 'false';
      break;
    case 'align':
      if (value === 'left' || value === 'right' || value === 'center') into.align = value;
      break;
    case 'width': {
      const n = Number(value);
      if (NUMBER.test(value) && n > 0) into.width = Math.round(n);
      break;
    }
    case 'wrap':
      into.wrap = value !== '0' && value !== 'false';
      break;
    default:
    // unknown declarations are skipped, so a newer name degrades to plain text
  }
}

// Everything a format string can say, before it is turned into what the grid
// wants. Shared by cell and column parsing — the two differ only in which
// properties they go on to use.
function parseInto(raw, pal) {
  const text = raw.trim().toLowerCase();
  if (text === '') return null;
  const into = {};
  if (text.includes(';') || text.includes(':')) {
    // A declaration list. The test is safe because no CSS colour syntax
    // contains either character.
    for (const part of text.split(';')) {
      const decl = part.trim();
      if (decl === '') continue;
      const colon = decl.indexOf(':');
      if (colon < 0) {
        // a bare word in a list: `warn; bold` should work
        declaration(decl, '', pal, into);
        if (pal.tokens[decl] !== undefined) into.bg = pal.tokens[decl];
        continue;
      }
      declaration(decl.slice(0, colon).trim(), decl.slice(colon + 1).trim(), pal, into);
    }
  } else {
    atom(text, pal, into);
  }
  return into;
}

// The object handed to glide, allocated once per distinct string and shared by
// every cell that uses it — glide does not cache the per-cell theme merge, so
// building one of these per call would be per-frame garbage.
function finalize(parsed, pal) {
  const override = {};
  let bgRgba = null;
  if (parsed.bg !== undefined) {
    const bg = resolveColor(parsed.bg);
    if (bg !== null) {
      override.bgCell = bg.css;
      bgRgba = bg.rgba;
    }
  }
  if (parsed.fg !== undefined) {
    const fg = resolveColor(parsed.fg);
    if (fg !== null) override.textDark = fg.css;
  } else if (bgRgba !== null) {
    const auto = autoFg(bgRgba, pal);
    if (auto !== undefined) override.textDark = auto;
  }
  if (parsed.bold || parsed.italic) {
    override.baseFontStyle =
      `${parsed.italic ? 'italic ' : ''}${parsed.bold ? '600 ' : ''}${pal.fontSize}`;
  }
  const styled = Object.keys(override).length > 0;
  if (!styled && parsed.align === undefined) return null;
  return { themeOverride: styled ? override : undefined, contentAlign: parsed.align };
}

const styleCache = new Map();

/// The style for one cell, or null for none. Memoised per theme and string.
export function cellFormat(raw, theme) {
  if (!raw) return null; // '' and NULL arrive identically
  const key = `${theme} ${raw}`;
  const hit = styleCache.get(key);
  if (hit !== undefined) return hit;
  let style = null;
  try {
    const pal = palette(theme);
    const parsed = parseInto(raw, pal);
    if (parsed !== null) style = finalize(parsed, pal);
  } catch {
    style = null; // a bad format string can leave a cell plain, never break a frame
  }
  if (styleCache.size > 4096) styleCache.clear();
  styleCache.set(key, style);
  return style;
}

// --- JSON cell specs --------------------------------------------------------

// A `_sqlnow_cell_<col>` value is a JSON object: `kind` picks the cell, the rest
// is that kind's payload, and the styling keys of the string grammar work here
// too so one column can do both. An unknown kind falls back to text, which is
// what lets a newer kind degrade instead of failing.
//
// Everything is read-only. The grid has no onCellEdited, so a cell that offered
// to edit would be lying about what happens next.

function num(v, fallback) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function strings(v) {
  if (Array.isArray(v)) return v.filter(x => x !== null && x !== undefined).map(String);
  if (v === null || v === undefined || v === '') return [];
  return [String(v)];
}

function numbers(v) {
  return Array.isArray(v) ? v.map(x => num(x, 0)) : [];
}

// The kinds, each returning the glide cell minus the styling, which is merged in
// by cellSpec. `text` is the shared fallback.
const KINDS = {
  link: (s) => ({
    kind: GridCellKind.Uri,
    data: String(s.href ?? s.value ?? ''),
    displayData: String(s.text ?? s.href ?? s.value ?? ''),
    readonly: true,
    hoverEffect: true,
  }),
  // No `markdown` kind: glide renders markdown only in the cell's overlay, so in
  // the row it shows raw asterisks — worse than not offering it. No `image`
  // either: it drew nothing in testing, and it would have the grid fetch
  // whatever URL the SQL names, which deserves its own decision.
  bool: (s) => ({
    kind: GridCellKind.Boolean,
    data: s.value === true || s.value === 'true' || s.value === 1,
    readonly: true,
    allowOverlay: false,
  }),
  bubble: (s) => ({ kind: GridCellKind.Bubble, data: strings(s.tags ?? s.value) }),
  // bar: a magnitude inside the cell. min/max default to a 0..1 fraction, which
  // is the same shape `heat:` takes, so a column can switch between the two.
  bar: (s) => {
    const min = num(s.min, 0);
    const max = num(s.max, 1);
    const value = num(s.value, min);
    return {
      kind: GridCellKind.Custom,
      allowOverlay: false,
      copyData: String(s.label ?? value),
      data: {
        kind: 'range-cell',
        value: Math.max(min, Math.min(max, value)),
        min,
        max,
        step: num(s.step, (max - min) / 100 || 0.01),
        label: s.label === undefined ? undefined : String(s.label),
      },
    };
  },
  sparkline: (s) => {
    const values = numbers(s.values ?? s.value);
    const lo = values.length ? Math.min(...values) : 0;
    const hi = values.length ? Math.max(...values) : 1;
    return {
      kind: GridCellKind.Custom,
      allowOverlay: false,
      copyData: values.join(', '),
      data: {
        kind: 'sparkline-cell',
        values,
        graphKind: ['line', 'bar', 'area'].includes(s.graph) ? s.graph : 'line',
        // the axis has to be given or the renderer has nothing to scale against
        yAxis: [num(s.min, Math.min(0, lo)), num(s.max, hi)],
        color: s.color === undefined ? undefined : resolveColor(s.color)?.css,
        hideAxis: s.axis === false,
      },
    };
  },
  tags: (s) => {
    const tags = strings(s.tags ?? s.value);
    // the renderer needs a colour per tag, and a stable one, so it is derived
    // from the tag text rather than from its position in this row
    const possibleTags = tags.map(tag => ({ tag, color: tagColor(tag) }));
    return {
      kind: GridCellKind.Custom,
      allowOverlay: false,
      copyData: tags.join(', '),
      data: { kind: 'tags-cell', tags, possibleTags },
    };
  },
};

const TAG_HUES = ['#3987e5', '#d95926', '#199e70', '#c98500', '#d55181', '#9085e9', '#e66767'];

function tagColor(tag) {
  let h = 0;
  for (let i = 0; i < tag.length; i++) h = (h * 31 + tag.charCodeAt(i)) | 0;
  return TAG_HUES[Math.abs(h) % TAG_HUES.length];
}

const specCache = new Map();

/// The cell a JSON directive asks for, or null. Merges the styling keys through
/// the same grammar as the format column, so `bg`/`bold`/`align` work here too.
export function cellSpec(raw, theme) {
  if (!raw) return null;
  const key = `${theme} ${raw}`;
  const hit = specCache.get(key);
  if (hit !== undefined) return hit;
  let spec = null;
  try {
    const s = JSON.parse(raw);
    if (s !== null && typeof s === 'object' && !Array.isArray(s)) {
      const build = KINDS[String(s.kind ?? '').toLowerCase()];
      // an unknown kind is not an error; it is a newer name, so show the text
      const cell = build ? build(s) : null;
      if (cell !== null) {
        const pal = palette(theme);
        const style = finalize(parseInto(styleOf(s), pal) ?? {}, pal);
        spec = {
          ...cell,
          themeOverride: style?.themeOverride,
          contentAlign: style?.contentAlign,
        };
      }
    }
  } catch {
    spec = null; // malformed JSON leaves the cell as plain text
  }
  if (specCache.size > 2048) specCache.clear();
  specCache.set(key, spec);
  return spec;
}

// The styling keys of a JSON spec, rewritten as the string grammar so there is
// exactly one parser for `bg`, `fg`, `bold`, `italic` and `align`.
function styleOf(s) {
  const parts = [];
  for (const k of ['bg', 'fg', 'align']) {
    if (s[k] !== undefined) parts.push(`${k}:${s[k]}`);
  }
  for (const k of ['bold', 'italic']) {
    if (s[k]) parts.push(k);
  }
  return parts.join('; ');
}

const columnCache = new Map();

/// Column-wide settings, read from the first row. Theme-independent.
export function columnConfig(raw, theme) {
  if (!raw) return null;
  const hit = columnCache.get(raw);
  if (hit !== undefined) return hit;
  let config = null;
  try {
    const parsed = parseInto(raw, palette(theme));
    if (parsed !== null && (parsed.width !== undefined || parsed.wrap || parsed.align)) {
      config = { width: parsed.width, wrap: parsed.wrap === true, align: parsed.align };
    }
  } catch {
    config = null;
  }
  if (columnCache.size > 512) columnCache.clear();
  columnCache.set(raw, config);
  return config;
}

/// A row height, or 0 when the value does not name a usable one.
export function rowHeight(raw) {
  if (!raw) return 0;
  const n = Number(raw);
  if (!Number.isFinite(n)) return 0;
  const h = Math.round(n);
  return h >= MIN_ROW_HEIGHT && h <= MAX_ROW_HEIGHT ? h : 0;
}

// --- the index plan ---------------------------------------------------------

/// Where every directive column points, and which columns the grid shows.
///
/// Matching is case-insensitive because DuckDB lowercases unquoted aliases, so
/// `AS _sqlnow_format_CO2` names the column `_sqlnow_format_co2` and still has
/// to find `co2`. Duplicate column names: the first wins, which is the same
/// ambiguity the grid already has in its column ids.
export function buildFormatPlan(headers) {
  const byName = new Map();
  for (let i = 0; i < headers.length; i++) {
    const key = headers[i].toLowerCase();
    if (!byName.has(key)) byName.set(key, i);
  }

  const visibleToData = [];
  const formatFor = new Array(headers.length).fill(-1);
  const cellFor = new Array(headers.length).fill(-1);
  const columnFor = new Array(headers.length).fill(-1);
  let rowHeightAt = -1;
  let anyFormat = false;
  let anyCell = false;
  let anyColumn = false;

  for (let i = 0; i < headers.length; i++) {
    const low = headers[i].toLowerCase();
    if (low === ROW_HEIGHT) continue;
    if (low.startsWith(FORMAT_PREFIX)) {
      const target = byName.get(low.slice(FORMAT_PREFIX.length));
      if (target !== undefined) {
        formatFor[target] = i;
        anyFormat = true;
      }
      continue;
    }
    if (low.startsWith(CELL_PREFIX)) {
      const target = byName.get(low.slice(CELL_PREFIX.length));
      if (target !== undefined) {
        cellFor[target] = i;
        anyCell = true;
      }
      continue;
    }
    if (low.startsWith(COLUMN_PREFIX)) {
      const target = byName.get(low.slice(COLUMN_PREFIX.length));
      if (target !== undefined) {
        columnFor[target] = i;
        anyColumn = true;
      }
      continue;
    }
    // the whole prefix is reserved, so an unknown directive is hidden too
    if (low.startsWith(DIRECTIVE_PREFIX)) continue;
    visibleToData.push(i);
  }

  const height = byName.get(ROW_HEIGHT);
  if (height !== undefined) rowHeightAt = height;

  return {
    visibleToData, formatFor, cellFor, columnFor, rowHeightAt,
    anyFormat, anyCell, anyColumn,
  };
}

/// The per-column settings, resolved from the first row. Empty when a result has
/// no rows, which is harmless — there is nothing to lay out.
export function buildColumnConfig(plan, rows, theme) {
  const configs = new Array(plan.columnFor.length).fill(null);
  if (!plan.anyColumn || rows.length === 0) return configs;
  for (let i = 0; i < plan.columnFor.length; i++) {
    const src = plan.columnFor[i];
    if (src >= 0) configs[i] = columnConfig(rows[0][src], theme);
  }
  return configs;
}
