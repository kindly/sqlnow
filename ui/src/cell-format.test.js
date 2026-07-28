// The pure half of the directive channel: which columns are visible and where
// each directive points, what a format string resolves to, and what a JSON cell
// spec becomes. These are the parts a bad change breaks silently — the grid
// draws to a canvas, so a wrong colour or a mis-mapped index looks like nothing
// at all until someone opens the page.

import { describe, it, expect } from 'vitest';
import {
  buildFormatPlan, buildColumnConfig, cellFormat, cellSpec, columnConfig,
  resolveColor, rowHeight, DEFAULT_ROW_HEIGHT,
} from './cell-format';

const LIGHT = 'light';
const DARK = 'dark';

// The token palette's `warn`, in the resolved form the grid is handed: every
// colour comes back as rgba() because that is the one notation glide is certain
// to parse. Written through resolveColor so these say "the token's own colour"
// rather than restating the normalisation.
const WARN_LIGHT = resolveColor('#fdeccd').css;
const WARN_DARK = resolveColor('#3d3115').css;

function bg(style) {
  return style?.themeOverride?.bgCell;
}

function fg(style) {
  return style?.themeOverride?.textDark;
}

describe('buildFormatPlan', () => {
  it('leaves a result with no directives exactly as it is', () => {
    const plan = buildFormatPlan(['a', 'b', 'c']);
    // the identity mapping is what every ordinary query goes through
    expect(plan.visibleToData).toEqual([0, 1, 2]);
    expect(plan.anyFormat).toBe(false);
    expect(plan.anyCell).toBe(false);
    expect(plan.anyColumn).toBe(false);
    expect(plan.rowHeightAt).toBe(-1);
    expect(plan.formatFor).toEqual([-1, -1, -1]);
  });

  it('hides directive columns and points each at its target', () => {
    const plan = buildFormatPlan([
      'name', '_sqlnow_format_name', 'co2', '_sqlnow_cell_co2',
      '_sqlnow_column_name', '_sqlnow_row_height',
    ]);
    expect(plan.visibleToData).toEqual([0, 2]);
    expect(plan.formatFor[0]).toBe(1);
    expect(plan.cellFor[2]).toBe(3);
    expect(plan.columnFor[0]).toBe(4);
    expect(plan.rowHeightAt).toBe(5);
    expect([plan.anyFormat, plan.anyCell, plan.anyColumn]).toEqual([true, true, true]);
  });

  it('hides an unknown directive without claiming to understand it', () => {
    // the whole prefix is reserved, so a later name is hidden here and stripped
    // from exports by the server, rather than showing up as a junk column
    const plan = buildFormatPlan(['a', '_sqlnow_whatever', '_sqlnow_cell_nosuchcolumn']);
    expect(plan.visibleToData).toEqual([0]);
    expect(plan.anyCell).toBe(false); // pointed at a column that is not there
  });

  it('matches case-insensitively, because DuckDB lowercases unquoted aliases', () => {
    const plan = buildFormatPlan(['CO2', '_sqlnow_format_co2']);
    expect(plan.visibleToData).toEqual([0]);
    expect(plan.formatFor[0]).toBe(1);
  });

  it('gives a duplicated column name to the first of them', () => {
    const plan = buildFormatPlan(['x', 'x', '_sqlnow_format_x']);
    expect(plan.visibleToData).toEqual([0, 1]);
    expect(plan.formatFor).toEqual([2, -1, -1]);
  });

  it('keeps data indexes stable so widths do not shift when a column hides', () => {
    // columnWidths is keyed by data index; if hiding renumbered the survivors,
    // resizing one column would resize a different one
    const plan = buildFormatPlan(['a', '_sqlnow_format_a', 'b', 'c']);
    expect(plan.visibleToData).toEqual([0, 2, 3]);
    expect(plan.visibleToData[1]).toBe(2);
  });
});

describe('cellFormat: the atoms', () => {
  it('resolves a token to its own colour per theme', () => {
    expect(bg(cellFormat('warn', LIGHT))).toBe(WARN_LIGHT);
    expect(bg(cellFormat('warn', DARK))).toBe(WARN_DARK);
  });

  it('takes a raw colour as the background', () => {
    expect(bg(cellFormat('#2d5016', LIGHT))).toBe('rgba(45, 80, 22, 1)');
  });

  it('reads a bare number as a position on the sequential ramp', () => {
    const low = bg(cellFormat('0', LIGHT));
    const high = bg(cellFormat('1', LIGHT));
    expect(low).toBeTruthy();
    expect(high).toBeTruthy();
    expect(low).not.toBe(high);
  });

  it('clamps a ramp position rather than running off the end', () => {
    expect(bg(cellFormat('5', LIGHT))).toBe(bg(cellFormat('1', LIGHT)));
    expect(bg(cellFormat('-5', LIGHT))).toBe(bg(cellFormat('0', LIGHT)));
  });

  it('never reads a negative as diverging', () => {
    // guessing between two ramps from one sign would be silently wrong; the
    // diverging ramp has to be asked for by name
    expect(bg(cellFormat('-1', LIGHT))).toBe(bg(cellFormat('0', LIGHT)));
    expect(bg(cellFormat('div:-1', LIGHT))).not.toBe(bg(cellFormat('0', LIGHT)));
  });

  it('ignores an empty or absent value', () => {
    expect(cellFormat('', LIGHT)).toBeNull();
    expect(cellFormat(null, LIGHT)).toBeNull();
    expect(cellFormat('   ', LIGHT)).toBeNull();
  });
});

describe('cellFormat: declarations', () => {
  it('accepts a token wherever a colour is expected', () => {
    // bg: used to take only literal colours, so `bg:warn` resolved to nothing
    // and the cell came out unstyled
    expect(bg(cellFormat('bg:warn', LIGHT))).toBe(WARN_LIGHT);
    expect(fg(cellFormat('bg:#000000; fg:warn', LIGHT))).toBe(WARN_LIGHT);
  });

  it('combines declarations, later winning', () => {
    const style = cellFormat('bg:#123456; fg:#ffffff; bold; align:right', LIGHT);
    expect(bg(style)).toBe('rgba(18, 52, 86, 1)');
    expect(fg(style)).toBe('rgba(255, 255, 255, 1)');
    expect(style.themeOverride.baseFontStyle).toContain('600');
    expect(style.contentAlign).toBe('right');
  });

  it('lets a bare token sit in a declaration list', () => {
    const style = cellFormat('warn; bold', LIGHT);
    expect(bg(style)).toBe(WARN_LIGHT);
    expect(style.themeOverride.baseFontStyle).toContain('600');
  });

  it('spells italic before weight, as the CSS font shorthand requires', () => {
    const style = cellFormat('italic; bold', LIGHT);
    expect(style.themeOverride.baseFontStyle).toMatch(/^italic 600 /);
  });

  it('skips what it does not understand and keeps the rest', () => {
    const style = cellFormat('flurb:3; bg:warn; heat:abc', LIGHT);
    expect(bg(style)).toBe(WARN_LIGHT);
  });

  it('treats an unresolvable colour as nothing, never as black', () => {
    // glide's own parser answers black for garbage, which would paint the cell
    expect(cellFormat('bg:not-a-colour', LIGHT)).toBeNull();
    expect(cellFormat('#12345', LIGHT)).toBeNull();
  });

  it('survives every malformed shape', () => {
    for (const bad of [';;;', ':', 'bg:', 'heat:', 'div:xyz', 'align:sideways', '{}']) {
      expect(() => cellFormat(bad, LIGHT)).not.toThrow();
    }
  });
});

describe('cellFormat: auto-contrast', () => {
  it('lightens text on a dark background and darkens it on a light one', () => {
    // the hot end of the light ramp is unreadable under the light theme's ink
    expect(fg(cellFormat('#0d366b', LIGHT))).toBe('#f7f8fa');
    expect(fg(cellFormat('#f5f5c0', DARK))).toBe('#0f1115');
  });

  it('leaves the theme ink alone when it is already legible', () => {
    // a subtle tint should keep the theme's character rather than being forced
    expect(fg(cellFormat('warn', LIGHT))).toBeUndefined();
  });

  it('does not override text the query asked for', () => {
    expect(fg(cellFormat('bg:#0d366b; fg:#ff0000', LIGHT))).toBe('rgba(255, 0, 0, 1)');
  });

  it('composites a translucent background before judging it', () => {
    // the same overlay is dark on a dark theme and light on a light one
    const onLight = fg(cellFormat('bg:rgba(0, 0, 0, 0.95)', LIGHT));
    const onDark = fg(cellFormat('bg:rgba(255, 255, 255, 0.95)', DARK));
    expect(onLight).toBe('#f7f8fa');
    expect(onDark).toBe('#0f1115');
  });
});

describe('cellSpec', () => {
  it('builds a bar, clamped to its range', () => {
    const spec = cellSpec('{"kind":"bar","value":99,"min":0,"max":1}', LIGHT);
    expect(spec.data.kind).toBe('range-cell');
    expect(spec.data.value).toBe(1);
    expect(spec.allowOverlay).toBe(false);
  });

  it('builds a sparkline with an axis it can scale against', () => {
    const spec = cellSpec('{"kind":"sparkline","values":[3,1,4]}', LIGHT);
    expect(spec.data.values).toEqual([3, 1, 4]);
    expect(spec.data.yAxis).toEqual([0, 4]);
    expect(spec.data.graphKind).toBe('line');
  });

  it('colours a tag from its text, so the colour is stable across rows', () => {
    const one = cellSpec('{"kind":"tags","tags":["coal","gas"]}', LIGHT);
    const two = cellSpec('{"kind":"tags","tags":["gas","coal"]}', LIGHT);
    const colourOf = (spec, tag) =>
      spec.data.possibleTags.find(t => t.tag === tag).color;
    expect(colourOf(one, 'coal')).toBe(colourOf(two, 'coal'));
    expect(colourOf(one, 'coal')).not.toBe(colourOf(one, 'gas'));
  });

  it('builds a link, a bool and a bubble', () => {
    expect(cellSpec('{"kind":"link","href":"https://x","text":"X"}', LIGHT).displayData).toBe('X');
    expect(cellSpec('{"kind":"bool","value":true}', LIGHT).data).toBe(true);
    expect(cellSpec('{"kind":"bubble","tags":["a"]}', LIGHT).data).toEqual(['a']);
  });

  it('carries the styling keys through the same grammar as a format string', () => {
    const spec = cellSpec('{"kind":"bar","value":0.5,"bg":"warn","bold":true}', LIGHT);
    expect(bg(spec)).toBe(WARN_LIGHT);
    expect(spec.themeOverride.baseFontStyle).toContain('600');
  });

  it('falls back to text for anything it cannot use', () => {
    // this is what lets a newer kind degrade instead of failing
    for (const bad of [
      '{"kind":"treemap"}',      // unknown kind
      '{"value":1}',             // no kind
      '{"kind":"bar", oops}',    // malformed json
      '[1,2,3]',                 // not an object
      'null',
      'warn',                    // a format string in the wrong column
      '',
    ]) {
      expect(cellSpec(bad, LIGHT)).toBeNull();
    }
  });

  it('does not offer to edit, because nothing in the grid edits', () => {
    expect(cellSpec('{"kind":"bool","value":true}', LIGHT).allowOverlay).toBe(false);
    expect(cellSpec('{"kind":"link","href":"https://x"}', LIGHT).readonly).toBe(true);
  });
});

describe('columnConfig and row height', () => {
  it('reads width, wrap and align', () => {
    expect(columnConfig('width:420; wrap; align:right', LIGHT))
      .toEqual({ width: 420, wrap: true, align: 'right' });
  });

  it('ignores a width that is not a usable number', () => {
    expect(columnConfig('width:banana', LIGHT)).toBeNull();
    expect(columnConfig('width:-5', LIGHT)).toBeNull();
  });

  it('takes column settings from the first row only', () => {
    const plan = buildFormatPlan(['a', '_sqlnow_column_a']);
    const configs = buildColumnConfig(plan, [['x', 'width:300'], ['y', 'width:99']], LIGHT);
    expect(configs[0].width).toBe(300);
  });

  it('has nothing to say about a result with no rows', () => {
    const plan = buildFormatPlan(['a', '_sqlnow_column_a']);
    expect(buildColumnConfig(plan, [], LIGHT)).toEqual([null, null]);
  });

  it('accepts a sane row height and refuses the rest', () => {
    expect(rowHeight('40')).toBe(40);
    expect(rowHeight('20')).toBe(20);
    expect(rowHeight('4')).toBe(0);      // below the floor
    expect(rowHeight('9000')).toBe(0);   // above the ceiling
    expect(rowHeight('-5')).toBe(0);
    expect(rowHeight('tall')).toBe(0);
    expect(rowHeight('')).toBe(0);
    expect(DEFAULT_ROW_HEIGHT).toBe(26);
  });
});

describe('resolveColor', () => {
  it('reads the pixel rather than the fillStyle string', () => {
    // Chrome hands oklch() and friends back verbatim, which glide cannot parse,
    // so the resolved form is always one glide is certain to understand
    expect(resolveColor('#2d5016')).toEqual({
      css: 'rgba(45, 80, 22, 1)',
      rgba: [45, 80, 22, 1],
    });
  });

  it('keeps alpha', () => {
    expect(resolveColor('rgba(255, 0, 0, 0.5)').rgba[3]).toBeCloseTo(0.5, 2);
  });

  it('answers null for what it cannot resolve', () => {
    for (const bad of ['not-a-colour', '', '#12345', 'flurb(1)']) {
      expect(resolveColor(bad)).toBeNull();
    }
  });
});
