import { createTheme } from '@uiw/codemirror-themes';
import { tags as t } from '@lezer/highlight';
import { storageKey } from './utils';

export function initialTheme() {
  const stored = localStorage.getItem(storageKey('theme'));
  if (stored === 'light' || stored === 'dark') {
    return stored;
  }
  if (window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches) {
    return 'light';
  }
  return 'dark';
}

export function applyTheme(theme) {
  document.documentElement.classList.toggle('dark', theme === 'dark');
  localStorage.setItem(storageKey('theme'), theme);
}

export function initialVim() {
  return localStorage.getItem(storageKey('vim')) !== 'off';
}

export function applyVim(enabled) {
  localStorage.setItem(storageKey('vim'), enabled ? 'on' : 'off');
}

const editorDark = createTheme({
  theme: 'dark',
  settings: {
    background: '#1a1d24',
    foreground: '#e9ebef',
    caret: '#79a7f5',
    selection: 'rgba(121, 167, 245, 0.25)',
    selectionMatch: 'rgba(121, 167, 245, 0.18)',
    lineHighlight: 'rgba(121, 167, 245, 0.06)',
    gutterBackground: '#1a1d24',
    gutterForeground: '#5c6472',
    gutterBorder: '#2b303b',
  },
  styles: [
    { tag: [t.keyword, t.operatorKeyword, t.modifier], color: '#79a7f5' },
    { tag: [t.string, t.special(t.string)], color: '#98c89f' },
    { tag: [t.number, t.bool, t.null], color: '#d9b078' },
    { tag: [t.comment, t.lineComment, t.blockComment], color: '#78818f', fontStyle: 'italic' },
    { tag: [t.operator, t.punctuation, t.separator], color: '#9ba3b0' },
    { tag: [t.function(t.variableName), t.standard(t.name)], color: '#c8a6e8' },
    { tag: [t.typeName, t.className], color: '#7fcbc4' },
  ],
});

const editorLight = createTheme({
  theme: 'light',
  settings: {
    background: '#ffffff',
    foreground: '#1b1e24',
    caret: '#2257cc',
    selection: 'rgba(34, 87, 204, 0.18)',
    selectionMatch: 'rgba(34, 87, 204, 0.12)',
    lineHighlight: 'rgba(34, 87, 204, 0.05)',
    gutterBackground: '#ffffff',
    gutterForeground: '#9aa1ad',
    gutterBorder: '#dbdee4',
  },
  styles: [
    { tag: [t.keyword, t.operatorKeyword, t.modifier], color: '#2257cc' },
    { tag: [t.string, t.special(t.string)], color: '#1e7b48' },
    { tag: [t.number, t.bool, t.null], color: '#9a5b00' },
    { tag: [t.comment, t.lineComment, t.blockComment], color: '#7c8492', fontStyle: 'italic' },
    { tag: [t.operator, t.punctuation, t.separator], color: '#5b6371' },
    { tag: [t.function(t.variableName), t.standard(t.name)], color: '#7b3fbf' },
    { tag: [t.typeName, t.className], color: '#0f766e' },
  ],
});

export function editorTheme(theme) {
  return theme === 'dark' ? editorDark : editorLight;
}

const gridFont = "'IBM Plex Mono', ui-monospace, monospace";

const gridDark = {
  accentColor: '#79a7f5',
  accentFg: '#0e1116',
  accentLight: 'rgba(121, 167, 245, 0.14)',
  textDark: '#e9ebef',
  textMedium: '#9ba3b0',
  textLight: '#78818f',
  textBubble: '#e9ebef',
  bgIconHeader: '#9ba3b0',
  fgIconHeader: '#1a1d24',
  textHeader: '#e9ebef',
  textHeaderSelected: '#0e1116',
  bgCell: '#14161b',
  bgCellMedium: '#1a1d24',
  bgHeader: '#1a1d24',
  bgHeaderHasFocus: '#232834',
  bgHeaderHovered: '#232834',
  bgBubble: '#232834',
  bgBubbleSelected: '#2b303b',
  bgSearchResult: 'rgba(217, 176, 120, 0.28)',
  borderColor: '#2b303b',
  drilldownBorder: '#3b4250',
  linkColor: '#79a7f5',
  fontFamily: gridFont,
  baseFontStyle: '12px',
  headerFontStyle: '600 12px',
  editorFontSize: '12px',
  lineHeight: 1.2,
  cellHorizontalPadding: 6,
  cellVerticalPadding: 1,
};

const gridLight = {
  accentColor: '#2257cc',
  accentFg: '#ffffff',
  accentLight: 'rgba(34, 87, 204, 0.10)',
  textDark: '#1b1e24',
  textMedium: '#5b6371',
  textLight: '#7c8492',
  textBubble: '#1b1e24',
  bgIconHeader: '#5b6371',
  fgIconHeader: '#ffffff',
  textHeader: '#1b1e24',
  textHeaderSelected: '#ffffff',
  bgCell: '#ffffff',
  bgCellMedium: '#f3f4f6',
  bgHeader: '#f3f4f6',
  bgHeaderHasFocus: '#ebedf1',
  bgHeaderHovered: '#ebedf1',
  bgBubble: '#ffffff',
  bgBubbleSelected: '#ebedf1',
  bgSearchResult: 'rgba(154, 91, 0, 0.16)',
  borderColor: '#dbdee4',
  drilldownBorder: '#b9bfc9',
  linkColor: '#2257cc',
  fontFamily: gridFont,
  baseFontStyle: '12px',
  headerFontStyle: '600 12px',
  editorFontSize: '12px',
  lineHeight: 1.2,
  cellHorizontalPadding: 6,
  cellVerticalPadding: 1,
};

export function gridTheme(theme) {
  return theme === 'dark' ? gridDark : gridLight;
}
