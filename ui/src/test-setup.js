// jsdom has no canvas, and cell-format resolves colours by asking one: an
// invalid fillStyle is left unchanged by the browser, and the pixel is read back
// for the exact rgba whatever notation went in.
//
// This stands in for the browser's CSS parser over the notations the tests use —
// #rgb, #rrggbb, rgb() and rgba() — and deliberately rejects everything else,
// which is what makes the "an unresolvable colour is ignored" cases mean
// something. It is NOT a CSS parser: named colours, oklch(), lab() and color()
// are the real parser's job, and that they work (and that unsupported ones
// degrade to no formatting) is checked in a browser, not here.

const HEX = /^#([0-9a-f]{3}|[0-9a-f]{6})$/i;
const RGB = /^rgba?\(\s*([\d.]+)\s*,\s*([\d.]+)\s*,\s*([\d.]+)\s*(?:,\s*([\d.]+)\s*)?\)$/i;

function parse(value) {
  const text = String(value).trim();
  const hex = HEX.exec(text);
  if (hex !== null) {
    const h = hex[1].length === 3
      ? hex[1].split('').map(c => c + c).join('')
      : hex[1];
    return [
      parseInt(h.slice(0, 2), 16),
      parseInt(h.slice(2, 4), 16),
      parseInt(h.slice(4, 6), 16),
      1,
    ];
  }
  const rgb = RGB.exec(text);
  if (rgb !== null) {
    return [
      Math.round(Number(rgb[1])),
      Math.round(Number(rgb[2])),
      Math.round(Number(rgb[3])),
      rgb[4] === undefined ? 1 : Number(rgb[4]),
    ];
  }
  return null;
}

function hex2(n) {
  return n.toString(16).padStart(2, '0');
}

class StubContext {
  constructor() {
    this.rgba = [0, 0, 0, 1];
    this.style = '#000000';
    this.pixel = [0, 0, 0, 255];
  }

  get fillStyle() {
    return this.style;
  }

  // The load-bearing behaviour: assigning something unparseable is a no-op, so
  // two different sentinels reveal an invalid colour.
  set fillStyle(value) {
    const rgba = parse(value);
    if (rgba === null) return;
    this.rgba = rgba;
    this.style = rgba[3] === 1
      ? `#${hex2(rgba[0])}${hex2(rgba[1])}${hex2(rgba[2])}`
      : `rgba(${rgba[0]}, ${rgba[1]}, ${rgba[2]}, ${rgba[3]})`;
  }

  clearRect() {
    this.pixel = [0, 0, 0, 0];
  }

  fillRect() {
    const [r, g, b, a] = this.rgba;
    this.pixel = [r, g, b, Math.round(a * 255)];
  }

  getImageData() {
    return { data: this.pixel };
  }
}

window.HTMLCanvasElement.prototype.getContext = function getContext() {
  return new StubContext();
};
