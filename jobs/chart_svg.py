"""Shared labels and scales for the offline SVG dashboards."""

from html import escape


def number(value, digits=2):
    return f'{float(value):,.{digits}f}'.replace(',', '_').replace('.', ',').replace('_', '.')


def svg(width, height, title, parts):
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" '
        f'role="img" aria-label="{escape(title)}">'
        '<style>text{font:15px Arial,sans-serif;fill:#334b61}'
        '.axis-title{font-weight:bold}.tick{font-size:14px}</style>'
        + ''.join(parts) + '</svg>'
    )


def axes(left, top, width, height, minimum, maximum, step, x_title, y_title):
    right, bottom = left + width, top + height
    middle = top + height / 2
    parts = []
    for tick in range(minimum, maximum + 1, step):
        y = top + (maximum - tick) * height / (maximum - minimum)
        parts.extend((
            f'<path d="M {left} {y:.1f} H {right}" stroke="#dbe5ed"/>',
            f'<text class="tick" x="{left-10}" y="{y+5:.1f}" text-anchor="end">{number(tick, 0)}</text>',
        ))
    parts.extend((
        f'<path d="M {left} {top} V {bottom} H {right}" fill="none" stroke="#60758a" stroke-width="1.3"/>',
        f'<text class="axis-title" x="{left+width/2:g}" y="{bottom+56}" text-anchor="middle">{escape(x_title)}</text>',
        f'<text class="axis-title" transform="translate(20 {middle:g}) rotate(-90)" '
        f'text-anchor="middle">{escape(y_title)}</text>',
    ))
    return parts
