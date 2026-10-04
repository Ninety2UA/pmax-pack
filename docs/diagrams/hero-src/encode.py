"""Encode the rendered PNG sequences into the README hero files.

Usage: python3 encode.py <work_dir> <out_dir>
<work_dir> holds light/, dark/, light-still/ and dark-still/ PNG sequences from
`hyperframes render --format png-sequence`. Needs Pillow built with WebP support.
"""
import os
import sys

from PIL import Image

FRAMES = 300          # 10 s at 30 fps
STATIC_FRAME = 259    # t = 8.6 s on the wide-camera render: gate passed, tables swapped, chart redrawn, report checked
DURATIONS = [34 if i % 3 == 0 else 33 for i in range(FRAMES)]  # sums to exactly 10000 ms
LOOP_START = 15       # open on the job's first pulse (t = 0.5 s) so motion starts on frame 1; the cycle is unchanged

work, out = sys.argv[1], sys.argv[2]
for theme, suffix in (("light", ""), ("dark", "-dark")):
    src = os.path.join(work, theme)
    order = [(LOOP_START + i) % FRAMES for i in range(FRAMES)]
    frames = [Image.open(os.path.join(src, "frame_%06d.png" % (i + 1))).convert("RGB") for i in order]
    webp = os.path.join(out, "readme-hero%s.webp" % suffix)
    frames[0].save(webp, save_all=True, append_images=frames[1:], duration=DURATIONS, loop=0,
                   quality=90, method=6, minimize_size=True)
    still = os.path.join(out, "readme-hero%s-static.png" % suffix)
    Image.open(os.path.join(work, theme + "-still", "frame_%06d.png" % STATIC_FRAME)).convert("RGB").save(still, optimize=True)
    print("%s  %.2f MB   %s  %.2f MB" % (webp, os.path.getsize(webp) / 1e6, still, os.path.getsize(still) / 1e6))
