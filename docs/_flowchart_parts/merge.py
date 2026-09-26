# -*- coding: utf-8 -*-
"""Merge 4 shard HTML fragments into the skeleton, grouped by layer."""
import re
import os

PARTS = r"F:\syncguard\docs\_flowchart_parts"
SKELETON = os.path.join(PARTS, "skeleton.html")
SHARDS = [
    os.path.join(PARTS, "shard1.html"),
    os.path.join(PARTS, "shard2.html"),
    os.path.join(PARTS, "shard3.html"),
    os.path.join(PARTS, "shard4.html"),
]
OUTPUT = r"F:\syncguard\docs\代码文件流程图.html"

# Layer -> list of file numbers (as zero-padded strings)
LAYERS = {
    1:  ["01", "02"],
    2:  ["03", "04", "05", "06", "07"],
    3:  ["08", "09", "10", "11"],
    4:  ["12", "13", "14", "15", "16", "17", "18", "19"],
    5:  ["20", "21"],
    6:  ["22", "23", "24", "25"],
    7:  ["26", "27", "28", "29", "30", "31", "32", "33", "34"],
    8:  ["35", "36"],
    9:  ["37"],
    10: ["38", "39", "40", "41"],
}

def read_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def extract_sections(html):
    """Extract all <section class="file-block" id="file-XX">...</section> blocks.
    Returns dict: file_number -> section HTML."""
    pattern = re.compile(
        r'<section\s+class="file-block"\s+id="file-(\d+)"\s*>(.*?)</section>',
        re.DOTALL
    )
    sections = {}
    for m in pattern.finditer(html):
        num = m.group(1)
        sections[num] = m.group(0)
    return sections

def main():
    # Read skeleton
    skeleton = read_file(SKELETON)

    # Read all shards and extract sections
    all_sections = {}
    for shard in SHARDS:
        if not os.path.exists(shard):
            print(f"WARNING: shard not found: {shard}")
            continue
        html = read_file(shard)
        secs = extract_sections(html)
        print(f"  {os.path.basename(shard)}: extracted {len(secs)} sections -> {sorted(secs.keys())}")
        all_sections.update(secs)

    print(f"\nTotal sections extracted: {len(all_sections)}")
    print(f"File numbers: {sorted(all_sections.keys())}")

    # Check for missing files
    all_nums = set()
    for nums in LAYERS.values():
        all_nums.update(nums)
    missing = all_nums - set(all_sections.keys())
    if missing:
        print(f"ERROR: Missing sections for files: {sorted(missing)}")
    else:
        print("All 41 file sections present.")

    # Build layer content
    for layer_num, file_nums in LAYERS.items():
        placeholder = f"<!--LAYER{layer_num}-->"
        layer_content_parts = []
        for fn in file_nums:
            if fn in all_sections:
                layer_content_parts.append(all_sections[fn])
            else:
                layer_content_parts.append(f'<!-- MISSING: file-{fn} -->')
        layer_content = "\n".join(layer_content_parts)
        if placeholder in skeleton:
            skeleton = skeleton.replace(placeholder, layer_content)
            print(f"  Layer {layer_num}: inserted {len(file_nums)} sections")
        else:
            print(f"  ERROR: placeholder {placeholder} not found in skeleton!")

    # Write output
    with open(OUTPUT, "w", encoding="utf-8") as f:
        f.write(skeleton)

    size = os.path.getsize(OUTPUT)
    print(f"\nOutput written: {OUTPUT}")
    print(f"File size: {size:,} bytes ({size/1024:.1f} KB)")

    # Quick validation
    final = read_file(OUTPUT)
    section_count = len(re.findall(r'<section\s+class="file-block"', final))
    print(f"Section count in final: {section_count}")
    layer_count = len(re.findall(r'class="layer-section"', final))
    print(f"Layer section count: {layer_count}")
    has_title = "<title>" in final and "SyncGuard" in final
    print(f"Has proper title: {has_title}")

if __name__ == "__main__":
    main()
