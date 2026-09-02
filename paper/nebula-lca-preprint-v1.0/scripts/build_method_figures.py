from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch, Rectangle


BLUE = "#3B6FB6"
TEAL = "#2A8C82"
ORANGE = "#D97706"
TEXT = "#1F2937"
GRID = "#D9D9D9"


def box(ax, xy, width, height, title, body="", color=BLUE, fill="#F5F8FC", title_size=8.2, body_size=7):
    patch = FancyBboxPatch(
        xy, width, height, boxstyle="round,pad=0.02,rounding_size=0.025",
        linewidth=1.0, edgecolor=color, facecolor=fill,
    )
    ax.add_patch(patch)
    ax.text(xy[0] + width / 2, xy[1] + height * 0.64, title, ha="center", va="center", weight="bold", color=TEXT, fontsize=title_size)
    if body:
        ax.text(xy[0] + width / 2, xy[1] + height * 0.30, body, ha="center", va="center", color=TEXT, fontsize=body_size)
    return patch


def arrow(ax, start, end, color=TEXT, dashed=False):
    ax.add_patch(FancyArrowPatch(start, end, arrowstyle="-|>", mutation_scale=10, lw=1.0,
                                 color=color, linestyle="--" if dashed else "-"))


def finish(fig, output: Path, name: str):
    output.mkdir(parents=True, exist_ok=True)
    for suffix, options in ((".pdf", {}), (".svg", {}), (".png", {"dpi": 600})):
        fig.savefig(output / f"{name}{suffix}", bbox_inches="tight", **options)
    plt.close(fig)


def method_chain(output: Path):
    fig, ax = plt.subplots(figsize=(7.1, 2.55))
    ax.set(xlim=(0, 10), ylim=(0, 3.2)); ax.axis("off")
    labels = [
        ("Source record", "Observed inputs, products,\nwastes, and emissions"),
        ("Attribution", "Exchange classes, allocation\nweights, and unit evidence"),
        ("Product model", "Derived process columns with\nnew identities and lineage"),
        ("Solve / PTS compile", "Functional-demand solve and\nboundary projection"),
    ]
    xs = [0.15, 2.7, 5.25, 7.8]
    for index, ((title, body), x) in enumerate(zip(labels, xs, strict=True)):
        box(ax, (x, 1.35), 2.05, 1.05, title, body, color=BLUE if index < 3 else TEAL)
        if index:
            arrow(ax, (x - 0.5, 1.88), (x, 1.88), color=BLUE)
    ax.plot([0.35, 9.65], [0.68, 0.68], color=ORANGE, lw=1.2)
    ax.text(5, 0.34, "Lineage preserved across source revision, transformation receipt, and calculation receipt", ha="center", color=TEXT, fontsize=7.5)
    finish(fig, output, "figure1_method_chain")


def quantity_semantics(output: Path):
    fig, axes = plt.subplots(1, 2, figsize=(7.1, 2.85))
    for ax in axes: ax.set(xlim=(0, 5), ylim=(0, 3.3)); ax.axis("off")
    axes[0].text(2.5, 2.95, "Source-state transfer audit", ha="center", weight="bold", color=TEXT, fontsize=9)
    box(axes[0], (0.2, 1.35), 1.65, 1.0, "Supplier record", "Quantity = 4\nCoefficient = 0.5", BLUE)
    box(axes[0], (3.15, 1.35), 1.65, 1.0, "Consumer record", "Quantity = 2\nCoefficient = 1.0", BLUE)
    arrow(axes[0], (1.85, 1.85), (3.15, 1.85), BLUE)
    axes[0].text(2.5, 0.75, "Realized transfer: 2.0 = 2.0", ha="center", fontsize=8, weight="bold", color=TEXT)
    axes[0].text(2.5, 0.40, "Audit compares quantities on the same source basis", ha="center", fontsize=7, color=TEXT)

    axes[1].text(2.5, 2.95, "Normalized computational closure", ha="center", weight="bold", color=TEXT, fontsize=9)
    box(axes[1], (0.15, 1.7), 1.55, 0.85, "Provider", "Coefficient = 1\nSolved activity = 2", TEAL)
    box(axes[1], (0.15, 0.45), 1.55, 0.85, "Consumer", "Coefficient = 2\nSolved activity = 1", TEAL)
    box(axes[1], (2.25, 1.05), 1.1, 0.9, "Scale", "coefficient\n× activity", ORANGE, "#FFF8ED")
    box(axes[1], (3.65, 1.05), 1.2, 0.9, "Ledger", "2.0 supplied\n2.0 consumed", TEAL, title_size=8, body_size=6.5)
    arrow(axes[1], (1.7, 2.05), (2.25, 1.62), TEAL); arrow(axes[1], (1.7, 0.88), (2.25, 1.36), TEAL); arrow(axes[1], (3.35, 1.5), (3.65, 1.5), TEAL)
    axes[1].text(2.5, 0.15, "Raw coefficients differ; activity-scaled exchanges close", ha="center", fontsize=7, color=TEXT)
    finish(fig, output, "figure2_quantity_semantics")


def pts_operator(output: Path):
    fig, ax = plt.subplots(figsize=(7.1, 3.25)); ax.set(xlim=(0, 10), ylim=(0, 4.4)); ax.axis("off")
    boundary = Rectangle((0.25, 0.75), 4.8, 3.0, facecolor="#F8FAFC", edgecolor=BLUE, lw=1.2, linestyle="--")
    ax.add_patch(boundary); ax.text(0.45, 3.48, "Expanded two-process PTS", color=BLUE, weight="bold", fontsize=9)
    box(ax, (0.75, 1.75), 1.55, 0.9, "Hydrotreating", "activity = 1.25", BLUE)
    box(ax, (3.0, 1.75), 1.55, 0.9, "Reforming", "activity = 1.25", BLUE)
    arrow(ax, (2.3, 2.30), (3.0, 2.30), BLUE)
    arrow(ax, (3.0, 2.02), (2.3, 2.02), ORANGE)
    ax.text(2.65, 2.67, "hydrotreated naphtha", ha="center", fontsize=6.5, color=BLUE)
    ax.text(2.65, 1.58, "0.2 recycle coefficient", ha="center", fontsize=6.5, color=ORANGE)
    ax.text(0.55, 1.25, r"External inputs: 1.000 kg naphtha; 0.025 kg H$_2$; 6.250 MJ electricity", fontsize=6.8, color=TEXT)
    ax.text(0.55, 0.98, r"Direct output: 0.100 kg fossil CO$_2$", fontsize=6.8, color=TEXT)

    arrow(ax, (5.05, 2.25), (6.0, 2.25), TEAL)
    ax.text(5.53, 2.55, "solve and\nproject", ha="center", fontsize=7, color=TEAL, weight="bold")
    compiled = FancyBboxPatch((6.0, 1.15), 3.65, 2.25, boxstyle="round,pad=0.02,rounding_size=0.025", linewidth=1.0, edgecolor=TEAL, facecolor="#F3FAF8")
    ax.add_patch(compiled)
    ax.text(7.825, 3.02, "Compiled PTS boundary", ha="center", va="center", weight="bold", color=TEXT, fontsize=9)
    ax.text(7.825, 2.70, "Reference product: 1.000 kg reformate", ha="center", va="center", color=TEXT, fontsize=7)
    ax.text(7.825, 2.28, "Boundary inputs", ha="center", va="center", weight="bold", color=TEXT, fontsize=7.2)
    ax.text(7.825, 1.95, r"1.000 kg naphtha · 0.025 kg H$_2$ · 6.250 MJ electricity", ha="center", va="center", color=TEXT, fontsize=6.6)
    ax.text(7.825, 1.58, r"Elementary output: 0.100 kg fossil CO$_2$", ha="center", va="center", color=TEXT, fontsize=6.8)
    ax.text(7.83, 0.65, r"Non-exposed co-product: 0.0375 kg H$_2$ (reported separately)", ha="center", fontsize=6.8, color="#6B7280")
    finish(fig, output, "figure3_pts_operator")


def system_boundary(output: Path):
    fig, ax = plt.subplots(figsize=(7.1, 3.1)); ax.set(xlim=(0, 11), ylim=(0, 4.2)); ax.axis("off")
    boundary = Rectangle((1.55, 0.65), 7.7, 2.9, facecolor="#F8FAFC", edgecolor=BLUE, lw=1.2, linestyle="--")
    ax.add_patch(boundary); ax.text(1.75, 3.3, "Foreground benchmark boundary", color=BLUE, weight="bold")
    titles = ["Distillation", "Hydrotreating", "Reforming", "Blending"]
    xs = [1.85, 3.65, 5.45, 7.25]
    for title, x in zip(titles, xs, strict=True): box(ax, (x, 1.65), 1.25, 0.75, title, "balanced", BLUE, title_size=6.8, body_size=6.5)
    for x in xs[1:]: arrow(ax, (x - 0.55, 2.02), (x, 2.02), BLUE)
    box(ax, (0.05, 2.35), 1.1, 0.65, "Crude feed", "boundary input", TEAL)
    box(ax, (0.05, 1.15), 1.1, 0.65, "Electricity", "normalized supply", ORANGE, "#FFF8ED")
    arrow(ax, (1.15, 2.67), (1.85, 2.25), TEAL); arrow(ax, (1.15, 1.48), (1.85, 1.82), ORANGE)
    ax.plot([1.15, 8.2], [1.48, 1.48], color=ORANGE, lw=0.9, linestyle="--")
    for x in xs[1:]: arrow(ax, (x + 0.62, 1.48), (x + 0.62, 1.65), ORANGE)
    ax.add_patch(FancyArrowPatch((6.05, 2.48), (4.25, 2.48), arrowstyle="-|>", connectionstyle="arc3,rad=0.35", mutation_scale=10, lw=1.0, color=ORANGE))
    ax.text(5.15, 3.0, "recycle", ha="center", fontsize=7, color=ORANGE, weight="bold")
    box(ax, (9.75, 1.65), 1.1, 0.75, "Gasoline", "1 kg demand", TEAL)
    arrow(ax, (8.5, 2.02), (9.75, 2.02), TEAL)
    ax.text(5.4, 0.25, "Direct elementary flows cross the boundary; licensed background inventories are excluded from the public package.", ha="center", fontsize=7.5, color=TEXT)
    finish(fig, output, "figure4_system_boundary")


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--output-dir", type=Path, required=True); args = parser.parse_args()
    plt.rcParams.update({"font.family": "Arial", "font.size": 8, "figure.facecolor": "white"})
    method_chain(args.output_dir); quantity_semantics(args.output_dir); pts_operator(args.output_dir); system_boundary(args.output_dir)


if __name__ == "__main__": main()
