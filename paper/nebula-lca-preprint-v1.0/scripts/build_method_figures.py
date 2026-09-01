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


def box(ax, xy, width, height, title, body="", color=BLUE, fill="#F5F8FC"):
    patch = FancyBboxPatch(
        xy, width, height, boxstyle="round,pad=0.02,rounding_size=0.025",
        linewidth=1.0, edgecolor=color, facecolor=fill,
    )
    ax.add_patch(patch)
    ax.text(xy[0] + width / 2, xy[1] + height * 0.64, title, ha="center", va="center", weight="bold", color=TEXT)
    if body:
        ax.text(xy[0] + width / 2, xy[1] + height * 0.30, body, ha="center", va="center", color=TEXT, fontsize=7)
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
    fig, ax = plt.subplots(figsize=(7.1, 2.5))
    ax.set(xlim=(0, 10), ylim=(0, 3)); ax.axis("off")
    labels = [
        ("Total-process facts", "inputs, outputs, emissions"),
        ("Attribution receipt", "classes, weights, units"),
        ("Product columns", "derived identities"),
        ("System solve", r"$T\mathbf{x}=\mathbf{f}$"),
        ("PTS compilation", r"$M X=D$; boundary projection"),
    ]
    xs = [0.1, 2.15, 4.2, 6.25, 8.3]
    for index, ((title, body), x) in enumerate(zip(labels, xs, strict=True)):
        box(ax, (x, 1.35), 1.55, 0.85, title, body, color=BLUE if index < 3 else TEAL)
        if index:
            arrow(ax, (x - 0.45, 1.78), (x, 1.78), color=BLUE)
    ax.plot([0.35, 9.65], [0.65, 0.65], color=ORANGE, lw=1.2)
    ax.text(5, 0.35, "Continuous lineage: source revision -> transformation receipt -> solve/compile receipt", ha="center", color=TEXT, fontsize=7.5)
    finish(fig, output, "figure1_method_chain")


def quantity_semantics(output: Path):
    fig, axes = plt.subplots(1, 2, figsize=(7.1, 2.6))
    for ax in axes: ax.set(xlim=(0, 5), ylim=(0, 3)); ax.axis("off")
    box(axes[0], (0.25, 1.2), 1.45, 0.8, "Supplier record", r"$Q_s c_s$", BLUE)
    box(axes[0], (3.3, 1.2), 1.45, 0.8, "Consumer record", r"$Q_d c_d$", BLUE)
    arrow(axes[0], (1.7, 1.6), (3.3, 1.6), BLUE)
    axes[0].text(2.5, 2.45, "Source-state transfer audit", ha="center", weight="bold", color=TEXT)
    axes[0].text(2.5, 0.65, r"Comparable realized quantities: $r_e^{source}=Q_s c_s-Q_d c_d$", ha="center", fontsize=7.5)
    box(axes[1], (0.15, 1.65), 1.35, 0.65, "Provider column", "1 unit / activity", TEAL)
    box(axes[1], (0.15, 0.55), 1.35, 0.65, "Consumer column", "2 units / activity", TEAL)
    box(axes[1], (2.15, 1.05), 1.15, 0.75, "Solve", r"$T\mathbf{x}=\mathbf{f}$", ORANGE, "#FFF8ED")
    box(axes[1], (3.75, 1.05), 1.05, 0.75, "Scaled ledger", "closed transfer", TEAL)
    arrow(axes[1], (1.5, 1.98), (2.15, 1.53), TEAL); arrow(axes[1], (1.5, 0.88), (2.15, 1.32), TEAL); arrow(axes[1], (3.3, 1.43), (3.75, 1.43), TEAL)
    axes[1].text(2.5, 2.65, "Normalized computational closure", ha="center", weight="bold", color=TEXT)
    axes[1].text(2.5, 0.2, "Raw coefficients need not be equal; scaled exchanges close after solving.", ha="center", fontsize=7.5)
    finish(fig, output, "figure2_quantity_semantics")


def pts_operator(output: Path):
    fig, ax = plt.subplots(figsize=(7.1, 2.8)); ax.set(xlim=(0, 10), ylim=(0, 4)); ax.axis("off")
    boundary = Rectangle((0.35, 0.55), 5.5, 2.95, facecolor="#F8FAFC", edgecolor=BLUE, lw=1.2, linestyle="--")
    ax.add_patch(boundary); ax.text(0.55, 3.2, "PTS internal boundary", color=BLUE, weight="bold")
    box(ax, (0.8, 1.55), 1.55, 0.85, "Process 1", "internal activity", BLUE)
    box(ax, (3.7, 1.55), 1.55, 0.85, "Process 2", "internal activity", BLUE)
    arrow(ax, (2.35, 2.05), (3.7, 2.05), BLUE); arrow(ax, (3.7, 1.85), (2.35, 1.85), ORANGE)
    box(ax, (6.35, 2.35), 1.35, 0.75, "Linear solve", r"$X=M^{-1}D$", TEAL)
    box(ax, (6.35, 1.15), 1.35, 0.75, "Projection", r"$\bar G=GX$; $\bar B=BX$", TEAL)
    box(ax, (8.35, 1.65), 1.35, 0.95, "Compiled PTS", "inputs, products,\nelementary flows", ORANGE, "#FFF8ED")
    arrow(ax, (5.85, 2.25), (6.35, 2.7), TEAL); arrow(ax, (7.02, 2.35), (7.02, 1.9), TEAL); arrow(ax, (7.7, 1.53), (8.35, 2.05), TEAL)
    finish(fig, output, "figure3_pts_operator")


def system_boundary(output: Path):
    fig, ax = plt.subplots(figsize=(7.1, 3.1)); ax.set(xlim=(0, 11), ylim=(0, 4.2)); ax.axis("off")
    boundary = Rectangle((1.55, 0.65), 7.7, 2.9, facecolor="#F8FAFC", edgecolor=BLUE, lw=1.2, linestyle="--")
    ax.add_patch(boundary); ax.text(1.75, 3.3, "Foreground benchmark boundary", color=BLUE, weight="bold")
    titles = ["Distillation", "Hydrotreating", "Reforming", "Blending"]
    xs = [1.85, 3.65, 5.45, 7.25]
    for title, x in zip(titles, xs, strict=True): box(ax, (x, 1.65), 1.25, 0.75, title, "balanced process", BLUE)
    for x in xs[1:]: arrow(ax, (x - 0.55, 2.02), (x, 2.02), BLUE)
    box(ax, (0.05, 2.35), 1.1, 0.65, "Crude feed", "boundary input", TEAL)
    box(ax, (0.05, 1.15), 1.1, 0.65, "Electricity", "normalized supply", ORANGE, "#FFF8ED")
    arrow(ax, (1.15, 2.67), (1.85, 2.25), TEAL); arrow(ax, (1.15, 1.48), (1.85, 1.82), ORANGE)
    ax.plot([1.15, 8.2], [1.48, 1.48], color=ORANGE, lw=0.9, linestyle="--")
    for x in xs[1:]: arrow(ax, (x + 0.62, 1.48), (x + 0.62, 1.65), ORANGE)
    box(ax, (9.75, 1.65), 1.1, 0.75, "Gasoline", "1 kg demand", TEAL)
    arrow(ax, (8.5, 2.02), (9.75, 2.02), TEAL)
    ax.text(5.4, 0.25, "Direct elementary flows cross the boundary; licensed background inventories are excluded from the public package.", ha="center", fontsize=7.5, color=TEXT)
    finish(fig, output, "figure4_system_boundary")


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--output-dir", type=Path, required=True); args = parser.parse_args()
    plt.rcParams.update({"font.family": "Arial", "font.size": 8, "figure.facecolor": "white"})
    method_chain(args.output_dir); quantity_semantics(args.output_dir); pts_operator(args.output_dir); system_boundary(args.output_dir)


if __name__ == "__main__": main()
