"""段落编号、实体线索分组和有限分块。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Paragraph:
    id: int
    text: str
    page: int | None = None

    def to_dict(self):
        return {"id": self.id, "text": self.text, "page": self.page}


def read_document(path: Path) -> list[Paragraph]:
    if path.suffix.lower() == ".pdf":
        try:
            from pypdf import PdfReader
        except ImportError as exc:
            raise ValueError("PDF 文本解析需要安装 pdf 可选依赖；不支持扫描件 OCR") from exc
        pages = [(i + 1, page.extract_text() or "") for i, page in enumerate(PdfReader(path).pages)]
        if not any(text.strip() for _, text in pages):
            raise ValueError("PDF 无可提取文本，需要独立 OCR")
    else:
        pages = [(None, path.read_text(encoding="utf-8-sig"))]
    paragraphs = []
    for page, text in pages:
        for block in re.split(r"\n\s*\n", text):
            block = block.strip()
            if block:
                paragraphs.append(Paragraph(len(paragraphs), block, page))
    if not paragraphs:
        raise ValueError("文档没有有效文本")
    return paragraphs


def identity_candidates(paragraph: Paragraph, labels: list[str]) -> list[str]:
    # 只接受标签后的显式编号；标签后必须有分隔符，避免 ID 匹配 Identifier。
    label_pattern = "|".join(
        re.escape(label)
        for label in sorted(labels, key=len, reverse=True)
        if label and len(label) <= 80
    )
    if not label_pattern:
        return []
    anchor = re.compile(
        r"(?<!\w)(?:" + label_pattern + r")(?:\s*[:#：-]\s*|\s+)([\w-]*\d[\w-]*)", re.IGNORECASE
    )
    return list(dict.fromkeys(anchor.findall(paragraph.text)))


def chunks(paragraphs: list[Paragraph], labels: list[str], max_chars: int) -> list[list[Paragraph]]:
    groups: dict[str, list[Paragraph]] = {}
    for paragraph in paragraphs:
        identities = identity_candidates(paragraph, labels)
        # 多实体段落不强制归给其中一个实体；仍进入抽取，来源不丢失。
        key = identities[0] if len(identities) == 1 else f"paragraph:{paragraph.id}"
        groups.setdefault(key, []).append(paragraph)
    output, current, size = [], [], 0
    for group in groups.values():
        group_size = sum(len(p.text) for p in group)
        if current and group_size <= max_chars and size + group_size > max_chars:
            output.append(current)
            current, size = [], 0
        for paragraph in group:
            if len(paragraph.text) > max_chars:
                raise ValueError(f"段落 {paragraph.id} 超过块预算，请提高 etl.chunk_chars")
            if current and size + len(paragraph.text) > max_chars:
                output.append(current)
                current, size = [], 0
            current.append(paragraph)
            size += len(paragraph.text)
        # 不主动切断能放进单块的下一实体；大实体必要时分块，由主键合并。
    if current:
        output.append(current)
    return output


def schema_sample(paragraphs: list[Paragraph], max_chars: int = 14000) -> str:
    # 跨全文等距取样，避免仅看开篇而遗漏后面章节的字段。
    selected, used = [], 0
    count = min(24, len(paragraphs))
    indices = sorted({round(i * (len(paragraphs) - 1) / max(count - 1, 1)) for i in range(count)})
    for index in indices:
        text = f"[段落 {index}] {paragraphs[index].text}\n"
        if used + len(text) <= max_chars:
            selected.append(text)
            used += len(text)
    return "\n".join(selected)
