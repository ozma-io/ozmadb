import textwrap


def strip_text(prompt: str) -> str:
    return textwrap.dedent(prompt).strip()
