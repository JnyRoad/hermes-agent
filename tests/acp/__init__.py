"""ACP tests require the optional ``acp`` package.

缺少依赖时统一跳过整个 tests/acp 目录，避免在收集阶段直接报错。
"""

import pytest

pytest.importorskip("acp")
