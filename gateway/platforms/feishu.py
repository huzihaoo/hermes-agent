"""Compatibility alias for the plugin-based Feishu adapter.

Hermes 0.18.2 moved platform implementations under ``plugins.platforms``.
The PNC overlay still imports the historical gateway path, so expose the
plugin module itself instead of maintaining a second adapter implementation.
"""

import sys

from plugins.platforms.feishu import adapter as _adapter

sys.modules[__name__] = _adapter
