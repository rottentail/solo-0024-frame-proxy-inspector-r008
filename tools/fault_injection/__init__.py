"""Test-only fault injection support for persistence fault verification.

Nothing in this package is imported by the production service.  The only way a
service process picks it up is when the verification driver starts the real
``python -m frame_proxy serve`` subprocess with this directory on
``PYTHONPATH`` so that :mod:`sitecustomize` is auto-imported at interpreter
startup.
"""

from __future__ import annotations
