"""
Academic profile matching database package.

Provides clean separation between:
- schema.py: Table definitions
- queries.py: SQL operations
- operations.py: High-level database operations
"""

from . import schema
from . import queries
from . import operations

__all__ = ['schema', 'queries', 'operations']