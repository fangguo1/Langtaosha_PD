"""CRUD操作模块"""
from .create import create_paper
from .read import read_paper, search_papers
from .update import update_paper
from .delete import delete_paper

__all__ = ['create_paper', 'read_paper', 'search_papers', 'update_paper', 'delete_paper']

