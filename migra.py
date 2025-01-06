import sys
import os
from tabulate import tabulate
from termcolor import colored

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'utils')))

from utils.functions_pgsql import get_short_tables, create_pgsql_tables

# Example usage
if __name__ == "__main__":
    short_tables = get_short_tables()
    create_pgsql_tables(short_tables,'rj')