import sys
import os
from tabulate import tabulate
from termcolor import colored

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'utils')))

from utils.functions import get_short_tables, get_table_columns, generate_mysql_table_ddl

# Example usage
if __name__ == "__main__":
    short_tables = get_short_tables()
    print(colored("Short tables:", 'yellow'))
    print(tabulate([[table] for table in short_tables], headers=['Table Name'], tablefmt='grid'))
    
    for table in short_tables:
        columns = get_table_columns(table)
        print(colored(f"Table: {table}", 'cyan'))
        print(tabulate(columns.items(), headers=['Column Name', 'Details'], tablefmt='grid'))
        
        generate_mysql_table_ddl(table)