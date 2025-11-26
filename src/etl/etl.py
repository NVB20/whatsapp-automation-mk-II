from src.etl.extract import run_multi_group_reader
from src.etl.sales_etl.sales_etl import run_sales_etl
from src.etl.students_etl.students_etl import run_students_etl

def run_etl():
    # Extract both groups
    extract_result = run_multi_group_reader()
    students_messages = extract_result["students"]
    sales_messages = extract_result["sales"]
    
    if len(students_messages) == 0:
        print("=" * 60 + "\n" + "failed to read students messages" + "\n" + "=" * 60)
        return
    else:
        run_students_etl(students_messages)

    if len(sales_messages) == 0:
        print("=" * 60 +"\n" + "failed to read sales messages" + "=" * 60 )
        return
    else:
        run_sales_etl(sales_messages)
