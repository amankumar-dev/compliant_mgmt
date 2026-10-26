from pathlib import Path
import time
import boto3
import pandas as pd
from openpyxl.styles import Font,PatternFill,Alignment
from openpyxl.utils import get_column_letter

# AWS Config
DATABASE='compliance_analytics'
REGION='us-east-1'
WORKGROUP='primary'
KPI_TABLE='gold_kpi_summary'
CONTROL_TABLE='gold_summary'

# Output
OUTPUT_FILE=Path('reports/compliance_gold_report.xlsx')

athena=boto3.client('athena',region_name=REGION)

def run_query(sql):
    response=athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={'Database':DATABASE},
        WorkGroup=WORKGROUP
    )
    
    query_id=response['QueryExecutionId']
    
    while True:
        result=athena.get_query_execution(
            QueryExecutionId=query_id
        )
        
        status=result['QueryExecution']['Status']['State']
        
        if status=='SUCCEEDED':
            break
        
        if status in ('FAILED','CANCELLED'):
            reason=result['QueryExecution']['Status'].get('StateChangeReason','Unknown error')
            raise RuntimeError(f'Athena Query: {status}:{reason}')
        
        time.sleep(2)
        
    rows=[]
    next_token=None
    
    while True:
        args={'QueryExecutionId':query_id}
        
        if next_token:
            args['NextToken']=next_token
            
        page=athena.get_query_results(**args)
        rows.extend(page['ResultSet']['Rows'])
        
        next_token=page.get('NextToken')
        
        if not next_token:
            break
        
    if not rows:
        return pd.DataFrame()

    headers = [
        cell.get("VarCharValue", "")
        for cell in rows[0]["Data"]
    ]

    data = [
        [cell.get("VarCharValue") for cell in row["Data"]]
        for row in rows[1:]
    ]

    return pd.DataFrame(data, columns=headers)


def format_sheet(worksheet):
    worksheet.freeze_panes='A2'
    worksheet.auto_filter.ref=worksheet.dimensions
    
    for cell in worksheet[1]:
        cell.font=Font(bold=True, color='FFFFFF')
        cell.fill=PatternFill(
            fill_type='solid',
            fgColor='1F4E78'
        )
        cell.alignment=Alignment(horizontal='center')
        
    for column_cells in worksheet.columns:
        max_length=max(
            len(str(cell.value or ""))
            for cell in column_cells
        )
        
        column_letter=get_column_letter(
            column_cells[0].column
        )
        
        worksheet.column_dimensions[column_letter].width=min(
            max_length+3,35
        )
        
def gold_report():
    print("Reading Gold tables from Athena...")
    
    kpi_df=run_query(
        f"""
        SELECT *
        FROM {KPI_TABLE}
        ORDER BY kpi_status
        """
    )
    
    control_df=run_query(
        f"""
        SELECT *
        FROM {CONTROL_TABLE}
        ORDER BY control_name, control_status
        """
    )
    
    if kpi_df.empty or control_df.empty:
        raise ValueError("One or both Gold tables returned no records.")
    
    # Convert numeric columns
    kpi_df["total_device"] = pd.to_numeric(
        kpi_df["total_device"], errors="raise"
    )
    kpi_df["percentage"] = pd.to_numeric(
        kpi_df["percentage"], errors="raise"
    )

    control_df["total_devices"] = pd.to_numeric(
        control_df["total_devices"], errors="raise"
    )
    control_df["percentage"] = pd.to_numeric(
        control_df["percentage"], errors="raise"
    )
    
    # Create output directory
    OUTPUT_FILE.parent.mkdir(parents=True,exist_ok=True)
    
    # Export to Excel
    with pd.ExcelWriter(OUTPUT_FILE,engine='openpyxl') as writer:
        kpi_df.to_excel(
            writer,
            sheet_name='KPI Summary',
            index=False
        )
        
        control_df.to_excel(
            writer,
            sheet_name='Control Summary',
            index=False
        )
        
        for worksheet in writer.book.worksheets:
            format_sheet(worksheet)
            
            # Format percentage columns
            for cell in worksheet[1]:
                if cell.value=='percentage':
                    for row in range(2,worksheet.max_row+1):
                        worksheet.cell(
                            row=row,
                            column=cell.column
                        ).number_format='0.00"%"'
                        
    print("\nEXCEL REPORT CREATED SUCCESSFULLY")
    print("KPI rows:", len(kpi_df))
    print("Control summary rows:", len(control_df))
    print("Output:", OUTPUT_FILE.resolve())
    
gold_report()