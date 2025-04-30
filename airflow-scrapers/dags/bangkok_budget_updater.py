import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import requests

import logging
import pendulum

from urllib.parse import urlencode
from airflow.decorators import dag, task
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker
from include.api_utils import get_bma_api_auth
from models.models import BangkokBudget
from utils.db_utils import setup_engine_and_metadata
engine,_=setup_engine_and_metadata()
Session=sessionmaker(bind=engine)

def pad(val,width):
    s=str(val) if val not in (None, "") else ""
    return s.zfill(width)

def make_misid(item):
    parts = [
        pad(item.get("FISCAL_YEAR"),      2),
        pad(item.get("DEPARTMENT_ID"),    8),
        pad(item.get("FUNC_ID"),          6),
        pad(item.get("FUNC_YEAR"),        2),
        pad(item.get("FUNC_SEQ"),         2),
        pad(item.get("EXP_OBJECT_ID"),    2),
        pad(item.get("EXP_SUBOBJECT_ID"), 2),
        pad(item.get("EXPENDITURE_ID"),   2),
        pad(item.get("ITEM_ID"),          3),
    ]
    return "".join(parts)

def transform_item(item):

    return {
          "department_name": item.get("DEPARTMENT_NAME", ""),
    "sector_name": item.get("SECTOR_NAME", ""),
    "program_name": item.get("PROGRAM_NAME", ""),
    "func_name": item.get("FUNC_NAME", ""),
    "expenditure_name": item.get("EXPENDITURE_NAME", ""),
    "subobject_name": item.get("SUBOBJECT_NAME", ""),
    "func_id": item.get("FUNC_ID", ""),
    "func_year": item.get("FUNC_YEAR", ""),
    "func_seq": item.get("FUNC_SEQ", ""),
    "exp_object_id": item.get("EXP_OBJECT_ID", ""),
    "exp_subobject_id": item.get("EXP_SUBOBJECT_ID", ""),
    "expenditure_id": item.get("EXPENDITURE_ID", ""),
    "item_id": item.get("ITEM_ID", ""),
    "detail": item.get("DETAIL", ""),
    "approve_on_hand": item.get("APPROVE_ON_HAND", ""),
    "allot_approve": item.get("ALLOT_APPROVE", ""),
    "allot_date": item.get("ALLOT_DATE", ""),
    "agr_date": item.get("AGR_DATE", ""),
    "open_date": item.get("OPEN_DATE", ""),
    "acc_date": item.get("ACC_DATE", ""),
    "acc_amt": item.get("ACC_AMT", ""),
    "sgn_date": item.get("SGN_DATE", ""),
    "st_sgn_date": item.get("ST_SGN_DATE", ""),
    "end_sgn_date": item.get("END_SGN_DATE", ""),
    "last_rcv_date": item.get("LAST_RCV_DATE", ""),
    "vendor_type_id": item.get("VENDOR_TYPE_ID", ""),
    "vendor_no": item.get("VENDOR_NO", ""),
    "vendor_description": item.get("VENDOR_DESCRIPTION", ""),
    "pay_total_amt": item.get("PAY_TOTAL_AMT", ""),
    "fin_dept_amt": item.get("FIN_DEPT_AMT", ""),
    "net_amt": float(item.get("NET_AMT", item.get("net_amt", 0)) or 0),
    "pur_hire_status": item.get("PUR_HIRE_STATUS", ""),
    "pur_hire_status_name": item.get("PUR_HIRE_STATUS_NAME", ""),
    "contract_id": item.get("CONTRACT_ID", ""),
    "purchasing_department": item.get("PURCHASING_DEPARTMENT", ""),
    "contract_name": item.get("CONTRACT_NAME", ""),
    "contract_amount": item.get("CONTRACT_AMOUNT", ""),
    "pur_hire_method": item.get("PUR_HIRE_METHOD", ""),
    "egp_project_code": item.get("EGP_PROJECT_CODE", ""),
    "egp_po_control_code": item.get("EGP_PO_CONTROL_CODE", ""),
    "updated_at": pendulum.now(tz="local"),
    }
  
@dag(
    dag_id="bangkok_budget_update",
    schedule="0 0 2 * *",
    start_date=pendulum.datetime(2025,4,28, tz="Asia/Bangkok"),
    tags=["api", "budget", "update"]
)

def update_and_store_budget():
    @task()
    def updater():
        session=Session()

        existing_budget_mis_ids=session.execute(select(BangkokBudget.mis_id)).scalars().all()
        unique_mis_id_set=set(existing_budget_mis_ids)

        logging.info(f"Found {len(unique_mis_id_set)} MISIDS in DB")
        bangkok_now=pendulum.now(tz="Asia/Bangkok")
        buddhist_year=bangkok_now.year+543
        current_suffix=buddhist_year%100 #2568 --> 68
        fiscal_years=[]
        for year in range(67, current_suffix+1):
            fiscal_years.append(str(year).zfill(2))
        logging.info(f"Will update fiscal years: {fiscal_years}")
        base_url=os.getenv("BMA_MIS_API_URL")
        default_params={
            "source_id":     "01",
            "book_id":       "0",
            "department_id": "11000000",
        }
        auth=get_bma_api_auth()
        for fy in fiscal_years:
            for exp_id in ["05", "07"]:
                params={**default_params, "fiscal_year":fy, "exp_object_id":exp_id}
                qs=urlencode(params)
                api_url=f"{base_url}?{qs}"
                logging.info(f"Fethching for FY={fy}, exp_object_id={exp_id}")
                response=requests.get(url=api_url, auth=auth,timeout=120)
                response.raise_for_status()
                data=response.json()
                for item in data:
                    mis_id=make_misid(item)
                    if mis_id in unique_mis_id_set:
                        updated=transform_item(item)
                        # update wrt mis id
                        session.query(BangkokBudget).filter_by(mis_id=mis_id) \
                                .update(updated)
                # commit after one batch
                session.commit()
                logging.info(f"Committed updated for FY={fy}, exp={exp_id}")
        session.close()
    updater()

update_and_store_budget()
                                
                        
                
        
