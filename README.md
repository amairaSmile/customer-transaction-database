# Golden Customer Record

## Approach 
observation: 
    * The two files don't share a customer id, so the main challenge is figuring out which records are actually the same person.
    * The data is inconsistent the same column is stored in different formats(i.e registration_date,last_updated),
        and phone numbers are sometimes +33 or just number 0033 ,email missing in some cases .CRM file has redundant records i.e exact same rows for CRM00006
    * The same person sometimes appears under different CRM ids,so needs to 
    * When someone is in both files, the city or country sometimes doesn't match
    * CRM has 8 exact duplicate rows
the pipeline: clean -> match -> reconcile -> write
The pipeline has four stages. Each is a function in `processing.py`.
1. Clean both sources into one common shape
2.  Combine and resolve identity
3. Reconcile to one row per person
4. Write the golden record
## Quick start

Prerequisites 
# 1. Install (Python 3.10+ required for Spark)
pip install -r requirements.txt
Java :17 or later required by Scala
# 2. How to Run
cd src
python processing.py

# 3. Run the test suite
pytest
python -m pytest tests/test_processing.py -v

