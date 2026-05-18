# Golden Customer Record

## Approach 
observation: 
    * The two files don't share a customer id, so the main challenge is figuring out which records are actually the same person.
    * The data is inconsistent the same column is stored in different formats(i.e registration_date,last_updated),
        and phone numbers are sometimes +33 or just number 0033 ,email missing in some cases .CRM file has redundant records i.e exact same rows for CRM00006
    * The same person sometimes appears under different CRM ids
    * When someone is in both files, the city or country sometimes doesn't match

## Quick start

```bash
# 1. Install (Python 3.10+ required for Spark)
pip install -r requirements.txt

# 2. Run the test suite
pytest


