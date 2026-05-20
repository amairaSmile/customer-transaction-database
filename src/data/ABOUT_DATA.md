# Synthetic Customer Data

This folder contains synthetic customer data from two source systems.

## Files

### 1. `crm_customers.csv`
**Source**: Primary CRM system

**Columns**:
- `customer_id` - Unique CRM identifier
- `first_name` - Customer first name
- `last_name` - Customer last name
- `email` - Email address
- `phone` - Phone number
- `address` - Street address
- `city` - City
- `country` - Country
- `registration_date` - Customer registration date
- `last_updated` - Last update date

**Sample Records**:
```
CRM00001,Matthew,Brown,matthew.brown@yahoo.com,,410 Oak Ave,Dallas,Canada,2018-12-23,2024-08-27
CRM00002,Charles,Lee,charles.lee@email.com,+339187587873,435 Oak Ave,Melbourne,France,20191016,20241215
```

---

### 2. `transaction_customers.csv`
**Source**: E-commerce transaction system

**Columns**:
- `transaction_id` - Transaction identifier
- `customer_email` - Customer email
- `first_name` - Customer first name
- `last_name` - Customer last name
- `phone` - Contact phone
- `shipping_address` - Shipping street address
- `city` - Shipping city
- `country` - Country
- `purchase_date` - Purchase date

**Sample Records**:
```
TXN000004,charles.smith@gmail.com,Charles,Smith,,9814 Park Blvd,Austin,USA,2022-10-30
TXN000009,john.williams@yahoo.com,John,Williams,+339569019624,4821 Park Blvd,San Diego,France,2020-07-10
```

---

## Notes

- Examine both datasets carefully to understand their structure and relationships
- Consider how customers can be identified across both systems
- Think about how to handle data quality issues you discover
- Design an appropriate approach for creating a unified customer view
