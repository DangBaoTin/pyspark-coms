
# 🧾 PySpark Mini Project: Customer Orders Management System (COMS)

## 🎯 Objective

Design and implement a PySpark-based data pipeline to manage, transform, and analyze customer order data. The pipeline should support ingestion, data cleaning, deduplication, enrichment, and analytics-ready output.

---

## 📁 Input Data

Raw input files will be stored in the `/raw/` directory as CSV files.

### 1. `customers.csv`
| Column Name   | Type     | Description             |
|---------------|----------|-------------------------|
| customer_id   | string   | Unique customer ID      |
| full_name     | string   | Customer full name      |
| email         | string   | Email address           |
| signup_date   | date     | Date of signup          |
| phone         | string   | Contact number          |
| region        | string   | Customer region         |

### 2. `orders.csv`
| Column Name   | Type     | Description             |
|---------------|----------|-------------------------|
| order_id      | string   | Unique order ID         |
| customer_id   | string   | Reference to customer   |
| order_date    | date     | Date of order           |
| status        | string   | Order status            |
| channel       | string   | Order channel           |
| total_amount  | double   | Total order amount      |
| currency      | string   | Currency code (e.g. USD)|

### 3. `order_items.csv`
| Column Name      | Type     | Description                     |
|------------------|----------|---------------------------------|
| order_item_id    | string   | Unique order item ID            |
| order_id         | string   | Reference to order              |
| product_id       | string   | Product identifier              |
| product_name     | string   | Name of the product             |
| category         | string   | Product category                |
| quantity         | integer  | Quantity ordered                |
| price_per_unit   | double   | Price of one unit               |
| discount         | double   | Discount applied                |

### 4. `payments.csv`
| Column Name     | Type     | Description                      |
|------------------|----------|----------------------------------|
| payment_id       | string   | Unique payment ID                |
| order_id         | string   | Reference to order               |
| payment_date     | date     | Date of payment                  |
| amount           | double   | Amount paid                      |
| payment_method   | string   | Method used for payment          |
| payment_status   | string   | Payment status                   |

---

## 🔨 Requirements

### Raw → Processed Zone
- Read and normalize CSVs into structured DataFrames.
- Convert all dates into consistent timestamp format.
- Deduplicate based on primary keys (e.g., order_id, order_item_id).
- Filter out invalid records:
  - Orders with total_amount <= 0
  - Payments with status = 'failed' or 'cancelled'

### Processed → Curated Zone
Generate the following curated datasets:

#### `customer_orders_summary`
- Total number of orders per customer
- Total amount spent
- Average order value
- First and last order dates
- Customer active status (last order within 90 days)

#### `order_facts`
- Join orders, items, payments
- Compute net revenue = (quantity × price - discount)
- Enrich with customer and region info

#### `daily_sales_aggregates`
- Group by order_date, region, and channel
- Metrics:
  - Total sales
  - Order count
  - Unique customers
  - Most used payment method

---

## 💎 Advanced Features (Optional)

- Use window functions to rank:
  - Top 3 customers by revenue in each region
  - First-time buyers this week
- Add alert for delayed payments (>2 days after order)
- Apply SCD Type 2 tracking on customer dimension

---

## 🗃 Output Format

All outputs stored in `/curated/` as Parquet format:

```
/curated/customer_orders_summary/
/curated/order_facts/order_date=YYYY-MM-DD/
/curated/daily_sales_aggregates/order_date=YYYY-MM-DD/
```

---

## ✅ Validation & Testing

- Schema validation
- No nulls in primary keys
- Amounts and dates must be valid
- Unit tests with PyTest or Unittest
