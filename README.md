# ShopData ETL Pipeline — Customer Lifetime Value (CLV) Analytics

ETL pipeline สำหรับย้ายข้อมูล order management ระบบเก่าของ "ShopData Inc." จาก SQLite views ที่มีปัญหาคุณภาพข้อมูล เข้าสู่ analytics data warehouse ที่สะอาดพร้อมใช้งาน เพื่อให้ทีม BI สามารถคำนวณ Customer Lifetime Value (CLV) ได้

---

## Project Overview & Architecture

### สถาปัตยกรรมโดยรวม

```
shopdata.db (raw SQLite views)
    │
    │  vw_raw_customers, vw_raw_orders, vw_exchange_rates
    ▼
pipeline.py  (Prefect @flow: shopdata_etl_pipeline)
    │
    ├── extract_data()          → ดึงข้อมูลดิบทั้ง 3 views
    ├── transform_customers()   → เรียก clean_customers() จาก transform.py
    ├── transform_orders()      → เรียก clean_orders() จาก transform.py
    └── load_data()             → เขียนผลลัพธ์ลง analytics.db
    ▼
analytics.db (dim_customers, fct_orders)
    ▼
clv_report.sql  →  รายงาน Customer Lifetime Value
```

### หลักการออกแบบ
- **แยก pure logic ออกจาก orchestration**: `transform.py` เก็บเฉพาะฟังก์ชัน pandas ล้วนๆ (`clean_customers`, `clean_orders`) ไม่แตะ DB หรือ Prefect เลย เพื่อให้ทดสอบแบบ isolated ได้ง่ายใน `test_pipeline.py`
- **`pipeline.py`** ทำหน้าที่ orchestration เท่านั้น: extract → transform → load พร้อม logging และ error handling ในทุก `@task`
- **Dimensional model** แบบง่าย (star schema แบบ 1 fact + 1 dimension) เพื่อรองรับการคำนวณ CLV โดยตรง

### Schema ของ analytics.db

**`dim_customers`**
| Column | Type | หมายเหตุ |
|---|---|---|
| customer_id | INTEGER | Primary key, รวม dummy row `-1 = Unknown` |
| full_name | TEXT | |
| email | TEXT | ค่าที่หายไปแทนด้วย `unknown@domain.com` |
| phone | TEXT | Standardize เหลือเฉพาะตัวเลข, ค่าที่หายไปเป็น NULL จริง |
| signup_date | TIMESTAMP | ใช้คำนวณ `customer_cohort` |

**`fct_orders`**
| Column | Type | หมายเหตุ |
|---|---|---|
| order_id | INTEGER | |
| customer_id | INTEGER | Orphan FK ถูก remap เป็น `-1` |
| order_date | TIMESTAMP | อาจเป็น NULL ได้ |
| currency | TEXT | ค่าที่หายไปถือเป็น `USD` |
| total_amount | REAL | กรองเฉพาะ `> 0` |
| status | TEXT | เก็บไว้ตามต้นฉบับ (COMPLETED/CANCELLED/PENDING) เพื่อ auditing |
| usd_amount | REAL | แปลงเป็น USD แล้ว (LOCF/fallback ตามกฎที่ระบุใน Data Quality Findings) |

รายละเอียดเหตุผลของแต่ละ decision ดูได้ในหัวข้อ [Data Exploration & Quality Findings](#data-exploration--quality-findings) และ [Analytical Query & Customer Lifetime Value (CLV) Report](#analytical-query--customer-lifetime-value-clv-report) ด้านล่าง

---

## Environment Setup & Dependencies

### Prerequisites
- Python 3.12+
- Git

### ขั้นตอนติดตั้ง

1. Clone repository
   ```bash
   git clone https://github.com/nonthaphat-ratt/Nonthaphat_DE_TechAssignment.git
   cd Nonthaphat_DE_TechAssignment
   ```

2. สร้างและเปิดใช้งาน virtual environment
   ```bash
   python3.12 -m venv venv

   # macOS / Linux
   source venv/bin/activate

   # Windows
   venv\Scripts\activate
   ```

3. ติดตั้ง dependencies
   ```bash
   pip install -r requirements.txt
   ```

   Dependencies หลักที่ pin เวอร์ชันไว้ (`requirements.txt`):
   | Package | Version |
   |---|---|
   | pandas | 2.3.3 |
   | prefect | 3.5.0 |
   | fastapi | 0.115.14 |
   | pytest | 8.4.2 |

4. วางไฟล์ `shopdata.db` (ได้รับจาก HR) ไว้ที่ root ของ project ให้อยู่ระดับเดียวกับ `pipeline.py`

---

## Data Exploration & Quality Findings

จากการรัน query สำรวจข้อมูลใน `exploration.sql` กับ `vw_raw_customers`, `vw_raw_orders` และ `vw_exchange_rates` พบความผิดปกติ **7 กลุ่ม** สรุปได้ดังนี้

### Executive Summary

| ลำดับ (No.) | มุมมองข้อมูล (Dimension / Category) | ตารางและฟิลด์ที่พบปัญหา (Target) | ปัญหาที่ตรวจพบ (Anomaly Summary) | แนวทางจัดการใน Pipeline (Actionable Fix) |
|---|---|---|---|---|
| 1 | Uniqueness | `vw_raw_customers.customer_id` | พบ `customer_id` ซ้ำ (record เดียวกันถูกบันทึกมากกว่า 1 ครั้ง ด้วยข้อมูลคนละ version) | Deduplicate โดยเก็บ record ที่ `signup_date` ล่าสุดไว้เป็น record จริง |
| 2 | Completeness | `vw_raw_customers.email/phone`, `vw_raw_orders.order_date` | พบค่า `NULL` ในฟิลด์สำคัญ ทั้งฝั่งลูกค้า (email, phone) และฝั่ง order (order_date) | แทน email ว่างด้วย `unknown@domain.com` ส่วน phone ปล่อยว่างไว้เหมือนเดิม และออกแบบ logic ให้ pipeline ไม่ล้มเมื่อ `order_date` เป็น NULL |
| 3 | Data Consistency | `vw_raw_customers.phone` | รูปแบบเบอร์โทรไม่เป็นมาตรฐานเดียวกัน (มีวงเล็บ, ขีด, ตัวอักษรปน, รหัสประเทศ) | Standardize ด้วย regex ให้เหลือเฉพาะตัวเลขล้วนตามกฎที่โจทย์กำหนด |
| 4 | Validity | `vw_raw_orders.total_amount` | พบ `total_amount` ≤ 0 ซึ่งเป็น system error ไม่ใช่ transaction จริง | Filter ออกจาก `fct_orders` ทั้งหมด (ไม่นำเข้าฝั่ง analytics) |
| 5 | Referential Integrity | `vw_raw_orders.customer_id` | มี order อ้างถึง `customer_id` ที่ไม่มีอยู่จริงในตาราง customers (**Orphan Foreign Key**) | ใช้ **Unknown Dimension Pattern**: เพิ่ม dummy record เข้า `dim_customers` โดยตรง (`customer_id = -1, full_name = 'Unknown'`) แล้ว remap FK ของ order ที่ orphan ให้ชี้ไปที่ record นี้ใน `fct_orders` |
| 6 | Validity & Integrity | `vw_raw_orders` × `vw_exchange_rates` | สกุลเงินต่างประเทศบางรายการหา exchange rate ตรงวันที่ (`order_date`) ไม่เจอ เพราะช่วงวันที่ของ `vw_exchange_rates` สั้นกว่าช่วงวันที่ของ orders | ใช้ **LOCF (Last Observation Carried Forward)**: ใช้ rate ล่าสุดที่มีอยู่ก่อนหน้าวันที่ order แทนการปล่อยให้เป็น `NULL` |
| 7 | Completeness | `vw_raw_orders.currency` | พบ `currency` เป็น `NULL` โดยตรง (order_id 107, 116) คนละกรณีกับ order_date ที่เป็น NULL | ถือเป็น `USD` (rate = 1.0) ตามที่โจทย์ระบุไว้ชัดเจนว่า "if a currency is missing... assume it is already 'USD'" |

---

### Detailed Breakdown

#### 1. Uniqueness — ข้อมูลลูกค้าซ้ำซ้อน
- **ปัญหาที่พบ**: มี `customer_id` เดียวกันปรากฏมากกว่า 1 record โดยแต่ละ record มีค่า `signup_date` และรายละเอียดการติดต่อ (email/phone) ต่างกัน ซึ่งเข้าใจได้ว่าเป็นการอัปเดตข้อมูลลูกค้าคนละช่วงเวลา
- **ความเสี่ยง/ผลกระทบ**: ถ้าไม่ deduplicate จะทำให้ `dim_customers` มี **Primary Key ซ้ำ** ส่งผลให้การ join กับ `fct_orders` นับจำนวนลูกค้าและคำนวณ CLV ผิดเพี้ยน (นับซ้ำ)
- **แนวทางแก้ไขในขั้นถัดไป**: Pipeline จะ group ตาม `customer_id` แล้วเลือกเฉพาะ record ที่มี `signup_date` ล่าสุดไว้เป็นตัวแทนก่อนโหลดเข้า `dim_customers`

#### 2. Completeness — ค่าที่ขาดหายไป
- **ปัญหาที่พบ**: `email`/`phone` เป็น `NULL` ในบางแถวของ customers และ `order_date` เป็น `NULL` ในบางแถวของ orders (เช่น order_id 117 ซึ่งมี `currency = USD`)
- **ความเสี่ยง/ผลกระทบ**: ค่า `NULL` ในฟิลด์ติดต่อทำให้ทีมอื่นใช้งานต่อยาก ส่วน `order_date` เป็น `NULL` กระทบโดยตรงต่อการ join หา exchange rate และการจัดกลุ่มข้อมูลตามช่วงเวลา
- **แนวทางแก้ไขในขั้นถัดไป**:
  - `email` ที่หายไป → แทนที่ด้วย `"unknown@domain.com"`
  - `phone` ที่หายไป → ปล่อยเป็น `NULL` หลัง standardize (ไม่ประดิษฐ์ค่าเบอร์โทรขึ้นมาเอง)
  - `order_date` เป็น `NULL` → **ไม่ตัดออก** จาก pipeline โดยใช้กติกา: ถ้า `currency = USD` ใช้ `total_amount` ตรงๆ ได้เลย (ไม่ต้องหา rate); ถ้า `currency ≠ USD` ให้ fallback เป็น USD เหมือนกฎ "หา exchange rate ไม่เจอ" (พร้อม log แจ้งเตือนไว้ debug) — และไม่กระทบ `customer_cohort` เพราะ cohort คำนวณจาก `signup_date` ของลูกค้า ไม่ใช่จาก `order_date`

#### 3. Data Consistency — รูปแบบเบอร์โทรไม่เป็นมาตรฐาน
- **ปัญหาที่พบ**: เบอร์โทรมีหลายรูปแบบปนกัน เช่น มีวงเล็บ/ขีดคั่น, มีรหัสประเทศนำหน้า, และบางค่าเป็นข้อความที่ไม่ใช่เบอร์จริง (มีตัวอักษรปนอยู่)
- **ความเสี่ยง/ผลกระทบ**: ทำให้ไม่สามารถใช้เบอร์โทรเป็น key เชื่อมกับระบบอื่น (เช่น CRM, marketing) ได้โดยตรง และเสี่ยงต่อการ export ข้อมูลผิดรูปแบบ
- **แนวทางแก้ไขในขั้นถัดไป**: เขียนฟังก์ชัน standardize เพื่อดึงเฉพาะตัวเลขออกมา ทดสอบแยกจาก DB ด้วย unit test ตามที่โจทย์กำหนดใน Part 3

#### 4. Validity & Integrity — ยอดเงิน, ลูกค้าที่ไม่มีอยู่จริง, และ exchange rate ไม่ครบ
- **ปัญหาที่พบ**:
  - **total_amount ≤ 0**: บาง order มียอดติดลบหรือเป็นศูนย์ ซึ่งเป็น system error
  - **Orphan customer_id**: order_id 106 และ 118 อ้างถึง `customer_id = 99` ที่ไม่มีอยู่จริงในตาราง customers
  - **Exchange rate date gap**: `vw_exchange_rates` มีข้อมูลถึงแค่ 2023-05-05 แต่ `vw_raw_orders` มีวันที่ยาวไปถึง 2023-05-14 ทำให้ order สกุลเงินต่างประเทศหลังวันที่ 05-05 หา rate ตรงวันไม่เจอ
- **ความเสี่ยง/ผลกระทบ**:
  - ยอดติดลบ/ศูนย์ทำให้ `lifetime_value_usd` คำนวณผิด
  - Orphan customer_id ถ้าตัดทิ้งจะทำให้ **รายได้รวม (Revenue) ถูกรายงานต่ำกว่าความจริง**
  - Exchange rate ไม่ครบวันจะทำให้ `usd_amount` เป็น `NULL` หากใช้ exact-date join ตรงๆ
- **แนวทางแก้ไขในขั้นถัดไป**:
  - **Filter order** ที่ `total_amount ≤ 0` ออกจาก `fct_orders` ทั้งหมด
  - **Unknown Dimension Pattern**: เพิ่ม dummy record (customer_id = -1, full_name = 'Unknown') เข้าไปใน dim_customers โดยตรง แล้ว remap customer_id ของ order ที่ orphan (106, 118) ใน fct_orders ให้ชี้ไปที่ -1 แทนการตัดทิ้งหรือปล่อยเป็น NULL 
  - **LOCF (Last Observation Carried Forward)**: เมื่อหา exchange rate ตรงวันที่ไม่เจอ ให้ใช้ rate ล่าสุดที่มีอยู่ก่อนหน้าวันที่ order นั้น (As-Of Join) แทนการปล่อยให้เป็น `NULL`

#### 5. Completeness — currency ที่ขาดหายไปในระดับ order
- **ปัญหาที่พบ**: `order_id 107` และ `116` มีค่า `currency` เป็น `NULL` โดยตรง (คนละกรณีกับ `order_date` ที่เป็น NULL ในหัวข้อที่ 2 — ที่นี่ตัว currency เองหายไปเลย)
- **ความเสี่ยง/ผลกระทบ**: ถ้าไม่จัดการ currency ที่หายไปให้ชัดเจน การ join หา exchange rate จะไม่พบ rate ที่ตรงกัน (เพราะไม่รู้ว่าจะ join ด้วย currency อะไร) ทำให้ `usd_amount` กลายเป็น `NULL` และ `lifetime_value_usd` ในรายงาน CLV คำนวณขาดไป
- **แนวทางแก้ไขในขั้นถัดไป**: ถือว่า currency ที่เป็น `NULL` คือ `USD` (rate = 1.0) ตรงตามข้อความในโจทย์ที่ระบุไว้ว่า *"If a currency is missing or doesn't have an exchange rate, assume it is already 'USD'"* — ใช้ logic เดียวกับกรณี "หา exchange rate ไม่เจอ" ในหัวข้อที่ 4

---

## ETL Pipeline Execution

รัน Prefect flow เพื่อทำ Extract → Transform → Load จาก `shopdata.db` ไปยัง `analytics.db`:

```bash
python pipeline.py
```

**สิ่งที่เกิดขึ้นเมื่อรันคำสั่งนี้:**
1. Prefect จะสร้าง temporary local server ขึ้นมาเองอัตโนมัติ (ไม่ต้องตั้งค่า Prefect server ล่วงหน้า)
2. Flow `shopdata_etl_pipeline` จะรันตามลำดับ: `extract_data` → `transform_customers` → `transform_orders` → `load_data`
3. Log ของแต่ละ `@task` (จำนวนแถวที่ extract/transform/load) จะแสดงผลทาง console
4. ไฟล์ `analytics.db` จะถูกสร้างใหม่ (หรือ replace ตารางเดิม) ที่ root ของ project พร้อมตาราง `dim_customers` และ `fct_orders`

**ตรวจสอบผลลัพธ์เบื้องต้น** (optional):
```bash
sqlite3 analytics.db "SELECT COUNT(*) FROM dim_customers;"
sqlite3 analytics.db "SELECT COUNT(*) FROM fct_orders;"
```
ผลลัพธ์ที่คาดหวัง: `dim_customers` 11 แถว (10 ลูกค้าจริงหลัง dedupe + 1 แถว Unknown), `fct_orders` 17 แถว (20 orders ดิบ ลบ 3 แถวที่ `total_amount ≤ 0`)

---

## Unit Test Execution

รัน unit test ทั้งหมดที่ทดสอบ `transform.py` (ไม่แตะ database จริง ใช้ mock DataFrame ล้วน):

```bash
pytest test_pipeline.py -v
```

Test suite ครอบคลุม logic การทำความสะอาดข้อมูลหลักทั้งหมด ได้แก่ customer deduplication, phone standardization, missing email/phone handling, dummy Unknown row, การกรอง `total_amount ≤ 0`, orphan customer_id remapping และการแปลงสกุลเงินเป็น USD (exact-date match, LOCF, fallback rate, missing currency)

---

## Analytical Query Execution

หลังจากรัน `python pipeline.py` จนได้ `analytics.db` แล้ว สามารถรัน `clv_report.sql` เพื่อดูรายงาน CLV ได้ **ผ่าน Python**
```bash
python -c "
import sqlite3, pandas as pd
conn = sqlite3.connect('analytics.db')
query = open('clv_report.sql').read()
print(pd.read_sql(query, conn))
"
```

ผลลัพธ์ที่ได้จะตรงกับตารางใน [Analytical Query & Customer Lifetime Value (CLV) Report](#analytical-query--customer-lifetime-value-clv-report) ด้านล่าง

---

## Analytical Query & Customer Lifetime Value (CLV) Report

สรุปผลลัพธ์และการวิเคราะห์เชิงธุรกิจจากการรันคำสั่ง `clv_report.sql`

### 1. Business Logic & Implementation Decisions
- **Revenue Recognition (คำนวณเฉพาะยอดเงินจริง)**: กรองคำนวณเฉพาะคำสั่งซื้อที่มีสถานะ `status = 'COMPLETED'` เท่านั้น ไม่นับรวมคำสั่งซื้อที่ถูกยกเลิก (`CANCELLED`) หรือรอดำเนินการ (`PENDING`) เพื่อให้ตัวเลขสะท้อนรายได้ที่เกิดขึ้นจริงตามหลักบัญชี
- **Retention of All Customers (LEFT JOIN)**: ใช้ `LEFT JOIN` จากตาราง `dim_customers` เพื่อให้ลูกค้ารายใหม่หรือผู้ที่ยังไม่เคยสั่งซื้อสำเร็จ (เช่น Ian Malcolm, Jane Doe) ยังคงปรากฏในรายงานด้วยยอดคำสั่งซื้อ `0` และ CLV `$0.00` ซึ่งเป็นข้อมูลสำคัญสำหรับทีม CRM ในการทำ Re-engagement Campaign
- **Revenue Preservation via Unknown Dimension**: คำสั่งซื้อที่เป็น Orphan Record ถูกจัดสรรเข้ากลุ่ม `Unknown` (`customer_id = -1`) ทำให้รายได้รวมจำนวน $705.50 ไม่สูญหายจากงบการเงิน และแสดง `customer_cohort` เป็น `'Unknown'` อย่างชัดเจน
- **Cohort Segmentation**: จัดกลุ่มลูกค้าตามเดือนที่ลงทะเบียน (`strftime('%Y-%m', signup_date)`) ช่วยให้ฝ่ายธุรกิจสามารถติดตามและเปรียบเทียบพฤติกรรมการซื้อซ้ำของลูกค้าในแต่ละช่วงเวลาได้อย่างแม่นยำ

### 2. Output Summary & Business Insights
| customer_id | full_name | total_orders_placed | lifetime_value_usd | customer_cohort |
|---|---|:---:|:---:|:---:|
| 1 | Alice Smith | 3 | $1,998.00 | 2023-06 |
| -1 | Unknown | 2 | $705.50 | Unknown |
| 6 | Fiona Gallagher | 2 | $525.00 | 2023-05 |
| 4 | Diana Prince | 2 | $389.50 | 2023-04 |
| 5 | Evan Wright | 2 | $230.49 | 2023-04 |
| 2 | Bob Jones | 1 | $220.00 | 2023-09 |
| 3 | Charlie Brown | 1 | $180.00 | 2023-03 |
| 8 | Hannah Abbott | 1 | $98.34 | 2023-07 |
| 7 | George Costanza | 1 | $40.00 | 2023-06 |
| 9 | Ian Malcolm | 0 | $0.00 | 2023-08 |
| 10 | Jane Doe | 0 | $0.00 | 2023-09 |

- **Top Spender**: Alice Smith ครองอันดับ 1 ด้วย CLV สูงสุดที่ $1,998.00 (3 คำสั่งซื้อ)
- **Data Leakage Alert**: กลุ่ม `Unknown` มียอดรวมสูงเป็นอันดับที่ 2 ($705.50) ชี้ให้เห็นว่าระบบ Transaction หน้าร้านยังมีปัญหา Data Integrity ที่ต้องวาง Data Validation ป้องกันการบันทึก Orphan Record ในอนาคต
- **Conversion Opportunity**: ลูกค้า 2 ราย (Ian Malcolm และ Jane Doe) ยังไม่มีการสั่งซื้อสำเร็จ สามารถส่งต่อข้อมูลให้ทีม Growth/Marketing ทำ Conversion หรือ Onboarding Campaign ต่อได้ทันที